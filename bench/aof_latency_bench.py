#!/usr/bin/env python3
"""Compare online QPS and latency percentiles across AOF policies."""

import argparse
import csv
import math
import os
import platform
import signal
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Dict, List


HOST = "127.0.0.1"
PORT = 9096
VALID_POLICIES = {"off", "no", "everysec", "always"}


def parse_policies(text: str) -> List[str]:
    policies = text.split(",")
    if not policies or any(policy not in VALID_POLICIES for policy in policies):
        raise argparse.ArgumentTypeError(
            "policies must be comma-separated off,no,everysec,always values"
        )
    return policies


def port_is_open() -> bool:
    try:
        with socket.create_connection((HOST, PORT), timeout=0.1):
            return True
    except OSError:
        return False


def wait_until_ready(process: subprocess.Popen, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(
                f"server exited with status {process.returncode}\n{stdout}\n{stderr}"
            )
        if port_is_open():
            return
        time.sleep(0.01)
    raise TimeoutError("server did not become ready")


def stop_server(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=10.0)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2.0)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        raise RuntimeError(
            f"server stopped with status {process.returncode}\n{stdout}\n{stderr}"
        )


def parse_metrics(output: str) -> Dict[str, str]:
    metrics = {}
    for line in output.splitlines():
        if ": " in line:
            key, value = line.split(": ", 1)
            metrics[key] = value
    return metrics


def median(values: List[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def percentile(values: List[float], quantile: float) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil(quantile * len(ordered)) - 1)
    return ordered[index]


def run_client(arguments: argparse.Namespace) -> Dict[str, str]:
    command = [
        arguments.client,
        "-s", HOST,
        "-p", str(PORT),
        "-c", str(arguments.connections),
        "-n", str(arguments.requests),
        "-w", str(arguments.warmup),
        "-P", str(arguments.pipeline),
        "-k", str(arguments.keyspace),
        "-T", str(arguments.ttl_ms),
        "-S", str(arguments.seed),
        "-L",
    ]
    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        timeout=arguments.timeout,
    )
    metrics = parse_metrics(result.stdout)
    required = (
        "qps",
        "duration_seconds",
        "requests_completed",
        "set_errors",
        "get_hit_rate_percent",
        "latency_mean_us",
        "latency_p50_us",
        "latency_p95_us",
        "latency_p99_us",
        "latency_p999_us",
        "latency_max_us",
        "latency_p99_over_p50",
        "get_latency_p99_us",
        "set_latency_p99_us",
    )
    missing = [name for name in required if name not in metrics]
    if missing:
        raise RuntimeError(f"client output is missing metrics: {', '.join(missing)}")
    return metrics


def write_csv(path: Path, rows: List[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="./kvstore")
    parser.add_argument("--client", default="./mixed_qps_client")
    parser.add_argument(
        "--policies",
        type=parse_policies,
        default=parse_policies("off,no,everysec"),
    )
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--connections", type=int, default=32)
    parser.add_argument("--requests", type=int, default=1000000)
    parser.add_argument("--warmup", type=int, default=1000)
    parser.add_argument("--pipeline", type=int, default=16)
    parser.add_argument("--keyspace", type=int, default=100000)
    parser.add_argument("--ttl-ms", type=int, default=0)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--maxmemory", default="64MiB")
    parser.add_argument("--timeout", type=float, default=600.0)
    parser.add_argument("--csv", type=Path)
    parser.add_argument("--aof-dir", type=Path)
    parser.add_argument(
        "--build-label",
        default="unknown",
        help="compiler/build options label copied into CSV",
    )
    arguments = parser.parse_args()

    if (
        arguments.repeats <= 0
        or arguments.connections <= 0
        or arguments.requests < 10
        or arguments.requests % 10 != 0
        or arguments.warmup < 0
        or arguments.pipeline <= 0
        or arguments.keyspace <= 0
        or arguments.ttl_ms < 0
    ):
        parser.error("invalid benchmark size, requests must be a positive multiple of 10")
    if not Path(arguments.server).is_file() or not Path(arguments.client).is_file():
        parser.error("build kvstore and mixed_qps_client before running this script")
    if port_is_open():
        parser.error("port 9096 is already in use; stop the running kvstore first")

    temporary = None
    if arguments.aof_dir is None:
        temporary = tempfile.TemporaryDirectory(prefix="storeSystem-latency-bench-")
        aof_directory = Path(temporary.name)
    else:
        aof_directory = arguments.aof_dir
        aof_directory.mkdir(parents=True, exist_ok=True)

    system = platform.uname()
    print(
        f"system={system.system} release={system.release} machine={system.machine} "
        f"cpu_count={os.cpu_count()} build_label={arguments.build_label}"
    )
    print(
        f"connections={arguments.connections} requests={arguments.requests} "
        f"warmup={arguments.warmup} pipeline={arguments.pipeline} "
        f"keyspace={arguments.keyspace} ttl_ms={arguments.ttl_ms} "
        f"repeats={arguments.repeats} client_server=same_host"
    )

    rows: List[Dict[str, object]] = []
    try:
        for policy in arguments.policies:
            policy_rows = []
            for run in range(1, arguments.repeats + 1):
                aof_path = aof_directory / f"{policy}-{run}.aof"
                if aof_path.exists():
                    aof_path.unlink()
                server_command = [
                    arguments.server,
                    "--maxmemory", arguments.maxmemory,
                    "--maxkeys", str(arguments.keyspace),
                ]
                if policy == "off":
                    server_command.extend(["--appendonly", "no"])
                else:
                    server_command.extend(
                        [
                            "--appendonly", "yes",
                            "--appendfilename", str(aof_path),
                            "--appendfsync", policy,
                        ]
                    )
                server = subprocess.Popen(
                    server_command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                try:
                    wait_until_ready(server)
                    metrics = run_client(arguments)
                finally:
                    stop_server(server)

                row: Dict[str, object] = {
                    "policy": policy,
                    "run": run,
                    "connections": arguments.connections,
                    "requests": arguments.requests,
                    "warmup": arguments.warmup,
                    "pipeline": arguments.pipeline,
                    "keyspace": arguments.keyspace,
                    "ttl_ms": arguments.ttl_ms,
                    "seed": arguments.seed,
                    "maxmemory": arguments.maxmemory,
                    "build_label": arguments.build_label,
                    "duration_seconds": metrics["duration_seconds"],
                    "qps": metrics["qps"],
                    "latency_mean_us": metrics["latency_mean_us"],
                    "latency_p50_us": metrics["latency_p50_us"],
                    "latency_p95_us": metrics["latency_p95_us"],
                    "latency_p99_us": metrics["latency_p99_us"],
                    "latency_p999_us": metrics["latency_p999_us"],
                    "latency_max_us": metrics["latency_max_us"],
                    "latency_p99_over_p50": metrics["latency_p99_over_p50"],
                    "get_latency_p99_us": metrics["get_latency_p99_us"],
                    "set_latency_p99_us": metrics["set_latency_p99_us"],
                    "get_hit_rate_percent": metrics["get_hit_rate_percent"],
                    "requests_completed": metrics["requests_completed"],
                    "set_errors": metrics["set_errors"],
                    "aof_bytes_after_run":
                        aof_path.stat().st_size if aof_path.exists() else 0,
                }
                rows.append(row)
                policy_rows.append(row)
                print(
                    f"policy={policy} run={run} qps={float(row['qps']):.2f} "
                    f"p50={float(row['latency_p50_us']):.3f}us "
                    f"p99={float(row['latency_p99_us']):.3f}us "
                    f"p999={float(row['latency_p999_us']):.3f}us "
                    f"max={float(row['latency_max_us']):.3f}us"
                )
            qps_values = [float(row["qps"]) for row in policy_rows]
            p99_values = [float(row["latency_p99_us"]) for row in policy_rows]
            p999_values = [float(row["latency_p999_us"]) for row in policy_rows]
            print(
                f"summary policy={policy} "
                f"qps_median={median(qps_values):.2f} "
                f"qps_min={min(qps_values):.2f} "
                f"qps_max={max(qps_values):.2f} "
                f"p99_median_us={median(p99_values):.3f} "
                f"p99_p95_run_us={percentile(p99_values, 0.95):.3f} "
                f"p999_median_us={median(p999_values):.3f}"
            )
    finally:
        if temporary is not None:
            temporary.cleanup()

    if arguments.csv is not None and rows:
        write_csv(arguments.csv, rows)
        print(f"csv: {arguments.csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

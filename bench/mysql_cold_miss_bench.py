#!/usr/bin/env python3
"""Measure unique-key MySQL Cache-Aside misses with a fresh cache per run."""

import argparse
import csv
import json
import os
import platform
import shutil
import signal
import socket
import statistics
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Tuple


VALUE_64 = b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
SERVER_PORT = 9096
RAW_FIELDS = [
    "run", "connections", "pipeline_depth", "requests", "value_bytes",
    "duration_seconds", "qps", "latency_mean_us", "latency_p50_us",
    "latency_p95_us", "latency_p99_us", "latency_p999_us",
    "latency_max_us", "get_completed", "values_found",
    "server_keys_before", "server_keys_after", "mysql_loads_delta",
    "mysql_coalesced_loads_delta", "mysql_load_errors_delta",
    "mysql_pending_reads_after", "mysql_connected_readers_after",
    "mysql_connected_writer_after", "validation",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Seed MySQL through kvstore, restart with an empty cache for every "
            "round, and report five-round medians for unique 64-byte GET misses."
        )
    )
    parser.add_argument("--requests", type=int, default=1_000_000)
    parser.add_argument("--connections", type=int, default=32)
    parser.add_argument("--pipeline", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--run-id", type=int, default=7000000)
    parser.add_argument("--mysql-host", default=os.getenv("KVSTORE_MYSQL_TEST_HOST", "127.0.0.1"))
    parser.add_argument("--mysql-port", type=int, default=int(os.getenv("KVSTORE_MYSQL_TEST_PORT", "3306")))
    parser.add_argument("--mysql-user", default=os.getenv("KVSTORE_MYSQL_TEST_USER", "kvstore"))
    parser.add_argument("--mysql-database", default=os.getenv("KVSTORE_MYSQL_TEST_DATABASE", "kvstore_test"))
    parser.add_argument("--mysql-read-workers", type=int, default=8)
    parser.add_argument("--bootstrap-timeout", type=float, default=7200.0)
    parser.add_argument("--run-timeout", type=float, default=1800.0)
    parser.add_argument(
        "--build-description",
        default=(
            "make MYSQL=1; default -O2 -g -Wall -Wextra -Wpedantic; "
            "jemalloc"
        ),
        help="build flags/allocator recorded in metadata",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("bench/result/v0.7.0-mysql-cold-miss.csv"),
    )
    args = parser.parse_args()
    if args.requests < args.connections or args.requests <= 0:
        parser.error("--requests must be positive and >= --connections")
    if not 1 <= args.connections <= 1024:
        parser.error("--connections must be in [1, 1024]")
    if not 1 <= args.pipeline <= 1024:
        parser.error("--pipeline must be in [1, 1024]")
    if args.repeats < 1:
        parser.error("--repeats must be positive")
    if args.run_id < 0:
        parser.error("--run-id must be unsigned")
    if not args.mysql_database.endswith("_test"):
        parser.error("the benchmark resets data; --mysql-database must end in _test")
    return args


def mysql_command(args: argparse.Namespace, sql: str) -> str:
    password = os.getenv("KVSTORE_MYSQL_TEST_PASSWORD")
    if password is None:
        raise RuntimeError("set KVSTORE_MYSQL_TEST_PASSWORD before running the benchmark")
    environment = os.environ.copy()
    environment["MYSQL_PWD"] = password
    result = subprocess.run(
        [
            "mysql", "--batch", "--skip-column-names",
            "-h", args.mysql_host, "-P", str(args.mysql_port),
            "-u", args.mysql_user, args.mysql_database, "-e", sql,
        ],
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"mysql command failed: {result.stderr.strip()}")
    return result.stdout.strip()


def reset_database(args: argparse.Namespace) -> None:
    mysql_command(
        args,
        "DELETE FROM kv_change_log; DELETE FROM kv_writer_state; "
        "DELETE FROM kv_keys; UPDATE kv_schema_meta SET "
        "bootstrap_state='EMPTY',active_writer_uuid=NULL WHERE singleton_id=1",
    )


def encode_command(*arguments: bytes) -> bytes:
    pieces = [f"*{len(arguments)}\r\n".encode("ascii")]
    for argument in arguments:
        pieces.extend((f"${len(argument)}\r\n".encode("ascii"), argument, b"\r\n"))
    return b"".join(pieces)


def receive_line(client: socket.socket) -> bytes:
    data = bytearray()
    while not data.endswith(b"\r\n"):
        chunk = client.recv(1)
        if not chunk:
            raise ConnectionError("server closed the connection")
        data.extend(chunk)
    return bytes(data[:-2])


def exchange_info(section: bytes = b"MYSQL") -> Dict[str, int]:
    with socket.create_connection(("127.0.0.1", SERVER_PORT), timeout=3.0) as client:
        client.settimeout(5.0)
        client.sendall(encode_command(b"INFO", section))
        prefix = client.recv(1)
        if prefix != b"$":
            raise RuntimeError(f"INFO returned unexpected RESP prefix {prefix!r}")
        length = int(receive_line(client))
        payload = bytearray()
        while len(payload) < length + 2:
            chunk = client.recv(length + 2 - len(payload))
            if not chunk:
                raise ConnectionError("truncated INFO response")
            payload.extend(chunk)
    fields: Dict[str, int] = {}
    for line in bytes(payload[:length]).decode("ascii").splitlines():
        name, value = line.split(":", 1)
        fields[name] = int(value)
    return fields


def wait_for_server(process: subprocess.Popen, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(
                f"kvstore exited with {process.returncode}\nstdout={stdout}\nstderr={stderr}"
            )
        try:
            info = exchange_info()
            if info.get("mysql_enabled") == 1:
                return
        except (ConnectionError, OSError, RuntimeError, ValueError):
            time.sleep(0.05)
    raise TimeoutError(f"kvstore did not become ready within {timeout:.0f}s")


def server_command(args: argparse.Namespace, aof_path: Path) -> List[str]:
    return [
        "./kvstore",
        "--maxmemory", "0", "--maxkeys", "0",
        "--appendonly", "yes", "--appendfilename", str(aof_path),
        "--appendfsync", "everysec",
        "--mysql", "yes", "--mysql-host", args.mysql_host,
        "--mysql-port", str(args.mysql_port),
        "--mysql-user", args.mysql_user,
        "--mysql-database", args.mysql_database,
        "--mysql-read-workers", str(args.mysql_read_workers),
    ]


def start_server(args: argparse.Namespace, aof_path: Path, timeout: float) -> subprocess.Popen:
    environment = os.environ.copy()
    environment["KVSTORE_MYSQL_PASSWORD"] = os.environ["KVSTORE_MYSQL_TEST_PASSWORD"]
    process = subprocess.Popen(
        server_command(args, aof_path),
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        wait_for_server(process, timeout)
    except BaseException:
        stop_server(process, allow_failure=True)
        raise
    return process


def stop_server(process: subprocess.Popen, allow_failure: bool = False) -> Tuple[str, str]:
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=15.0)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5.0)
    stdout, stderr = process.communicate()
    if not allow_failure and process.returncode not in (0, -signal.SIGTERM):
        raise RuntimeError(
            f"kvstore exited with {process.returncode}\nstdout={stdout}\nstderr={stderr}"
        )
    return stdout, stderr


def ensure_server_port_free() -> None:
    try:
        with socket.create_connection(("127.0.0.1", SERVER_PORT), timeout=0.2):
            raise RuntimeError(f"port {SERVER_PORT} is already in use; stop the existing kvstore")
    except ConnectionRefusedError:
        return
    except TimeoutError:
        return


def make_key(run_id: int, index: int) -> bytes:
    return f"mixed:{run_id}:{index}".encode("ascii")


def write_seed_aof(path: Path, run_id: int, requests: int) -> Tuple[int, float]:
    started = time.monotonic()
    with path.open("wb", buffering=1024 * 1024) as output:
        for index in range(requests):
            output.write(encode_command(b"SET", make_key(run_id, index), VALUE_64))
    return path.stat().st_size, time.monotonic() - started


def parse_metrics(output: str) -> Dict[str, str]:
    metrics: Dict[str, str] = {}
    for line in output.splitlines():
        if ": " not in line:
            continue
        name, value = line.split(": ", 1)
        metrics[name] = value
    return metrics


def capture_command(command: List[str]) -> str:
    try:
        result = subprocess.run(
            command, text=True, capture_output=True, timeout=10.0, check=False
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return f"unavailable: {error}"
    output = (result.stdout or result.stderr).strip()
    return output if result.returncode == 0 else f"exit={result.returncode}: {output}"


def delta(before: Dict[str, int], after: Dict[str, int], field: str) -> int:
    return after[field] - before[field]


def run_once(args: argparse.Namespace, root: Path, run: int) -> Dict[str, object]:
    aof_path = root / f"cold-run-{run}.aof"
    process = start_server(args, aof_path, 30.0)
    try:
        before = exchange_info()
        command = [
            "./mixed_qps_client",
            "-s", "127.0.0.1", "-p", str(SERVER_PORT),
            "-c", str(args.connections), "-n", str(args.requests),
            "-w", "0", "-P", str(args.pipeline),
            "-k", str(args.requests), "-R", str(args.run_id),
            "-M", "-L",
        ]
        result = subprocess.run(
            command,
            text=True,
            capture_output=True,
            timeout=args.run_timeout,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"mixed_qps_client failed with {result.returncode}\n"
                f"stdout={result.stdout}\nstderr={result.stderr}"
            )
        metrics = parse_metrics(result.stdout)
        after = exchange_info()
    finally:
        stop_server(process)

    loads = delta(before, after, "mysql_loads")
    coalesced = delta(before, after, "mysql_coalesced_loads")
    load_errors = delta(before, after, "mysql_load_errors")
    expected = args.requests
    checks = {
        "cold_mode": metrics.get("full_cold_mysql_miss_mode") == "enabled",
        "requests_completed": int(metrics.get("requests_completed", -1)) == expected,
        "get_completed": int(metrics.get("get_completed", -1)) == expected,
        "all_values_found": int(metrics.get("get_hits", -1)) == expected,
        "no_null_values": int(metrics.get("get_misses", -1)) == 0,
        "empty_cache_before": int(metrics.get("server_keys_before", -1)) == 0,
        "all_keys_loaded": int(metrics.get("server_keys_after", -1)) == expected,
        "one_mysql_load_per_key": loads == expected,
        "no_request_coalescing": coalesced == 0,
        "no_mysql_load_errors": load_errors == 0,
        "read_queue_drained": after["mysql_pending_reads"] == 0,
        "readers_connected": after["mysql_connected_readers"] == args.mysql_read_workers,
        "writer_connected": after["mysql_connected_writer"] == 1,
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise RuntimeError(
            f"run {run} is not a valid full-cold MySQL miss run: {', '.join(failed)}\n"
            f"client output:\n{result.stdout}\nINFO MYSQL before={before}\nafter={after}"
        )
    return {
        "run": run,
        "connections": args.connections,
        "pipeline_depth": args.pipeline,
        "requests": args.requests,
        "value_bytes": len(VALUE_64),
        "duration_seconds": metrics["duration_seconds"],
        "qps": metrics["qps"],
        "latency_mean_us": metrics["latency_mean_us"],
        "latency_p50_us": metrics["latency_p50_us"],
        "latency_p95_us": metrics["latency_p95_us"],
        "latency_p99_us": metrics["latency_p99_us"],
        "latency_p999_us": metrics["latency_p999_us"],
        "latency_max_us": metrics["latency_max_us"],
        "get_completed": metrics["get_completed"],
        "values_found": metrics["get_hits"],
        "server_keys_before": metrics["server_keys_before"],
        "server_keys_after": metrics["server_keys_after"],
        "mysql_loads_delta": loads,
        "mysql_coalesced_loads_delta": coalesced,
        "mysql_load_errors_delta": load_errors,
        "mysql_pending_reads_after": after["mysql_pending_reads"],
        "mysql_connected_readers_after": after["mysql_connected_readers"],
        "mysql_connected_writer_after": after["mysql_connected_writer"],
        "validation": "PASS",
    }


def write_raw(path: Path, rows: List[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=RAW_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def write_summary(path: Path, args: argparse.Namespace, rows: List[Dict[str, object]]) -> Dict[str, object]:
    qps_values = [float(row["qps"]) for row in rows]
    p99_us_values = [float(row["latency_p99_us"]) for row in rows]
    summary = {
        "connections": args.connections,
        "pipeline_depth": args.pipeline,
        "requests_per_run": args.requests,
        "value_bytes": len(VALUE_64),
        "runs": len(rows),
        "qps_median": statistics.median(qps_values),
        "qps_min": min(qps_values),
        "qps_max": max(qps_values),
        "qps_cv_percent": (
            statistics.pstdev(qps_values) * 100.0 / statistics.mean(qps_values)
            if len(qps_values) > 1 else 0.0
        ),
        "p99_ms_median": statistics.median(p99_us_values) / 1000.0,
        "p99_ms_min": min(p99_us_values) / 1000.0,
        "p99_ms_max": max(p99_us_values) / 1000.0,
        "validation": "PASS",
    }
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=list(summary.keys()))
        writer.writeheader()
        writer.writerow(summary)
    return summary


def main() -> int:
    args = parse_args()
    if shutil.which("mysql") is None:
        raise RuntimeError("mysql client is required")
    for executable in (Path("kvstore"), Path("mixed_qps_client")):
        if not executable.is_file():
            raise RuntimeError(f"missing ./{executable}; run make MYSQL=1 kvstore mixed_qps_client")
    ensure_server_port_free()
    reset_database(args)
    output = args.output.resolve()
    summary_path = output.with_name(output.stem + ".summary.csv")
    metadata_path = output.with_name(output.stem + ".metadata.json")
    rows: List[Dict[str, object]] = []

    with tempfile.TemporaryDirectory(prefix="kvstore-mysql-cold-miss-") as directory:
        root = Path(directory)
        seed_path = root / "seed.aof"
        print(f"[seed] writing {args.requests} SET records to {seed_path}", flush=True)
        seed_bytes, seed_seconds = write_seed_aof(seed_path, args.run_id, args.requests)
        print(f"[bootstrap] starting kvstore; this imports the recovered cache into MySQL", flush=True)
        bootstrap = start_server(args, seed_path, args.bootstrap_timeout)
        stop_server(bootstrap)
        database_keys = int(mysql_command(args, "SELECT COUNT(*) FROM kv_keys"))
        state = mysql_command(
            args, "SELECT bootstrap_state FROM kv_schema_meta WHERE singleton_id=1"
        )
        if database_keys != args.requests or state != "READY":
            raise RuntimeError(
                f"bootstrap validation failed: state={state!r}, keys={database_keys}, "
                f"expected={args.requests}"
            )
        print(f"[bootstrap] READY with {database_keys} keys", flush=True)

        for run in range(1, args.repeats + 1):
            print(f"[run {run}/{args.repeats}] fresh process and empty AOF/cache", flush=True)
            row = run_once(args, root, run)
            rows.append(row)
            write_raw(output, rows)
            print(
                f"[run {run}] qps={float(row['qps']):.2f}, "
                f"p99={float(row['latency_p99_us']) / 1000.0:.3f} ms, validation=PASS",
                flush=True,
            )

    summary = write_summary(summary_path, args, rows)
    metadata = {
        "benchmark": "MySQL Cache-Aside full-cold unique-key GET",
        "platform": platform.platform(),
        "python": platform.python_version(),
        "mysql_host": args.mysql_host,
        "mysql_port": args.mysql_port,
        "mysql_database": args.mysql_database,
        "mysql_read_workers": args.mysql_read_workers,
        "mysql_buffer_pool": "not flushed; MySQL remains running between rounds",
        "client_server_topology": "same host",
        "server_port": SERVER_PORT,
        "build_description": args.build_description,
        "compiler": capture_command(["cc", "--version"]),
        "mysql_client": capture_command(["mysql", "--version"]),
        "cpu": capture_command(["lscpu"]),
        "memory": capture_command(["free", "-h"]),
        "storage": capture_command(
            ["lsblk", "-o", "NAME,MODEL,SIZE,ROTA,TYPE,MOUNTPOINTS"]
        ),
        "git_commit": capture_command(["git", "rev-parse", "HEAD"]),
        "git_status": capture_command(["git", "status", "--short"]),
        "run_id": args.run_id,
        "seed_aof_bytes": seed_bytes,
        "seed_aof_generation_seconds": seed_seconds,
        "raw_csv": str(output),
        "summary_csv": str(summary_path),
        "command": " ".join(os.sys.argv),
    }
    with metadata_path.open("w", encoding="utf-8") as output_file:
        json.dump(metadata, output_file, ensure_ascii=False, indent=2)
        output_file.write("\n")

    print(f"raw_csv: {output}")
    print(f"summary_csv: {summary_path}")
    print(f"metadata_json: {metadata_path}")
    print(
        f"resume_metric: 在{args.connections}并发、64B value、全量冷key miss条件下，"
        f"MySQL回源吞吐约{summary['qps_median']:.0f} QPS，"
        f"P99延迟约{summary['p99_ms_median']:.2f} ms（{len(rows)}轮中位数，"
        f"Pipeline={args.pipeline}）。"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

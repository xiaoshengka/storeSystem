#!/usr/bin/env python3
"""Run the same RESP2 C collection workloads against storeSystem and Redis 6.2.23."""

import argparse
import csv
import signal
import socket
import statistics
import subprocess
import tempfile
import time
from pathlib import Path


ALL_WORKLOADS = (
    "hash-insert", "hget", "hash-mixed", "zset-insert",
    "zscore", "zset-mixed", "zrange",
)
SUPPORTED_WORKLOADS = ("string-mixed",) + ALL_WORKLOADS
ALL_TARGETS = ("skiplist", "rbtree", "redis")
LOCAL_TARGETS = ("skiplist", "rbtree")
HASH_WORKLOADS = {"hash-insert", "hget", "hash-mixed"}
STRING_WORKLOADS = {"string-mixed"}
WORKLOAD_PROFILES = {
    "string-mixed": "90% GET / 10% SET",
    "hash-mixed": "90% HGET / 10% HSET",
    "zset-mixed": "90% ZSCORE / 10% ZADD",
}
WRITE_WORKLOADS = {
    "string-mixed", "hash-insert", "hash-mixed", "zset-insert", "zset-mixed",
}
CSV_FIELDS = (
    "target", "policy", "workload", "repeat", "connections", "pipeline",
    "requests", "keyspace", "keyspace_scope", "dataset_key", "seed", "qps",
    "operation_mix", "payload_kind", "value_bytes",
    "initial_cardinality", "expected_final_cardinality",
    "actual_final_cardinality",
    "mean_us", "p50_us", "p95_us", "p99_us", "p999_us", "max_us", "errors",
    "rss_kb", "logical_memory", "aof_bytes",
    "aof_queue_bytes", "aof_backpressure_events",
)


def wait_port(port: int, process: subprocess.Popen, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"server exited with {process.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"server did not open port {port}")


def encode(*parts: bytes) -> bytes:
    return b"*%d\r\n" % len(parts) + b"".join(
        b"$%d\r\n" % len(part) + part + b"\r\n" for part in parts
    )


def info_memory(port: int, redis: bool) -> int:
    command = (b"INFO", b"MEMORY") if redis else (b"INFO", b"CACHE")
    with socket.create_connection(("127.0.0.1", port), timeout=3.0) as client:
        client.sendall(encode(*command))
        data = bytearray()
        while b"\r\n" not in data:
            data.extend(client.recv(4096))
        line, payload = bytes(data).split(b"\r\n", 1)
        length = int(line[1:])
        while len(payload) < length + 2:
            payload += client.recv(65536)
    field = b"used_memory:" if redis else b"used_memory:"
    for line in payload[:length].splitlines():
        if line.startswith(field):
            return int(line.split(b":", 1)[1])
    return 0


def persistence_metrics(port: int, redis: bool) -> tuple[int, int]:
    if redis:
        return 0, 0
    with socket.create_connection(("127.0.0.1", port), timeout=3.0) as client:
        client.sendall(encode(b"INFO", b"PERSISTENCE"))
        data = bytearray()
        while b"\r\n" not in data:
            data.extend(client.recv(4096))
        line, payload = bytes(data).split(b"\r\n", 1)
        length = int(line[1:])
        while len(payload) < length + 2:
            payload += client.recv(65536)
    values = {}
    for item in payload[:length].splitlines():
        if b":" in item:
            key, value = item.split(b":", 1)
            values[key] = int(value)
    return (values.get(b"aof_queue_bytes", 0),
            values.get(b"aof_backpressure_events", 0))


def integer_command(port: int, *parts: bytes) -> int:
    with socket.create_connection(("127.0.0.1", port), timeout=3.0) as client:
        client.sendall(encode(*parts))
        data = bytearray()
        while b"\r\n" not in data:
            chunk = client.recv(128)
            if not chunk:
                raise ConnectionError("server closed before integer response")
            data.extend(chunk)
    line = bytes(data).split(b"\r\n", 1)[0]
    if not line.startswith(b":"):
        raise RuntimeError(f"expected integer response, received {line!r}")
    return int(line[1:])


def peak_rss_kb(pid: int) -> int:
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmHWM:"):
                return int(line.split()[1])
    except (FileNotFoundError, ProcessLookupError):
        pass
    return 0


def parse_metrics(output: str) -> dict:
    metrics = {}
    for line in output.splitlines():
        if ": " in line:
            key, value = line.split(": ", 1)
            metrics[key] = value
    return metrics


def stop(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=10.0)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3.0)


def server_command(target: str, args, port: int, policy: str, aof_path: Path):
    if target == "redis":
        command = [args.redis_server, "--port", str(port), "--save", "",
                   "--hash-max-ziplist-entries", "0",
                   "--zset-max-ziplist-entries", "0",
                   "--auto-aof-rewrite-percentage", "0"]
        if policy == "off":
            command += ["--appendonly", "no"]
        else:
            command += ["--appendonly", "yes", "--appendfsync", policy,
                        "--dir", str(aof_path.parent),
                        "--appendfilename", aof_path.name]
        return command
    command = [args.server, "--zset-engine", target, "--maxkeys", "0",
               "--maxmemory", "0"]
    if policy != "off":
        command += ["--appendonly", "yes", "--appendfsync", policy,
                    "--appendfilename", str(aof_path)]
    return command


def build_cases(targets, policies, workloads, repeats, pipelines):
    """Build a matrix without repeating local Hash for each ZSet engine."""
    cases = []
    local_targets = tuple(value for value in targets if value in LOCAL_TARGETS)
    hash_server_engine = ("skiplist" if "skiplist" in local_targets
                          else local_targets[0] if local_targets else None)
    for policy in policies:
        selected = workloads if policy == "off" else tuple(
            value for value in workloads if value in WRITE_WORKLOADS
        )
        for workload in selected:
            if workload in STRING_WORKLOADS:
                if hash_server_engine is not None:
                    for pipeline in pipelines:
                        for repeat in range(1, repeats + 1):
                            cases.append(("string", hash_server_engine, policy,
                                          workload, pipeline, repeat))
            elif workload in HASH_WORKLOADS:
                if hash_server_engine is not None:
                    for pipeline in pipelines:
                        for repeat in range(1, repeats + 1):
                            cases.append(("hash", hash_server_engine, policy,
                                          workload, pipeline, repeat))
            else:
                for target in local_targets:
                    for pipeline in pipelines:
                        for repeat in range(1, repeats + 1):
                            cases.append((target, target, policy, workload,
                                          pipeline, repeat))
            if "redis" in targets:
                for pipeline in pipelines:
                    for repeat in range(1, repeats + 1):
                        cases.append(("redis", "redis", policy, workload,
                                      pipeline, repeat))
    return cases


def verify_redis_version(parser: argparse.ArgumentParser, executable: str) -> str:
    try:
        result = subprocess.run(
            [executable, "--version"], check=True, capture_output=True, text=True,
            timeout=10.0,
        )
    except (OSError, subprocess.SubprocessError) as error:
        parser.error(f"cannot execute Redis server {executable!r}: {error}")
    version = (result.stdout or result.stderr).strip()
    if "v=6.2.23" not in version:
        parser.error(
            "Redis comparison requires 6.2.23; "
            f"{executable!r} reported: {version}"
        )
    return version


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-server",
                        help="path to the Redis 6.2.23 redis-server binary")
    parser.add_argument("--targets", default=",".join(ALL_TARGETS),
                        help="comma-separated skiplist,rbtree,redis targets")
    parser.add_argument("--server", default="./kvstore")
    parser.add_argument("--client", default="./collection_bench_client")
    parser.add_argument("--mixed-client", default="./mixed_qps_client",
                        help="client used by the string-mixed workload")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--connections", type=int, default=32)
    parser.add_argument("--requests", type=int, default=1_000_000)
    parser.add_argument("--warmup", type=int, default=1000)
    parser.add_argument("--pipelines", default="16",
                        help="one pipeline depth or a comma-separated list")
    parser.add_argument("--keyspace", type=int, default=100_000)
    parser.add_argument("--seed", type=int, default=20260820)
    parser.add_argument("--workloads", default=",".join(ALL_WORKLOADS))
    parser.add_argument("--aof-policies", default="off,no,everysec")
    parser.add_argument("--timeout", type=float, default=3600.0,
                        help="maximum seconds allowed for one client run")
    parser.add_argument("--csv", required=True)
    parser.add_argument("--summary-csv",
                        help="summary CSV (default: <csv>.summary.csv)")
    args = parser.parse_args()
    workloads = tuple(value.strip() for value in args.workloads.split(","))
    targets = tuple(value.strip() for value in args.targets.split(","))
    policies = tuple(value.strip() for value in args.aof_policies.split(","))
    try:
        pipelines = tuple(dict.fromkeys(
            int(value.strip()) for value in args.pipelines.split(",")
        ))
    except ValueError:
        parser.error("--pipelines must contain comma-separated integers")
    if not targets or any(value not in ALL_TARGETS for value in targets):
        parser.error("targets must be skiplist,rbtree,redis")
    if "redis" in targets and not args.redis_server:
        parser.error("--redis-server is required when the redis target is selected")
    if any(value not in SUPPORTED_WORKLOADS for value in workloads):
        parser.error("unknown workload")
    if any(value not in ("off", "no", "everysec", "always") for value in policies):
        parser.error("AOF policies must be off,no,everysec,always")
    if (args.repeats < 1 or args.connections < 1 or args.requests < 1 or
            args.warmup < 0 or not pipelines or args.keyspace < 10 or
            args.timeout <= 0 or args.connections > 1024 or
            any(value < 1 or value > 1024 for value in pipelines) or
            args.requests < args.connections):
        parser.error("numeric benchmark arguments are out of range")
    redis_version = (verify_redis_version(parser, args.redis_server)
                     if "redis" in targets else "not selected")
    cases = build_cases(targets, policies, workloads, args.repeats, pipelines)
    if not cases:
        parser.error("selected policies and workloads produce no benchmark runs")
    output = Path(args.csv)
    summary_output = (Path(args.summary_csv) if args.summary_csv else
                      output.with_suffix(output.suffix + ".summary.csv"))
    output.parent.mkdir(parents=True, exist_ok=True)
    print(f"redis: {redis_version}", flush=True)
    print(
        f"dataset: shared keyspace={args.keyspace}; collection insert grows to "
        f"requests={args.requests}; mixed workloads use exact 90% reads/10% writes; "
        f"connections={args.connections}; pipelines={','.join(map(str, pipelines))}",
        flush=True,
    )
    print(f"runs: {len(cases)}; csv: {output}", flush=True)
    collected_rows = []
    with tempfile.TemporaryDirectory(prefix="collection-compare-") as directory:
        temp = Path(directory)
        with output.open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=CSV_FIELDS)
            writer.writeheader()
            stream.flush()
            for run_number, (target, server_engine, policy, workload,
                             pipeline, repeat) in enumerate(cases, 1):
                started = time.monotonic()
                print(
                    f"[{run_number}/{len(cases)}] START target={target} "
                    f"policy={policy} workload={workload} "
                    f"pipeline={pipeline} "
                    f"repeat={repeat}/{args.repeats}",
                    flush=True,
                )
                port = 6380 if target == "redis" else 9096
                aof_path = temp / (
                    f"{target}-{policy}-{workload}-p{pipeline}-{repeat}.aof"
                )
                process = subprocess.Popen(
                    server_command(server_engine, args, port, policy, aof_path),
                    stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
                )
                metrics = None
                rss_kb = 0
                logical_memory = 0
                actual_cardinality = 0
                aof_queue_bytes = 0
                aof_backpressure_events = 0
                try:
                    wait_port(port, process)
                    if workload in STRING_WORKLOADS:
                        client_command = [
                            args.mixed_client, "-s", "127.0.0.1",
                            "-p", str(port), "-c", str(args.connections),
                            "-n", str(args.requests), "-w", str(args.warmup),
                            "-P", str(pipeline), "-k", str(args.keyspace),
                            "-T", "0", "-S", str(args.seed), "-C", "-L",
                        ]
                    else:
                        client_command = [
                            args.client, "-m", workload, "-p", str(port),
                            "-c", str(args.connections), "-n", str(args.requests),
                            "-w", str(args.warmup), "-P", str(pipeline),
                            "-k", str(args.keyspace), "-S", str(args.seed), "-C",
                        ]
                    result = subprocess.run(
                        client_command, check=True, capture_output=True, text=True,
                        timeout=args.timeout,
                    )
                    metrics = parse_metrics(result.stdout)
                    if workload in STRING_WORKLOADS:
                        metrics.update({
                            "keyspace_scope": "shared",
                            "dataset_key": "mixed:*",
                            "initial_cardinality": str(args.keyspace),
                            "expected_final_cardinality": str(args.keyspace),
                            "errors": str(
                                int(metrics.get("set_errors", "0")) +
                                int(metrics.get("get_misses", "0"))
                            ),
                        })
                    if metrics.get("keyspace_scope") != "shared":
                        raise RuntimeError(
                            "collection client did not report a shared keyspace"
                        )
                    if workload in STRING_WORKLOADS:
                        actual_cardinality = integer_command(port, b"DBSIZE")
                    else:
                        cardinality_command = (
                            b"HLEN" if workload.startswith("h") else b"ZCARD"
                        )
                        actual_cardinality = integer_command(
                            port, cardinality_command,
                            metrics["dataset_key"].encode(),
                        )
                    if actual_cardinality != int(
                            metrics["expected_final_cardinality"]):
                        raise RuntimeError(
                            f"shared dataset cardinality mismatch: expected "
                            f"{metrics['expected_final_cardinality']}, got "
                            f"{actual_cardinality}"
                        )
                    rss_kb = peak_rss_kb(process.pid)
                    logical_memory = info_memory(port, target == "redis")
                    aof_queue_bytes, aof_backpressure_events = \
                        persistence_metrics(port, target == "redis")
                finally:
                    stop(process)
                row = {
                    "target": target, "policy": policy,
                    "workload": workload, "repeat": repeat,
                    "connections": args.connections,
                    "pipeline": pipeline, "requests": args.requests,
                    "keyspace": args.keyspace,
                    "keyspace_scope": metrics["keyspace_scope"],
                    "dataset_key": metrics["dataset_key"],
                    "seed": args.seed,
                    "operation_mix": WORKLOAD_PROFILES.get(workload, workload),
                    "payload_kind": ("string-value"
                                     if workload in STRING_WORKLOADS else
                                     metrics.get("payload_kind", "")),
                    "value_bytes": (64 if workload in STRING_WORKLOADS else
                                     metrics.get("payload_bytes", "")),
                    "initial_cardinality": metrics["initial_cardinality"],
                    "expected_final_cardinality":
                        metrics["expected_final_cardinality"],
                    "actual_final_cardinality": actual_cardinality,
                    "qps": metrics["qps"],
                    "mean_us": metrics["latency_mean_us"],
                    "p50_us": metrics["latency_p50_us"],
                    "p95_us": metrics["latency_p95_us"],
                    "p99_us": metrics["latency_p99_us"],
                    "p999_us": metrics["latency_p999_us"],
                    "max_us": metrics["latency_max_us"],
                    "errors": metrics["errors"],
                    "rss_kb": rss_kb,
                    "logical_memory": logical_memory,
                    "aof_bytes": aof_path.stat().st_size
                        if aof_path.exists() else 0,
                    "aof_queue_bytes": aof_queue_bytes,
                    "aof_backpressure_events": aof_backpressure_events,
                }
                writer.writerow(row)
                collected_rows.append(row)
                stream.flush()
                print(
                    f"[{run_number}/{len(cases)}] DONE "
                    f"qps={metrics['qps']} errors={metrics['errors']} "
                    f"cardinality={actual_cardinality} "
                    f"seconds={time.monotonic() - started:.2f}",
                    flush=True,
                )
    grouped = {}
    for row in collected_rows:
        key = (row["target"], row["policy"], row["workload"],
               row["pipeline"])
        grouped.setdefault(key, []).append(row)
    summary_fields = (
        "target", "policy", "workload", "connections", "pipeline", "rounds",
        "operation_mix", "payload_kind", "value_bytes",
        "qps_median", "p50_us_median", "p95_us_median", "p99_us_median",
        "p999_us_median", "max_us_median", "errors_total",
        "actual_final_cardinality", "aof_backpressure_events_max",
    )
    summary_output.parent.mkdir(parents=True, exist_ok=True)
    with summary_output.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=summary_fields)
        writer.writeheader()
        for key, values in sorted(grouped.items()):
            target, policy, workload, pipeline = key
            writer.writerow({
                "target": target, "policy": policy, "workload": workload,
                "connections": args.connections, "pipeline": pipeline,
                "rounds": len(values),
                "operation_mix": values[-1]["operation_mix"],
                "payload_kind": values[-1]["payload_kind"],
                "value_bytes": values[-1]["value_bytes"],
                "qps_median": statistics.median(float(v["qps"]) for v in values),
                "p50_us_median": statistics.median(float(v["p50_us"]) for v in values),
                "p95_us_median": statistics.median(float(v["p95_us"]) for v in values),
                "p99_us_median": statistics.median(float(v["p99_us"]) for v in values),
                "p999_us_median": statistics.median(float(v["p999_us"]) for v in values),
                "max_us_median": statistics.median(float(v["max_us"]) for v in values),
                "errors_total": sum(int(v["errors"]) for v in values),
                "actual_final_cardinality": values[-1]["actual_final_cardinality"],
                "aof_backpressure_events_max": max(
                    int(v["aof_backpressure_events"]) for v in values),
            })
    print(f"completed {len(cases)} runs; wrote {output} and {summary_output}",
          flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

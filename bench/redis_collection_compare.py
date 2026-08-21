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
ALL_SCENARIOS = ("normal", "bgsave")
CSV_FIELDS = (
    "target", "policy", "scenario", "workload", "repeat", "connections", "pipeline",
    "requests", "keyspace", "keyspace_scope", "dataset_key", "seed", "qps",
    "operation_mix", "payload_kind", "value_bytes",
    "initial_cardinality", "expected_final_cardinality",
    "actual_final_cardinality",
    "mean_us", "p50_us", "p95_us", "p99_us", "p999_us", "max_us", "errors",
    "rss_kb", "logical_memory", "aof_bytes",
    "aof_queue_bytes", "aof_backpressure_events",
    "bgsave_duration_us", "fork_pause_us", "child_peak_rss_kb",
    "child_minor_faults", "child_major_faults", "cow_bytes",
    "rdb_recovery_cardinality",
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


def persistence_values(port: int, redis: bool) -> dict:
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
            try:
                values[key] = int(value)
            except ValueError:
                continue
    return values


def persistence_metrics(port: int, redis: bool) -> tuple[int, int]:
    values = persistence_values(port, redis)
    return (values.get(b"aof_queue_bytes", 0),
            values.get(b"aof_backpressure_events", 0))


def simple_command(port: int, *parts: bytes) -> bytes:
    with socket.create_connection(("127.0.0.1", port), timeout=3.0) as client:
        client.sendall(encode(*parts))
        data = bytearray()
        while b"\r\n" not in data:
            chunk = client.recv(256)
            if not chunk:
                raise ConnectionError("server closed before command response")
            data.extend(chunk)
    line = bytes(data).split(b"\r\n", 1)[0]
    if not line.startswith(b"+"):
        raise RuntimeError(f"expected simple response, received {line!r}")
    return line[1:]


def wait_and_trigger_bgsave(port: int, target: str, workload: str,
                            keyspace: int, seed: int, delay: float,
                            client_process: subprocess.Popen) -> None:
    deadline = time.monotonic() + 60.0
    if workload in STRING_WORKLOADS:
        cardinality = (b"DBSIZE",)
    else:
        kind = b"hash" if workload in HASH_WORKLOADS else b"zset"
        dataset_key = b"collection:" + kind + b":" + str(seed).encode()
        cardinality = ((b"HLEN" if workload in HASH_WORKLOADS else b"ZCARD"),
                       dataset_key)
    while time.monotonic() < deadline:
        if client_process.poll() is not None:
            raise RuntimeError("client finished before the BGSAVE trigger")
        try:
            if integer_command(port, *cardinality) >= keyspace:
                break
        except OSError:
            pass
        time.sleep(0.02)
    else:
        raise TimeoutError("dataset was not ready for BGSAVE")
    time.sleep(delay)
    if client_process.poll() is not None:
        raise RuntimeError("client finished before the delayed BGSAVE trigger")
    response = simple_command(port, b"BGSAVE")
    if b"Background saving started" not in response:
        raise RuntimeError(f"unexpected BGSAVE response: {response!r}")


def wait_bgsave_complete(port: int, redis: bool, timeout: float = 120.0) -> dict:
    deadline = time.monotonic() + timeout
    progress_field = (b"rdb_bgsave_in_progress" if redis
                      else b"rdb_bgsave_in_progress")
    while time.monotonic() < deadline:
        values = persistence_values(port, redis)
        if values.get(progress_field, 0) == 0:
            return values
        time.sleep(0.05)
    raise TimeoutError("BGSAVE did not finish before metrics collection")


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


def server_command(target: str, args, port: int, policy: str, aof_path: Path,
                   scenario: str = "normal", rdb_path=None):
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
        if scenario == "bgsave":
            command += ["--dir", str(rdb_path.parent),
                        "--dbfilename", rdb_path.name]
        return command
    command = [args.server, "--zset-engine", target, "--maxkeys", "0",
               "--maxmemory", "0"]
    if policy != "off":
        command += ["--appendonly", "yes", "--appendfsync", policy,
                    "--appendfilename", str(aof_path)]
    if scenario == "bgsave":
        command += ["--rdb", "yes", "--dbfilename", str(rdb_path)]
    return command


def recovery_server_command(target: str, engine: str, args, port: int,
                            rdb_path: Path):
    if target == "redis":
        return [args.redis_server, "--port", str(port), "--save", "",
                "--appendonly", "no", "--dir", str(rdb_path.parent),
                "--dbfilename", rdb_path.name]
    return [args.server, "--zset-engine", engine, "--maxkeys", "0",
            "--maxmemory", "0", "--rdb", "yes", "--dbfilename",
            str(rdb_path)]


def workload_cardinality(port: int, workload: str, dataset_key: str) -> int:
    if workload in STRING_WORKLOADS:
        return integer_command(port, b"DBSIZE")
    command = b"HLEN" if workload in HASH_WORKLOADS else b"ZCARD"
    return integer_command(port, command, dataset_key.encode())


def build_cases(targets, policies, workloads, repeats, pipelines,
                scenarios=("normal",)):
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
    selected = []
    if "normal" in scenarios:
        selected.extend(case + ("normal",) for case in cases)
    if "bgsave" in scenarios:
        selected.extend(case + ("bgsave",) for case in cases
                        if case[2] == "everysec" and
                        case[3] in WORKLOAD_PROFILES)
    return selected


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
    parser.add_argument("--scenarios", default="normal",
                        help="comma-separated normal,bgsave")
    parser.add_argument("--bgsave-delay", type=float, default=1.0,
                        help="seconds after preload before BGSAVE")
    parser.add_argument("--timeout", type=float, default=3600.0,
                        help="maximum seconds allowed for one client run")
    parser.add_argument("--csv", required=True)
    parser.add_argument("--summary-csv",
                        help="summary CSV (default: <csv>.summary.csv)")
    args = parser.parse_args()
    workloads = tuple(value.strip() for value in args.workloads.split(","))
    targets = tuple(value.strip() for value in args.targets.split(","))
    policies = tuple(value.strip() for value in args.aof_policies.split(","))
    scenarios = tuple(value.strip() for value in args.scenarios.split(","))
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
    if not scenarios or any(value not in ALL_SCENARIOS for value in scenarios):
        parser.error("scenarios must be normal,bgsave")
    if (args.repeats < 1 or args.connections < 1 or args.requests < 1 or
            args.warmup < 0 or not pipelines or args.keyspace < 10 or
            args.timeout <= 0 or args.bgsave_delay < 0 or
            args.connections > 1024 or
            any(value < 1 or value > 1024 for value in pipelines) or
            args.requests < args.connections):
        parser.error("numeric benchmark arguments are out of range")
    redis_version = (verify_redis_version(parser, args.redis_server)
                     if "redis" in targets else "not selected")
    cases = build_cases(targets, policies, workloads, args.repeats, pipelines,
                        scenarios)
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
                             pipeline, repeat, scenario) in enumerate(cases, 1):
                started = time.monotonic()
                print(
                    f"[{run_number}/{len(cases)}] START target={target} "
                    f"policy={policy} scenario={scenario} workload={workload} "
                    f"pipeline={pipeline} "
                    f"repeat={repeat}/{args.repeats}",
                    flush=True,
                )
                port = 6380 if target == "redis" else 9096
                aof_path = temp / (
                    f"{target}-{policy}-{scenario}-{workload}-p{pipeline}-{repeat}.aof"
                )
                rdb_path = temp / (
                    f"{target}-{policy}-{scenario}-{workload}-p{pipeline}-{repeat}.rdb"
                )
                process = subprocess.Popen(
                    server_command(server_engine, args, port, policy, aof_path,
                                   scenario, rdb_path),
                    stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
                )
                metrics = None
                rss_kb = 0
                logical_memory = 0
                actual_cardinality = 0
                aof_queue_bytes = 0
                aof_backpressure_events = 0
                bgsave_values = {}
                rdb_recovery_cardinality = 0
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
                    if scenario == "bgsave":
                        client_process = subprocess.Popen(
                            client_command, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True)
                        try:
                            wait_and_trigger_bgsave(
                                port, target, workload, args.keyspace,
                                args.seed, args.bgsave_delay, client_process)
                            stdout, stderr = client_process.communicate(
                                timeout=args.timeout)
                        except BaseException:
                            client_process.kill()
                            client_process.wait(timeout=3.0)
                            raise
                        if client_process.returncode != 0:
                            raise RuntimeError(
                                f"client failed with {client_process.returncode}: {stderr}")
                        bgsave_values = wait_bgsave_complete(
                            port, target == "redis")
                        client_stdout = stdout
                    else:
                        result = subprocess.run(
                            client_command, check=True, capture_output=True,
                            text=True, timeout=args.timeout)
                        client_stdout = result.stdout
                    metrics = parse_metrics(client_stdout)
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
                    actual_cardinality = workload_cardinality(
                        port, workload, metrics["dataset_key"])
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
                if scenario == "bgsave":
                    recovery = subprocess.Popen(
                        recovery_server_command(target, server_engine, args,
                                                port, rdb_path),
                        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                        text=True)
                    try:
                        wait_port(port, recovery)
                        rdb_recovery_cardinality = workload_cardinality(
                            port, workload, metrics["dataset_key"])
                        if rdb_recovery_cardinality != args.keyspace:
                            raise RuntimeError(
                                "BGSAVE RDB recovery cardinality mismatch: "
                                f"expected {args.keyspace}, got "
                                f"{rdb_recovery_cardinality}")
                    finally:
                        stop(recovery)
                row = {
                    "target": target, "policy": policy, "scenario": scenario,
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
                    "bgsave_duration_us": (
                        bgsave_values.get(b"rdb_last_save_duration_us", 0)
                        if target != "redis" else
                        bgsave_values.get(b"rdb_last_bgsave_time_sec", 0) * 1_000_000),
                    "fork_pause_us": bgsave_values.get(
                        b"rdb_last_fork_pause_us", 0),
                    "child_peak_rss_kb": bgsave_values.get(
                        b"rdb_last_child_peak_rss_kb", 0),
                    "child_minor_faults": bgsave_values.get(
                        b"rdb_last_child_minor_faults", 0),
                    "child_major_faults": bgsave_values.get(
                        b"rdb_last_child_major_faults", 0),
                    "cow_bytes": bgsave_values.get(b"rdb_last_cow_size", 0),
                    "rdb_recovery_cardinality": rdb_recovery_cardinality,
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
        key = (row["target"], row["policy"], row["scenario"], row["workload"],
               row["pipeline"])
        grouped.setdefault(key, []).append(row)
    summary_fields = (
        "target", "policy", "scenario", "workload", "connections", "pipeline", "rounds",
        "operation_mix", "payload_kind", "value_bytes",
        "qps_median", "qps_cv_percent", "cv_valid",
        "p50_us_median", "p95_us_median", "p99_us_median",
        "p999_us_median", "max_us_median", "errors_total",
        "actual_final_cardinality", "aof_backpressure_events_max",
        "bgsave_duration_us_median", "fork_pause_us_median",
        "child_peak_rss_kb_max", "child_minor_faults_median",
        "child_major_faults_median", "cow_bytes_median",
        "rdb_recovery_cardinality",
    )
    summary_output.parent.mkdir(parents=True, exist_ok=True)
    invalid_cv_groups = 0
    with summary_output.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=summary_fields)
        writer.writeheader()
        for key, values in sorted(grouped.items()):
            target, policy, scenario, workload, pipeline = key
            qps_values = [float(v["qps"]) for v in values]
            qps_mean = statistics.fmean(qps_values)
            qps_cv = (0.0 if len(qps_values) < 2 or qps_mean == 0 else
                      statistics.pstdev(qps_values) / qps_mean * 100.0)
            if qps_cv > 5.0:
                invalid_cv_groups += 1
            writer.writerow({
                "target": target, "policy": policy, "scenario": scenario,
                "workload": workload,
                "connections": args.connections, "pipeline": pipeline,
                "rounds": len(values),
                "operation_mix": values[-1]["operation_mix"],
                "payload_kind": values[-1]["payload_kind"],
                "value_bytes": values[-1]["value_bytes"],
                "qps_median": statistics.median(qps_values),
                "qps_cv_percent": qps_cv,
                "cv_valid": int(qps_cv <= 5.0),
                "p50_us_median": statistics.median(float(v["p50_us"]) for v in values),
                "p95_us_median": statistics.median(float(v["p95_us"]) for v in values),
                "p99_us_median": statistics.median(float(v["p99_us"]) for v in values),
                "p999_us_median": statistics.median(float(v["p999_us"]) for v in values),
                "max_us_median": statistics.median(float(v["max_us"]) for v in values),
                "errors_total": sum(int(v["errors"]) for v in values),
                "actual_final_cardinality": values[-1]["actual_final_cardinality"],
                "aof_backpressure_events_max": max(
                    int(v["aof_backpressure_events"]) for v in values),
                "bgsave_duration_us_median": statistics.median(
                    int(v["bgsave_duration_us"]) for v in values),
                "fork_pause_us_median": statistics.median(
                    int(v["fork_pause_us"]) for v in values),
                "child_peak_rss_kb_max": max(
                    int(v["child_peak_rss_kb"]) for v in values),
                "child_minor_faults_median": statistics.median(
                    int(v["child_minor_faults"]) for v in values),
                "child_major_faults_median": statistics.median(
                    int(v["child_major_faults"]) for v in values),
                "cow_bytes_median": statistics.median(
                    int(v["cow_bytes"]) for v in values),
                "rdb_recovery_cardinality": min(
                    int(v["rdb_recovery_cardinality"]) for v in values),
            })
    print(f"completed {len(cases)} runs; wrote {output} and {summary_output}",
          flush=True)
    if invalid_cv_groups:
        print(f"invalid groups with QPS CV > 5%: {invalid_cv_groups}; rerun them",
              flush=True)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

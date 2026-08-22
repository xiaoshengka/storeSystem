#!/usr/bin/env python3
"""Run v0.7.0 hot-hit and hot-write MySQL performance gates."""

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
import sys
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))
import mysql_cold_miss_bench as common


RAW_FIELDS = [
    "scenario", "target", "pipeline", "run", "connections", "requests",
    "keyspace", "value_bytes", "duration_seconds", "qps", "p50_us",
    "p99_us", "p999_us", "max_us", "get_completed", "set_completed",
    "set_errors", "server_keys_before", "server_keys_after",
    "mysql_loads_delta", "mysql_submitted_delta", "mysql_applied_delta",
    "mysql_write_errors_delta", "validation",
]
SUMMARY_FIELDS = [
    "scenario", "target", "pipeline", "runs", "qps_median",
    "qps_cv_percent", "p50_us_median", "p99_us_median",
    "p999_us_median", "max_us_median", "validation",
]


def parse_csv_numbers(text: str) -> Tuple[int, ...]:
    try:
        values = tuple(int(item) for item in text.split(","))
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected comma-separated integers") from error
    if not values or any(value <= 0 for value in values):
        raise argparse.ArgumentTypeError("values must be positive")
    return values


def parse_scenarios(text: str) -> Tuple[str, ...]:
    allowed = {"hot-get", "hot-write"}
    values = tuple(item.strip() for item in text.split(",") if item.strip())
    if not values or any(value not in allowed for value in values):
        raise argparse.ArgumentTypeError("use hot-get,hot-write")
    return values


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare MySQL disabled/enabled hot-cache paths and enforce v0.7.0 gates."
    )
    parser.add_argument("--requests", type=int, default=1_000_000)
    parser.add_argument("--connections", type=int, default=32)
    parser.add_argument("--pipelines", type=parse_csv_numbers, default=(16, 64))
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--keyspace", type=int, default=100_000)
    parser.add_argument("--warmup", type=int, default=1_000)
    parser.add_argument("--run-id", type=int, default=7100000)
    parser.add_argument("--scenarios", type=parse_scenarios,
                        default=("hot-get", "hot-write"))
    parser.add_argument("--max-cv-percent", type=float, default=5.0)
    parser.add_argument("--mysql-host", default=os.getenv("KVSTORE_MYSQL_TEST_HOST", "127.0.0.1"))
    parser.add_argument("--mysql-port", type=int,
                        default=int(os.getenv("KVSTORE_MYSQL_TEST_PORT", "3306")))
    parser.add_argument("--mysql-user", default=os.getenv("KVSTORE_MYSQL_TEST_USER", "kvstore"))
    parser.add_argument("--mysql-database",
                        default=os.getenv("KVSTORE_MYSQL_TEST_DATABASE", "kvstore_test"))
    parser.add_argument("--mysql-read-workers", type=int, default=8)
    parser.add_argument("--bootstrap-timeout", type=float, default=7200.0)
    parser.add_argument("--run-timeout", type=float, default=1800.0)
    parser.add_argument("--writer-timeout", type=float, default=300.0)
    parser.add_argument(
        "--output", type=Path,
        default=Path("bench/result/v0.7.0-mysql-performance.csv"),
    )
    args = parser.parse_args()
    if args.requests < args.connections or args.requests % 10 != 0:
        parser.error("--requests must be >= connections and a multiple of 10")
    if not 1 <= args.connections <= 1024:
        parser.error("--connections must be in [1, 1024]")
    if any(pipeline > 1024 for pipeline in args.pipelines):
        parser.error("pipelines must be <= 1024")
    if args.repeats < 1 or args.keyspace < 1 or args.warmup < 0:
        parser.error("repeats/keyspace must be positive and warmup non-negative")
    if not args.mysql_database.endswith("_test"):
        parser.error("benchmark resets data; MySQL database must end in _test")
    return args


def ping_server() -> None:
    with socket.create_connection(("127.0.0.1", common.SERVER_PORT), timeout=1.0) as client:
        client.settimeout(2.0)
        client.sendall(common.encode_command(b"PING"))
        if client.recv(7) != b"+PONG\r\n":
            raise RuntimeError("unexpected PING response")


def wait_for_off_server(process: subprocess.Popen, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(
                f"baseline kvstore exited with {process.returncode}\n"
                f"stdout={stdout}\nstderr={stderr}"
            )
        try:
            ping_server()
            return
        except (ConnectionError, OSError, RuntimeError):
            time.sleep(0.05)
    raise TimeoutError("baseline kvstore did not become ready")


def start_off_server(aof_path: Path) -> subprocess.Popen:
    process = subprocess.Popen(
        [
            "./kvstore", "--maxmemory", "0", "--maxkeys", "0",
            "--appendonly", "yes", "--appendfilename", str(aof_path),
            "--appendfsync", "everysec", "--mysql", "no",
        ],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    try:
        wait_for_off_server(process)
    except BaseException:
        common.stop_server(process, allow_failure=True)
        raise
    return process


def copy_seed(seed: Path, root: Path, name: str) -> Path:
    target = root / f"{name}.aof"
    shutil.copyfile(seed, target)
    return target


def bootstrap_database(args: argparse.Namespace, seed: Path, root: Path, label: str) -> None:
    common.reset_database(args)
    bootstrap_aof = copy_seed(seed, root, f"bootstrap-{label}")
    process = common.start_server(args, bootstrap_aof, args.bootstrap_timeout)
    common.stop_server(process)
    keys = int(common.mysql_command(args, "SELECT COUNT(*) FROM kv_keys"))
    state = common.mysql_command(
        args, "SELECT bootstrap_state FROM kv_schema_meta WHERE singleton_id=1"
    )
    if keys != args.keyspace or state != "READY":
        raise RuntimeError(
            f"bootstrap failed for {label}: state={state}, keys={keys}, "
            f"expected={args.keyspace}"
        )


def wait_for_writer(timeout: float) -> Dict[str, int]:
    deadline = time.monotonic() + timeout
    last: Dict[str, int] = {}
    while time.monotonic() < deadline:
        last = common.exchange_info()
        if (last["mysql_submitted_sequence"] == last["mysql_applied_sequence"] and
                last["mysql_pending_write_bytes"] == 0):
            return last
        time.sleep(0.02)
    raise TimeoutError(f"MySQL writer did not drain: {last}")


def counter_delta(before: Dict[str, int], after: Dict[str, int], name: str) -> int:
    return after[name] - before[name]


def client_command(args: argparse.Namespace, scenario: str, pipeline: int) -> List[str]:
    mode = "-G" if scenario == "hot-get" else "-N"
    return [
        "./mixed_qps_client", "-s", "127.0.0.1", "-p", str(common.SERVER_PORT),
        "-c", str(args.connections), "-n", str(args.requests),
        "-w", str(args.warmup), "-P", str(pipeline),
        "-k", str(args.keyspace), "-R", str(args.run_id), mode, "-L",
    ]


def run_client(args: argparse.Namespace, scenario: str, pipeline: int) -> Dict[str, str]:
    result = subprocess.run(
        client_command(args, scenario, pipeline), text=True, capture_output=True,
        timeout=args.run_timeout, check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"mixed_qps_client failed with {result.returncode}\n"
            f"stdout={result.stdout}\nstderr={result.stderr}"
        )
    return common.parse_metrics(result.stdout)


def validate_client(args: argparse.Namespace, scenario: str,
                    metrics: Dict[str, str]) -> None:
    expected_sets = 0 if scenario == "hot-get" else args.requests // 10
    expected_gets = args.requests - expected_sets
    checks = {
        "requests": int(metrics.get("requests_completed", -1)) == args.requests,
        "gets": int(metrics.get("get_completed", -1)) == expected_gets,
        "sets": int(metrics.get("set_completed", -1)) == expected_sets,
        "set_errors": int(metrics.get("set_errors", -1)) == 0,
        "values_found": int(metrics.get("get_hits", -1)) == expected_gets,
        "cache_preseeded": int(metrics.get("server_keys_before", -1)) == args.keyspace,
        "cache_cardinality": int(metrics.get("server_keys_after", -1)) == args.keyspace,
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise RuntimeError(f"invalid {scenario} client result: {', '.join(failed)}")


def result_row(args: argparse.Namespace, scenario: str, target: str,
               pipeline: int, run: int, metrics: Dict[str, str],
               before: Dict[str, int] = None,
               after: Dict[str, int] = None) -> Dict[str, object]:
    mysql = target == "mysql-on"
    return {
        "scenario": scenario,
        "target": target,
        "pipeline": pipeline,
        "run": run,
        "connections": args.connections,
        "requests": args.requests,
        "keyspace": args.keyspace,
        "value_bytes": len(common.VALUE_64),
        "duration_seconds": metrics["duration_seconds"],
        "qps": metrics["qps"],
        "p50_us": metrics["latency_p50_us"],
        "p99_us": metrics["latency_p99_us"],
        "p999_us": metrics["latency_p999_us"],
        "max_us": metrics["latency_max_us"],
        "get_completed": metrics["get_completed"],
        "set_completed": metrics["set_completed"],
        "set_errors": metrics["set_errors"],
        "server_keys_before": metrics["server_keys_before"],
        "server_keys_after": metrics["server_keys_after"],
        "mysql_loads_delta": counter_delta(before, after, "mysql_loads") if mysql else 0,
        "mysql_submitted_delta": (
            counter_delta(before, after, "mysql_submitted_sequence") if mysql else 0
        ),
        "mysql_applied_delta": (
            counter_delta(before, after, "mysql_applied_sequence") if mysql else 0
        ),
        "mysql_write_errors_delta": (
            counter_delta(before, after, "mysql_write_errors") if mysql else 0
        ),
        "validation": "PASS",
    }


def run_target(args: argparse.Namespace, root: Path, seed: Path, scenario: str,
               target: str, pipeline: int, run: int) -> Dict[str, object]:
    label = f"{scenario}-{target}-p{pipeline}-r{run}"
    run_aof = copy_seed(seed, root, label)
    process = (common.start_server(args, run_aof, 30.0)
               if target == "mysql-on" else start_off_server(run_aof))
    before: Dict[str, int] = {}
    after: Dict[str, int] = {}
    try:
        if target == "mysql-on":
            before = common.exchange_info()
        metrics = run_client(args, scenario, pipeline)
        validate_client(args, scenario, metrics)
        if target == "mysql-on":
            after = wait_for_writer(args.writer_timeout)
            loads = counter_delta(before, after, "mysql_loads")
            writes = counter_delta(before, after, "mysql_submitted_sequence")
            expected_writes = 0 if scenario == "hot-get" else args.requests // 10
            if (loads != 0 or writes != expected_writes or
                    counter_delta(before, after, "mysql_write_errors") != 0):
                raise RuntimeError(
                    f"invalid MySQL deltas: loads={loads}, writes={writes}, "
                    f"expected_writes={expected_writes}, before={before}, after={after}"
                )
    finally:
        common.stop_server(process)
    return result_row(args, scenario, target, pipeline, run, metrics, before, after)


def write_rows(path: Path, fields: List[str], rows: List[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def median(rows: List[Dict[str, object]], field: str) -> float:
    return statistics.median(float(row[field]) for row in rows)


def summarize(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    result: List[Dict[str, object]] = []
    groups = sorted({(row["scenario"], row["target"], row["pipeline"]) for row in rows})
    for scenario, target, pipeline in groups:
        group = [row for row in rows if (row["scenario"], row["target"], row["pipeline"]) ==
                 (scenario, target, pipeline)]
        qps = [float(row["qps"]) for row in group]
        result.append({
            "scenario": scenario,
            "target": target,
            "pipeline": pipeline,
            "runs": len(group),
            "qps_median": statistics.median(qps),
            "qps_cv_percent": (
                statistics.pstdev(qps) * 100.0 / statistics.mean(qps)
                if len(qps) > 1 else 0.0
            ),
            "p50_us_median": median(group, "p50_us"),
            "p99_us_median": median(group, "p99_us"),
            "p999_us_median": median(group, "p999_us"),
            "max_us_median": median(group, "max_us"),
            "validation": "PASS",
        })
    return result


def evaluate_gate(args: argparse.Namespace,
                  summaries: List[Dict[str, object]]) -> Dict[str, object]:
    limits = {
        "hot-get": {"qps_loss_percent": 5.0, "p99_growth_percent": 10.0},
        "hot-write": {"qps_loss_percent": 10.0, "p99_growth_percent": 20.0},
    }
    checks = []
    for scenario in args.scenarios:
        for pipeline in args.pipelines:
            baseline = next(row for row in summaries if row["scenario"] == scenario and
                            row["target"] == "mysql-off" and row["pipeline"] == pipeline)
            enabled = next(row for row in summaries if row["scenario"] == scenario and
                           row["target"] == "mysql-on" and row["pipeline"] == pipeline)
            qps_change = (float(enabled["qps_median"]) /
                          float(baseline["qps_median"]) - 1.0) * 100.0
            p99_change = (float(enabled["p99_us_median"]) /
                          float(baseline["p99_us_median"]) - 1.0) * 100.0
            cv_valid = (float(baseline["qps_cv_percent"]) <= args.max_cv_percent and
                        float(enabled["qps_cv_percent"]) <= args.max_cv_percent)
            passed = (cv_valid and
                      qps_change >= -limits[scenario]["qps_loss_percent"] and
                      p99_change <= limits[scenario]["p99_growth_percent"])
            checks.append({
                "scenario": scenario,
                "pipeline": pipeline,
                "baseline_qps": baseline["qps_median"],
                "mysql_qps": enabled["qps_median"],
                "qps_change_percent": qps_change,
                "baseline_p99_us": baseline["p99_us_median"],
                "mysql_p99_us": enabled["p99_us_median"],
                "p99_change_percent": p99_change,
                "cv_valid": cv_valid,
                "passed": passed,
            })
    return {"passed": all(check["passed"] for check in checks), "checks": checks}


def main() -> int:
    args = parse_args()
    if os.getenv("KVSTORE_MYSQL_TEST_PASSWORD") is None:
        raise RuntimeError("set KVSTORE_MYSQL_TEST_PASSWORD")
    if shutil.which("mysql") is None:
        raise RuntimeError("mysql client is required")
    for executable in (Path("kvstore"), Path("mixed_qps_client")):
        if not executable.is_file():
            raise RuntimeError(f"missing ./{executable}; build with MYSQL=1 first")
    common.ensure_server_port_free()
    output = args.output.resolve()
    summary_path = output.with_name(output.stem + ".summary.csv")
    gate_path = output.with_name(output.stem + ".gate.json")
    metadata_path = output.with_name(output.stem + ".metadata.json")
    rows: List[Dict[str, object]] = []

    with tempfile.TemporaryDirectory(prefix="kvstore-mysql-gate-") as directory:
        root = Path(directory)
        seed = root / "seed.aof"
        print(f"[seed] {args.keyspace} stable 64B String keys", flush=True)
        common.write_seed_aof(seed, args.run_id, args.keyspace)

        if "hot-get" in args.scenarios:
            print("[bootstrap] preparing READY database for hot-get", flush=True)
            bootstrap_database(args, seed, root, "hot-get")
        for scenario in args.scenarios:
            for pipeline in args.pipelines:
                for run in range(1, args.repeats + 1):
                    for target in ("mysql-off", "mysql-on"):
                        if scenario == "hot-write" and target == "mysql-on":
                            print(f"[bootstrap] reset before {scenario}/P{pipeline}/run{run}",
                                  flush=True)
                            bootstrap_database(args, seed, root,
                                               f"write-p{pipeline}-r{run}")
                        print(f"[{scenario}/P{pipeline}/run{run}] {target}", flush=True)
                        row = run_target(args, root, seed, scenario, target, pipeline, run)
                        rows.append(row)
                        write_rows(output, RAW_FIELDS, rows)
                        print(f"  qps={float(row['qps']):.2f} "
                              f"p99={float(row['p99_us']) / 1000.0:.3f} ms PASS",
                              flush=True)

    summaries = summarize(rows)
    write_rows(summary_path, SUMMARY_FIELDS, summaries)
    gate = evaluate_gate(args, summaries)
    with gate_path.open("w", encoding="utf-8") as output_file:
        json.dump(gate, output_file, indent=2)
        output_file.write("\n")
    metadata = {
        "benchmark": "v0.7.0 MySQL hot-cache performance gate",
        "platform": platform.platform(),
        "client_server_topology": "same host",
        "mysql_host": args.mysql_host,
        "mysql_port": args.mysql_port,
        "mysql_read_workers": args.mysql_read_workers,
        "connections": args.connections,
        "requests": args.requests,
        "keyspace": args.keyspace,
        "pipelines": args.pipelines,
        "repeats": args.repeats,
        "compiler": common.capture_command(["cc", "--version"]),
        "mysql_client": common.capture_command(["mysql", "--version"]),
        "cpu": common.capture_command(["lscpu"]),
        "memory": common.capture_command(["free", "-h"]),
        "storage": common.capture_command(
            ["lsblk", "-o", "NAME,MODEL,SIZE,ROTA,TYPE,MOUNTPOINTS"]
        ),
        "git_commit": common.capture_command(["git", "rev-parse", "HEAD"]),
        "command": " ".join(os.sys.argv),
    }
    with metadata_path.open("w", encoding="utf-8") as output_file:
        json.dump(metadata, output_file, ensure_ascii=False, indent=2)
        output_file.write("\n")
    print(f"raw_csv: {output}")
    print(f"summary_csv: {summary_path}")
    print(f"gate_json: {gate_path}")
    print(f"gate: {'PASS' if gate['passed'] else 'FAIL'}")
    return 0 if gate["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())

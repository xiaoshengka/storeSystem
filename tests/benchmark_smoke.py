#!/usr/bin/env python3
import signal
import socket
import subprocess
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace

from reactor_integration import RespReader, encode_command, wait_for_server


def parse_metrics(output: str):
    result = {}
    for line in output.splitlines():
        if ": " in line:
            key, value = line.split(": ", 1)
            result[key] = value
    return result


def stop_server(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2.0)
    process.communicate()


def exchange(*arguments: bytes):
    with socket.create_connection(("127.0.0.1", 9096), timeout=3.0) as client:
        client.sendall(encode_command(*arguments))
        return RespReader(client).read()


def load_collection_compare_module():
    path = Path("bench/redis_collection_compare.py")
    spec = spec_from_file_location("redis_collection_compare", path)
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_collection_compare_matrix() -> None:
    module = load_collection_compare_module()
    cases = module.build_cases(
        ("skiplist", "rbtree", "redis"),
        ("off", "no", "everysec"),
        module.ALL_WORKLOADS,
        5,
    )
    assert len(cases) == 190
    hash_cases = [case for case in cases if case[3] in module.HASH_WORKLOADS]
    local_hash_cases = [case for case in hash_cases if case[0] != "redis"]
    assert local_hash_cases
    assert {case[0] for case in local_hash_cases} == {"hash"}
    assert all(case[1] == "skiplist" for case in local_hash_cases)

    redis_command = module.server_command(
        "redis",
        SimpleNamespace(redis_server="/usr/local/bin/redis-server"),
        6380,
        "everysec",
        Path("/tmp/collection-smoke.aof"),
    )
    rewrite_option = redis_command.index("--auto-aof-rewrite-percentage")
    assert redis_command[rewrite_option + 1] == "0"


def test_latency_metrics() -> None:
    server = subprocess.Popen(
        ["./kvstore", "--maxmemory", "4MiB", "--maxkeys", "4096"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_for_server(server)
        baseline = subprocess.run(
            [
                "./mixed_qps_client",
                "-c", "2",
                "-n", "1000",
                "-w", "10",
                "-P", "4",
                "-k", "100",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=30.0,
        )
        result = subprocess.run(
            [
                "./mixed_qps_client",
                "-c", "2",
                "-n", "1000",
                "-w", "10",
                "-P", "4",
                "-k", "100",
                "-L",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=30.0,
        )
    finally:
        stop_server(server)

    baseline_metrics = parse_metrics(baseline.stdout)
    assert baseline_metrics["latency_measurement"] == "disabled"
    assert "latency_samples" not in baseline_metrics
    metrics = parse_metrics(result.stdout)
    assert metrics["latency_measurement"] == "enabled"
    assert int(metrics["latency_samples"]) == 1000
    assert int(metrics["get_latency_samples"]) == 900
    assert int(metrics["set_latency_samples"]) == 100
    for name in (
        "latency_mean_us",
        "latency_p50_us",
        "latency_p95_us",
        "latency_p99_us",
        "latency_p999_us",
        "latency_max_us",
        "get_latency_p99_us",
        "set_latency_p99_us",
    ):
        assert float(metrics[name]) >= 0.0
    assert float(metrics["latency_p50_us"]) <= float(metrics["latency_p99_us"])
    assert float(metrics["latency_p99_us"]) <= float(metrics["latency_max_us"])


def test_collection_metrics() -> None:
    for engine in ("skiplist", "rbtree"):
        server = subprocess.Popen(
            ["./kvstore", "--zset-engine", engine, "--maxkeys", "4096"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            wait_for_server(server)
            result = subprocess.run(
                ["./collection_bench_client", "-m", "zset-mixed",
                 "-c", "2", "-n", "500", "-w", "10", "-P", "8",
                 "-k", "100", "-C"],
                check=True, capture_output=True, text=True, timeout=30.0,
            )
            metrics = parse_metrics(result.stdout)
            assert metrics["keyspace_scope"] == "shared"
            assert metrics["dataset_key"] == "collection:zset:20260820"
            assert exchange(b"ZCARD", metrics["dataset_key"].encode()) == (
                "integer", 100
            )
            assert exchange(b"DEL", metrics["dataset_key"].encode()) == (
                "integer", 1
            )

            inserted = subprocess.run(
                ["./collection_bench_client", "-m", "zset-insert",
                 "-c", "2", "-n", "500", "-w", "10", "-P", "8",
                 "-k", "100", "-S", "20260821", "-C"],
                check=True, capture_output=True, text=True, timeout=30.0,
            )
            insert_metrics = parse_metrics(inserted.stdout)
            assert exchange(b"ZCARD", insert_metrics["dataset_key"].encode()) == (
                "integer", 500
            )
            assert exchange(b"DEL", insert_metrics["dataset_key"].encode()) == (
                "integer", 1
            )

            hashed = subprocess.run(
                ["./collection_bench_client", "-m", "hash-mixed",
                 "-c", "2", "-n", "200", "-w", "10", "-P", "8",
                 "-k", "100", "-S", "20260822", "-C"],
                check=True, capture_output=True, text=True, timeout=30.0,
            )
            hash_metrics = parse_metrics(hashed.stdout)
            assert exchange(b"HLEN", hash_metrics["dataset_key"].encode()) == (
                "integer", 100
            )
            all_fields = exchange(b"HGETALL", hash_metrics["dataset_key"].encode())
            assert all_fields[0] == "array"
            values = [item[1] for item in all_fields[1][1::2]]
            base_value = (
                b"0123456789abcdef0123456789abcdef"
                b"0123456789abcdef0123456789abcdef"
            )
            assert all(len(value) == 64 for value in values)
            assert any(value != base_value for value in values)
            assert exchange(b"DEL", hash_metrics["dataset_key"].encode()) == (
                "integer", 1
            )

            ranged = subprocess.run(
                ["./collection_bench_client", "-m", "zrange",
                 "-c", "2", "-n", "100", "-w", "5", "-P", "8",
                 "-k", "100", "-S", "20260823"],
                check=True, capture_output=True, text=True, timeout=30.0,
            )
            assert int(parse_metrics(ranged.stdout)["errors"]) == 0
        finally:
            stop_server(server)
        assert int(metrics["completed"]) == 500
        assert int(metrics["errors"]) == 0
        assert float(metrics["latency_p50_us"]) <= float(metrics["latency_p99_us"])


def test_replay_benchmark() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "bench/aof_replay_bench.py",
            "--sizes", "100",
            "--repeats", "2",
            "--value-size", "8",
            "--timeout", "10",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=30.0,
    )
    assert "summary commands=100" in result.stdout
    assert "replay_p50=" in result.stdout
    assert "rate=" in result.stdout


def test_latency_benchmark_script() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "bench/aof_latency_bench.py",
            "--policies", "off,no",
            "--repeats", "1",
            "--connections", "2",
            "--requests", "1000",
            "--warmup", "10",
            "--pipeline", "4",
            "--keyspace", "100",
            "--maxmemory", "4MiB",
            "--timeout", "30",
            "--build-label", "smoke",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=60.0,
    )
    assert "summary policy=off" in result.stdout
    assert "summary policy=no" in result.stdout
    assert "p99_median_us=" in result.stdout


def main() -> int:
    test_collection_compare_matrix()
    test_latency_metrics()
    test_collection_metrics()
    test_replay_benchmark()
    test_latency_benchmark_script()
    print("benchmark_smoke: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

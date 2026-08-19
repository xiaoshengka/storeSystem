#!/usr/bin/env python3
import signal
import subprocess
import sys

from reactor_integration import wait_for_server


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
    test_latency_metrics()
    test_replay_benchmark()
    test_latency_benchmark_script()
    print("benchmark_smoke: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

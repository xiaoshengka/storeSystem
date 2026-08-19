#!/usr/bin/env python3
"""Generate deterministic AOF files and measure kvstore cold-start replay."""

import argparse
import csv
import math
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Dict, List


HOST = "127.0.0.1"
PORT = 9096
REPLAY_SECONDS = re.compile(r"^aof_replay_duration_seconds: ([0-9.]+)$", re.M)
REPLAY_COMMANDS = re.compile(r"^aof_replay_commands: ([0-9]+)$", re.M)


def parse_sizes(text: str) -> List[int]:
    result = []
    for item in text.split(","):
        try:
            value = int(item)
        except ValueError as error:
            raise argparse.ArgumentTypeError("sizes must be comma-separated integers") from error
        if value <= 0:
            raise argparse.ArgumentTypeError("all sizes must be positive")
        result.append(value)
    if not result:
        raise argparse.ArgumentTypeError("at least one size is required")
    return result


def percentile(values: List[float], quantile: float) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil(quantile * len(ordered)) - 1)
    return ordered[index]


def encode_set(index: int, value: bytes) -> bytes:
    key = f"replay:{index}".encode("ascii")
    return (
        b"*3\r\n$3\r\nSET\r\n$"
        + str(len(key)).encode("ascii")
        + b"\r\n"
        + key
        + b"\r\n$"
        + str(len(value)).encode("ascii")
        + b"\r\n"
        + value
        + b"\r\n"
    )


def generate_aof(path: Path, command_count: int, value_size: int) -> int:
    value = b"x" * value_size
    pending = bytearray()
    with path.open("wb") as output:
        for index in range(command_count):
            pending.extend(encode_set(index, value))
            if len(pending) >= 1024 * 1024:
                output.write(pending)
                pending.clear()
        if pending:
            output.write(pending)
    return path.stat().st_size


def port_is_open() -> bool:
    try:
        with socket.create_connection((HOST, PORT), timeout=0.1):
            return True
    except OSError:
        return False


def wait_until_ready(
    process: subprocess.Popen,
    timeout: float,
    start: float,
) -> float:
    deadline = start + timeout
    while time.perf_counter() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(
                f"server exited with status {process.returncode}\n{stdout}\n{stderr}"
            )
        if port_is_open():
            return time.perf_counter() - start
        time.sleep(0.005)
    process.kill()
    stdout, stderr = process.communicate()
    raise TimeoutError(f"server did not become ready\n{stdout}\n{stderr}")


def peak_rss_kib(pid: int) -> int:
    try:
        status = Path(f"/proc/{pid}/status").read_text(encoding="ascii")
    except OSError:
        return 0
    match = re.search(r"^VmHWM:\s+([0-9]+)\s+kB$", status, re.M)
    return int(match.group(1)) if match else 0


def run_once(server: str, aof_path: Path, timeout: float) -> Dict[str, float]:
    command = [
        server,
        "--appendonly", "yes",
        "--appendfilename", str(aof_path),
        "--appendfsync", "no",
        "--maxmemory", "0",
        "--maxkeys", "0",
    ]
    startup_start = time.perf_counter()
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        ready_seconds = wait_until_ready(process, timeout, startup_start)
        rss_kib = peak_rss_kib(process.pid)
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=10.0)
    except BaseException:
        if process.poll() is None:
            process.kill()
            process.communicate()
        raise
    if process.returncode != 0:
        raise RuntimeError(
            f"server stopped with status {process.returncode}\n{stdout}\n{stderr}"
        )
    replay_match = REPLAY_SECONDS.search(stderr)
    commands_match = REPLAY_COMMANDS.search(stderr)
    if replay_match is None or commands_match is None:
        raise RuntimeError(f"missing replay metrics in server log\n{stdout}\n{stderr}")
    return {
        "startup_ready_seconds": ready_seconds,
        "replay_seconds": float(replay_match.group(1)),
        "commands_loaded": int(commands_match.group(1)),
        "peak_rss_kib": rss_kib,
    }


def write_csv(path: Path, rows: List[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="./kvstore", help="kvstore executable")
    parser.add_argument(
        "--sizes",
        type=parse_sizes,
        default=parse_sizes("10000,100000,1000000"),
        help="comma-separated AOF command counts",
    )
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--value-size", type=int, default=64)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--csv", type=Path, help="optional raw-result CSV path")
    parser.add_argument("--aof-dir", type=Path, help="keep/generated AOF directory")
    arguments = parser.parse_args()

    if arguments.repeats <= 0 or arguments.value_size < 0 or arguments.value_size > 60000:
        parser.error("repeats must be positive and value-size must be in 0..60000")
    if port_is_open():
        parser.error("port 9096 is already in use; stop the running kvstore first")
    if not Path(arguments.server).is_file():
        parser.error(f"server executable not found: {arguments.server}")

    temporary = None
    if arguments.aof_dir is None:
        temporary = tempfile.TemporaryDirectory(prefix="storeSystem-replay-bench-")
        aof_directory = Path(temporary.name)
    else:
        aof_directory = arguments.aof_dir
        aof_directory.mkdir(parents=True, exist_ok=True)

    rows: List[Dict[str, object]] = []
    try:
        for command_count in arguments.sizes:
            aof_path = aof_directory / f"replay-{command_count}.aof"
            aof_bytes = generate_aof(aof_path, command_count, arguments.value_size)
            print(f"generated commands={command_count} bytes={aof_bytes}")
            size_rows = []
            for run in range(1, arguments.repeats + 1):
                metrics = run_once(arguments.server, aof_path, arguments.timeout)
                if metrics["commands_loaded"] != command_count:
                    raise RuntimeError(
                        f"expected {command_count} commands, loaded {metrics['commands_loaded']}"
                    )
                replay_seconds = metrics["replay_seconds"]
                row: Dict[str, object] = {
                    "commands": command_count,
                    "aof_bytes": aof_bytes,
                    "value_bytes": arguments.value_size,
                    "run": run,
                    "replay_seconds": f"{replay_seconds:.9f}",
                    "startup_ready_seconds": f"{metrics['startup_ready_seconds']:.9f}",
                    "commands_per_second": f"{command_count / replay_seconds:.2f}",
                    "mib_per_second": f"{aof_bytes / replay_seconds / (1024 * 1024):.2f}",
                    "peak_rss_kib": int(metrics["peak_rss_kib"]),
                }
                rows.append(row)
                size_rows.append(metrics)
                print(
                    f"run={run} replay={replay_seconds:.6f}s "
                    f"ready={metrics['startup_ready_seconds']:.6f}s "
                    f"rate={command_count / replay_seconds:.2f} cmd/s "
                    f"peak_rss={int(metrics['peak_rss_kib'])} KiB"
                )
            replay_values = [float(item["replay_seconds"]) for item in size_rows]
            ready_values = [float(item["startup_ready_seconds"]) for item in size_rows]
            print(
                "summary "
                f"commands={command_count} "
                f"replay_min={min(replay_values):.6f}s "
                f"replay_p50={percentile(replay_values, 0.50):.6f}s "
                f"replay_p95={percentile(replay_values, 0.95):.6f}s "
                f"replay_max={max(replay_values):.6f}s "
                f"ready_p50={percentile(ready_values, 0.50):.6f}s"
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

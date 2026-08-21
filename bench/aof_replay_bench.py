#!/usr/bin/env python3
"""Benchmark deterministic String/Hash/ZSet RESP2 AOF replay against Redis 6.2.23."""

import argparse
import csv
import math
import re
import shutil
import signal
import socket
import statistics
import subprocess
import tempfile
import time
from pathlib import Path


PROJECT_REPLAY = re.compile(r"^aof_replay_duration_seconds: ([0-9.]+)$", re.M)
PROJECT_COMMANDS = re.compile(r"^aof_replay_commands: ([0-9]+)$", re.M)
REDIS_REPLAY = re.compile(r"DB loaded from append only file: ([0-9.]+) seconds")
TARGETS = ("project-skiplist", "project-rbtree", "redis")
WORKLOADS = ("string", "hash", "zset")
VALUE = b"x" * 64


def comma_values(text, cast=str):
    try:
        values = tuple(dict.fromkeys(cast(item.strip()) for item in text.split(",")))
    except ValueError as error:
        raise argparse.ArgumentTypeError("invalid comma-separated value") from error
    if not values:
        raise argparse.ArgumentTypeError("at least one value is required")
    return values


def resp(*parts: bytes) -> bytes:
    return b"*%d\r\n" % len(parts) + b"".join(
        b"$%d\r\n" % len(part) + part + b"\r\n" for part in parts
    )


def command(workload: str, index: int) -> bytes:
    if workload == "string":
        return resp(b"SET", f"replay:string:{index}".encode(), VALUE)
    if workload == "hash":
        return resp(b"HSET", b"replay:hash", f"field:{index}".encode(), VALUE)
    return resp(b"ZADD", b"replay:zset", str(index).encode(),
                f"member:{index}".encode())


def generate(path: Path, workload: str, count: int) -> int:
    pending = bytearray()
    with path.open("wb") as output:
        for index in range(count):
            pending.extend(command(workload, index))
            if len(pending) >= 1024 * 1024:
                output.write(pending)
                pending.clear()
        if pending:
            output.write(pending)
    return path.stat().st_size


def read_line(stream) -> bytes:
    line = stream.readline()
    if not line.endswith(b"\r\n"):
        raise RuntimeError("incomplete RESP line")
    return line[:-2]


def request(port: int, *parts: bytes, timeout: float = 10.0):
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as client:
        client.settimeout(timeout)
        client.sendall(resp(*parts))
        stream = client.makefile("rb")
        line = read_line(stream)
        if line[:1] == b":":
            return int(line[1:])
        if line[:1] == b"$":
            length = int(line[1:])
            if length < 0:
                return None
            payload = stream.read(length)
            if len(payload) != length or stream.read(2) != b"\r\n":
                raise RuntimeError("incomplete bulk response")
            return payload
        if line[:1] == b"-":
            raise RuntimeError(line[1:].decode(errors="replace"))
        return line[1:] if line[:1] == b"+" else line


def verify(port: int, workload: str, count: int) -> None:
    expected_keys = count if workload == "string" else 1
    if request(port, b"DBSIZE") != expected_keys:
        raise RuntimeError("DBSIZE mismatch")
    if workload == "string":
        for index in (0, count - 1):
            if request(port, b"GET", f"replay:string:{index}".encode()) != VALUE:
                raise RuntimeError("GET validation failed")
    elif workload == "hash":
        if request(port, b"HLEN", b"replay:hash") != count:
            raise RuntimeError("HLEN mismatch")
        for index in (0, count - 1):
            if request(port, b"HGET", b"replay:hash",
                       f"field:{index}".encode()) != VALUE:
                raise RuntimeError("HGET validation failed")
    else:
        if request(port, b"ZCARD", b"replay:zset") != count:
            raise RuntimeError("ZCARD mismatch")
        for index in (0, count - 1):
            value = request(port, b"ZSCORE", b"replay:zset",
                            f"member:{index}".encode())
            if value is None or float(value) != float(index):
                raise RuntimeError("ZSCORE validation failed")


def port_open(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.05):
            return True
    except OSError:
        return False


def wait_ready(process, port: int, start: float, timeout: float) -> float:
    deadline = start + timeout
    last_error = None
    while time.perf_counter() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(f"server exited {process.returncode}\n{stdout}\n{stderr}")
        try:
            remaining = max(0.01, deadline - time.perf_counter())
            if request(port, b"PING", timeout=min(0.2, remaining)) == b"PONG":
                return time.perf_counter() - start
            last_error = RuntimeError("PING did not return PONG")
        except (OSError, RuntimeError) as error:
            # Redis starts accepting TCP connections while an AOF is still
            # being replayed and replies with LOADING until commands are safe.
            last_error = error
        time.sleep(0.005)
    process.kill()
    stdout, stderr = process.communicate()
    raise TimeoutError(
        f"server not command-ready; last readiness error: {last_error}\n"
        f"{stdout}\n{stderr}"
    )


def process_metrics(pid: int):
    rss = 0
    cpu = 0.0
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmHWM:"):
                rss = int(line.split()[1])
        fields = Path(f"/proc/{pid}/stat").read_text().split()
        cpu = (int(fields[13]) + int(fields[14])) / 100.0
    except OSError:
        pass
    return cpu, rss


def server_command(target: str, args, aof: Path, port: int):
    if target == "redis":
        return [args.redis_server, "--port", str(port), "--save", "",
                "--appendonly", "yes", "--appendfsync", "no",
                "--auto-aof-rewrite-percentage", "0", "--dir", str(aof.parent),
                "--appendfilename", aof.name]
    engine = "skiplist" if target.endswith("skiplist") else "rbtree"
    return [args.server, "--appendonly", "yes", "--appendfilename", str(aof),
            "--appendfsync", "no", "--zset-engine", engine,
            "--maxmemory", "0", "--maxkeys", "0"]


def run_once(target: str, workload: str, count: int, source: Path,
             args, run_dir: Path):
    port = 6380 if target == "redis" else 9096
    aof = run_dir / "appendonly.aof"
    shutil.copyfile(source, aof)
    started = time.perf_counter()
    process = subprocess.Popen(server_command(target, args, aof, port),
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True)
    try:
        ready = wait_ready(process, port, started, args.timeout)
        verify(port, workload, count)
        cpu, rss = process_metrics(process.pid)
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=15.0)
    except BaseException:
        if process.poll() is None:
            process.kill()
            process.communicate()
        raise
    if process.returncode != 0:
        raise RuntimeError(f"server stopped {process.returncode}\n{stdout}\n{stderr}")
    if target == "redis":
        match = REDIS_REPLAY.search(stdout + "\n" + stderr)
        commands_loaded = count
    else:
        match = PROJECT_REPLAY.search(stderr)
        loaded = PROJECT_COMMANDS.search(stderr)
        commands_loaded = int(loaded.group(1)) if loaded else -1
    if match is None or commands_loaded != count:
        raise RuntimeError(f"missing/invalid replay metrics\n{stdout}\n{stderr}")
    return ready, float(match.group(1)), cpu, rss


def percentile(values, q: float):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]


def cv(values):
    mean = statistics.mean(values)
    return 0.0 if mean == 0 or len(values) < 2 else statistics.stdev(values) / mean


def verify_redis(parser, executable):
    try:
        result = subprocess.run([executable, "--version"], check=True,
                                capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.SubprocessError) as error:
        parser.error(f"cannot run Redis: {error}")
    version = (result.stdout or result.stderr).strip()
    if "v=6.2.23" not in version:
        parser.error(f"Redis 6.2.23 required; got {version}")
    return version


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="./kvstore")
    parser.add_argument("--redis-server")
    parser.add_argument("--targets", default=",".join(TARGETS))
    parser.add_argument("--workloads", default=",".join(WORKLOADS))
    parser.add_argument("--sizes", default="10000,100000,1000000")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=600.0)
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--summary-csv", type=Path)
    parser.add_argument("--aof-dir", type=Path)
    args = parser.parse_args()
    targets = comma_values(args.targets)
    workloads = comma_values(args.workloads)
    sizes = comma_values(args.sizes, int)
    if any(value not in TARGETS for value in targets): parser.error("unknown target")
    if any(value not in WORKLOADS for value in workloads): parser.error("unknown workload")
    if any(value <= 0 for value in sizes) or args.repeats < 1 or args.timeout <= 0:
        parser.error("sizes must be positive, repeats >= 1, timeout > 0")
    if "redis" in targets:
        if not args.redis_server: parser.error("--redis-server is required")
        print(f"redis: {verify_redis(parser, args.redis_server)}")
    for port in (9096, 6380):
        if port_open(port): parser.error(f"port {port} is already in use")

    summary_path = args.summary_csv or args.csv.with_suffix(args.csv.suffix + ".summary.csv")
    temporary = None
    if args.aof_dir is None:
        temporary = tempfile.TemporaryDirectory(prefix="aof-replay-")
        root = Path(temporary.name)
    else:
        root = args.aof_dir
        root.mkdir(parents=True, exist_ok=True)
    rows = []
    try:
        for workload in workloads:
            for count in sizes:
                source = root / f"{workload}-{count}.aof"
                aof_bytes = generate(source, workload, count)
                selected_targets = targets if workload == "zset" else tuple(
                    target for target in targets if target != "project-rbtree"
                )
                for target in selected_targets:
                    for run in range(1, args.repeats + 1):
                        run_dir = root / f"run-{target}-{workload}-{count}-{run}"
                        run_dir.mkdir(exist_ok=True)
                        ready, replay, cpu, rss = run_once(
                            target, workload, count, source, args, run_dir)
                        row = {
                            "target": target, "workload": workload,
                            "commands": count, "run": run,
                            "page_cache_group": "first" if run == 1 else "subsequent",
                            "aof_bytes": aof_bytes,
                            "startup_ready_seconds": f"{ready:.9f}",
                            "replay_seconds": f"{replay:.9f}",
                            "commands_per_second": f"{count / replay:.2f}",
                            "mib_per_second": f"{aof_bytes / replay / 1048576:.2f}",
                            "cpu_seconds": f"{cpu:.6f}",
                            "vmhwm_kib": rss,
                            "cardinality_valid": 1,
                        }
                        rows.append(row)
                        print(f"target={target} workload={workload} commands={count} "
                              f"run={run} replay={replay:.6f}s ready={ready:.6f}s")
        args.csv.parent.mkdir(parents=True, exist_ok=True)
        with args.csv.open("w", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=list(rows[0]))
            writer.writeheader(); writer.writerows(rows)
        groups = {}
        for row in rows:
            key = (row["target"], row["workload"], row["commands"],
                   row["page_cache_group"])
            groups.setdefault(key, []).append(float(row["replay_seconds"]))
        summary_rows = []
        for key, values in sorted(groups.items()):
            target, workload, count, cache_group = key
            summary_rows.append({
                "target": target, "workload": workload, "commands": count,
                "page_cache_group": cache_group, "runs": len(values),
                "replay_min_seconds": min(values),
                "replay_median_seconds": statistics.median(values),
                "replay_p95_seconds": percentile(values, .95),
                "replay_max_seconds": max(values), "replay_cv": cv(values),
            })
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        with summary_path.open("w", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=list(summary_rows[0]))
            writer.writeheader(); writer.writerows(summary_rows)
        print(f"raw csv: {args.csv}\nsummary csv: {summary_path}")
    finally:
        if temporary is not None: temporary.cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

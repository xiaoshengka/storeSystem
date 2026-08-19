#!/usr/bin/env python3
import os
import shlex
import signal
import socket
import subprocess
import sys
import tempfile
import time

from reactor_integration import RespReader, connect, encode_command, wait_for_server


def exchange(reader: RespReader, client: socket.socket, *arguments: bytes):
    client.sendall(encode_command(*arguments))
    return reader.read()


def stop_server(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2.0)
    stdout, stderr = process.communicate()
    if (os.environ.get("KVSTORE_SHOW_SERVER_LOGS") == "1" or
            process.returncode not in (0, -signal.SIGTERM)):
        print(stdout, file=sys.stderr)
        print(stderr, file=sys.stderr)


def start_server(
    aof_path: str,
    policy: str = "everysec",
    max_keys: int = 4,
) -> subprocess.Popen:
    command = shlex.split(os.environ.get("KVSTORE_AOF_SERVER_PREFIX", "")) + [
        "./kvstore",
        "--appendonly", "yes",
        "--appendfilename", aof_path,
        "--appendfsync", policy,
        "--maxmemory", "4MiB",
        "--maxkeys", str(max_keys),
    ]
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_for_server(process)
    except BaseException:
        stop_server(process)
        raise
    return process


def kill_server(process: subprocess.Popen) -> None:
    process.kill()
    process.wait(timeout=5.0)
    stdout, stderr = process.communicate()
    if os.environ.get("KVSTORE_SHOW_SERVER_LOGS") == "1":
        print(stdout, file=sys.stderr)
        print(stderr, file=sys.stderr)


def populate() -> None:
    binary_key = b"binary\x00key"
    binary_value = b"value\x00bytes"
    with connect() as client:
        reader = RespReader(client)
        assert exchange(reader, client, b"SET", binary_key, binary_value) == (
            "simple", b"OK"
        )
        assert exchange(reader, client, b"SET", b"keep", b"old") == (
            "simple", b"OK"
        )
        assert exchange(reader, client, b"SET", b"keep", b"new", b"PX", b"5000") == (
            "simple", b"OK"
        )
        assert exchange(reader, client, b"PERSIST", b"keep") == (
            "integer", 1
        )
        assert exchange(reader, client, b"SET", b"gone", b"value") == (
            "simple", b"OK"
        )
        assert exchange(reader, client, b"DEL", b"gone") == ("integer", 1)
        assert exchange(reader, client, b"SET", b"expires", b"soon", b"PX", b"150") == (
            "simple", b"OK"
        )
        assert exchange(reader, client, b"SET", b"victim", b"lru") == (
            "simple", b"OK"
        )
        assert exchange(reader, client, b"GET", binary_key) == (
            "bulk", binary_value
        )
        assert exchange(reader, client, b"GET", b"keep") == ("bulk", b"new")
        assert exchange(reader, client, b"GET", b"expires") == (
            "bulk", b"soon"
        )
        assert exchange(reader, client, b"SET", b"evictor", b"kept") == (
            "simple", b"OK"
        )
        assert exchange(reader, client, b"GET", b"victim") == ("bulk", None)


def verify_recovery() -> None:
    with connect() as client:
        reader = RespReader(client)
        assert exchange(reader, client, b"GET", b"binary\x00key") == (
            "bulk", b"value\x00bytes"
        )
        assert exchange(reader, client, b"GET", b"keep") == ("bulk", b"new")
        assert exchange(reader, client, b"PTTL", b"keep") == ("integer", -1)
        assert exchange(reader, client, b"GET", b"gone") == ("bulk", None)
        assert exchange(reader, client, b"GET", b"expires") == ("bulk", None)
        assert exchange(reader, client, b"GET", b"victim") == ("bulk", None)
        assert exchange(reader, client, b"GET", b"evictor") == (
            "bulk", b"kept"
        )


def verify_crash_batch_recovery(aof_path: str) -> None:
    first = start_server(aof_path, "no", 64)
    try:
        with connect() as client:
            reader = RespReader(client)
            pipeline = b"".join(
                encode_command(b"SET", f"crash-{index}".encode(), b"value")
                for index in range(32)
            )
            client.sendall(pipeline)
            for _ in range(32):
                assert reader.read() == ("simple", b"OK")
    finally:
        kill_server(first)

    second = start_server(aof_path, "no", 64)
    try:
        with connect() as client:
            reader = RespReader(client)
            for index in range(32):
                assert exchange(
                    reader,
                    client,
                    b"GET",
                    f"crash-{index}".encode(),
                ) == ("bulk", b"value")
    finally:
        stop_server(second)


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="storeSystem-aof-") as directory:
        path = os.path.join(directory, "appendonly.aof")
        first = start_server(path)
        try:
            populate()
        finally:
            stop_server(first)
        time.sleep(0.2)
        second = start_server(path)
        try:
            verify_recovery()
        finally:
            stop_server(second)
        verify_crash_batch_recovery(os.path.join(directory, "crash.aof"))
    print("aof_integration: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

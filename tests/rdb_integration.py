#!/usr/bin/env python3
import os
import shlex
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

from reactor_integration import RespReader, connect, encode_command, wait_for_server


def exchange(reader: RespReader, client: socket.socket, *arguments: bytes):
    client.sendall(encode_command(*arguments))
    return reader.read()


def stop_server(process: subprocess.Popen, kill: bool = False) -> None:
    if process.poll() is None:
        process.send_signal(signal.SIGKILL if kill else signal.SIGTERM)
        try:
            process.wait(timeout=10.0)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3.0)
    stdout, stderr = process.communicate()
    if (os.environ.get("KVSTORE_SHOW_SERVER_LOGS") == "1" or
            (not kill and process.returncode not in (0, -signal.SIGTERM))):
        print(stdout, file=sys.stderr)
        print(stderr, file=sys.stderr)


def start_server(rdb_path: str, aof_path=None, engine: str = "skiplist",
                 automatic=False) -> subprocess.Popen:
    command = shlex.split(os.environ.get("KVSTORE_RDB_SERVER_PREFIX", "")) + [
        "./kvstore", "--rdb", "yes", "--dbfilename", rdb_path,
        "--zset-engine", engine, "--maxmemory", "64MiB",
        "--maxkeys", "50000",
    ]
    if aof_path is not None:
        command += ["--appendonly", "yes", "--appendfilename", aof_path,
                    "--appendfsync", "everysec"]
    if automatic:
        command += ["--save-seconds", "1", "--save-changes", "1"]
    process = subprocess.Popen(command, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True)
    try:
        wait_for_server(process)
    except BaseException:
        stop_server(process)
        raise
    return process


def expect_start_failure(rdb_path: str, aof_path: str) -> None:
    command = ["./kvstore", "--rdb", "yes", "--dbfilename", rdb_path,
               "--appendonly", "yes", "--appendfilename", aof_path,
               "--appendfsync", "everysec"]
    process = subprocess.Popen(command, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True)
    try:
        process.wait(timeout=5.0)
    except subprocess.TimeoutExpired:
        stop_server(process)
        raise AssertionError("mismatched RDB/AOF unexpectedly started")
    stdout, stderr = process.communicate()
    assert process.returncode != 0, (stdout, stderr)


def info(reader: RespReader, client: socket.socket):
    response = exchange(reader, client, b"INFO", b"PERSISTENCE")
    assert response[0] == "bulk" and isinstance(response[1], bytes)
    result = {}
    for line in response[1].split(b"\r\n"):
        if b":" in line:
            key, value = line.split(b":", 1)
            result[key.decode()] = value.decode()
    return result


def wait_bgsave(reader: RespReader, client: socket.socket) -> None:
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        state = info(reader, client)
        if state.get("rdb_bgsave_in_progress") == "0":
            assert state.get("rdb_last_save_status") == "0", state
            return
        time.sleep(0.05)
    raise TimeoutError("BGSAVE did not finish")


def populate_collections(reader: RespReader, client: socket.socket) -> None:
    assert exchange(reader, client, b"SET", b"binary\x00key",
                    b"value\x00bytes") == ("simple", b"OK")
    assert exchange(reader, client, b"HSET", b"hash", b"a\x00f", b"one",
                    b"other", b"two") == ("integer", 2)
    assert exchange(reader, client, b"ZADD", b"zset", b"-inf", b"first",
                    b"2.5", b"middle", b"+inf", b"last") == ("integer", 3)
    assert exchange(reader, client, b"SET", b"expires", b"gone",
                    b"PX", b"100") == ("simple", b"OK")


def verify_collections(reader: RespReader, client: socket.socket) -> None:
    assert exchange(reader, client, b"GET", b"binary\x00key") == (
        "bulk", b"value\x00bytes")
    assert exchange(reader, client, b"HGET", b"hash", b"a\x00f") == (
        "bulk", b"one")
    assert exchange(reader, client, b"ZRANGE", b"zset", b"0", b"-1") == (
        "array", [("bulk", b"first"), ("bulk", b"middle"),
                  ("bulk", b"last")])
    assert exchange(reader, client, b"GET", b"expires") == ("bulk", None)


def functional_and_cross_engine(directory: str) -> None:
    rdb_path = os.path.join(directory, "standalone.kvrdb")
    first = start_server(rdb_path, engine="skiplist")
    try:
        with connect() as client:
            reader = RespReader(client)
            assert exchange(reader, client, b"LASTSAVE") == ("integer", 0)
            populate_collections(reader, client)
            assert exchange(reader, client, b"SAVE") == ("simple", b"OK")
            lastsave = exchange(reader, client, b"LASTSAVE")
            assert lastsave[0] == "integer" and lastsave[1] > 0
    finally:
        stop_server(first)
    time.sleep(0.15)
    second = start_server(rdb_path, engine="rbtree")
    try:
        with connect() as client:
            verify_collections(RespReader(client), client)
    finally:
        stop_server(second)


def hybrid_tail_and_pipeline(directory: str) -> None:
    rdb_path = os.path.join(directory, "hybrid.kvrdb")
    aof_path = os.path.join(directory, "hybrid.aof")
    first = start_server(rdb_path, aof_path)
    errors = []
    try:
        with connect() as client:
            reader = RespReader(client)
            populate_collections(reader, client)
            payload = b"x" * 1024
            pipeline = b"".join(encode_command(
                b"SET", f"seed-{index}".encode(), payload
            ) for index in range(2000))
            client.sendall(pipeline)
            for _ in range(2000):
                assert reader.read() == ("simple", b"OK")

            def load_during_save():
                try:
                    with connect() as worker:
                        worker_reader = RespReader(worker)
                        batch = b"".join(
                            encode_command(b"SET", f"tail-{index}".encode(), b"v") +
                            encode_command(b"GET", f"tail-{index}".encode())
                            for index in range(300)
                        )
                        worker.sendall(batch)
                        for _ in range(300):
                            assert worker_reader.read() == ("simple", b"OK")
                            assert worker_reader.read() == ("bulk", b"v")
                except BaseException as error:  # surfaced in the main thread
                    errors.append(error)

            worker = threading.Thread(target=load_during_save)
            client.sendall(encode_command(b"BGSAVE") +
                           encode_command(b"BGSAVE"))
            assert reader.read() == ("simple", b"Background saving started")
            assert reader.read()[0] == "error"
            worker.start()
            worker.join(timeout=15.0)
            assert not worker.is_alive() and not errors, errors
            wait_bgsave(reader, client)
            assert exchange(reader, client, b"SET", b"after-snapshot",
                            b"tail") == ("simple", b"OK")
    finally:
        stop_server(first, kill=True)

    second = start_server(rdb_path, aof_path, engine="rbtree")
    try:
        with connect() as client:
            reader = RespReader(client)
            verify_collections(reader, client)
            assert exchange(reader, client, b"GET", b"after-snapshot") == (
                "bulk", b"tail")
            assert exchange(reader, client, b"GET", b"tail-299") == (
                "bulk", b"v")
    finally:
        stop_server(second)

    wrong_aof = os.path.join(directory, "wrong.aof")
    open(wrong_aof, "wb").close()
    expect_start_failure(rdb_path, wrong_aof)

    rdb_only = start_server(rdb_path, engine="skiplist")
    try:
        with connect() as client:
            reader = RespReader(client)
            assert exchange(reader, client, b"GET", b"binary\x00key") == (
                "bulk", b"value\x00bytes")
    finally:
        stop_server(rdb_only)


def automatic_and_child_failure(directory: str) -> None:
    automatic_path = os.path.join(directory, "automatic.kvrdb")
    server = start_server(automatic_path, automatic=True)
    try:
        with connect() as client:
            reader = RespReader(client)
            assert exchange(reader, client, b"SET", b"auto", b"value") == (
                "simple", b"OK")
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                if exchange(reader, client, b"LASTSAVE")[1] > 0:
                    break
                time.sleep(0.1)
            else:
                raise TimeoutError("automatic BGSAVE did not run")
    finally:
        stop_server(server)

    bad_path = os.path.join(directory, "missing", "dump.kvrdb")
    server = start_server(bad_path)
    try:
        with connect() as client:
            reader = RespReader(client)
            assert exchange(reader, client, b"BGSAVE") == (
                "simple", b"Background saving started")
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                state = info(reader, client)
                if state.get("rdb_bgsave_in_progress") == "0":
                    assert state.get("rdb_last_save_status") == "-1", state
                    break
                time.sleep(0.05)
            else:
                raise TimeoutError("failed child was not reaped")
    finally:
        stop_server(server)


def strict_uncheckpointed_pair(directory: str) -> None:
    rdb_path = os.path.join(directory, "no-checkpoint.kvrdb")
    server = start_server(rdb_path)
    try:
        with connect() as client:
            reader = RespReader(client)
            assert exchange(reader, client, b"SET", b"key", b"value") == (
                "simple", b"OK")
            assert exchange(reader, client, b"SAVE") == ("simple", b"OK")
    finally:
        stop_server(server)
    empty_aof = os.path.join(directory, "empty.aof")
    open(empty_aof, "wb").close()
    expect_start_failure(rdb_path, empty_aof)


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="storeSystem-rdb-") as directory:
        functional_and_cross_engine(directory)
        hybrid_tail_and_pipeline(directory)
        automatic_and_child_failure(directory)
        strict_uncheckpointed_pair(directory)
    print("rdb_integration: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

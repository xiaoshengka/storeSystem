#!/usr/bin/env python3
import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Optional

from reactor_integration import RespReader, encode_command, wait_for_server


HOST = os.environ.get("KVSTORE_MYSQL_TEST_HOST", "127.0.0.1")
PORT = int(os.environ.get("KVSTORE_MYSQL_TEST_PORT", "3306"))
USER = os.environ.get("KVSTORE_MYSQL_TEST_USER", "kvstore")
DATABASE = os.environ.get("KVSTORE_MYSQL_TEST_DATABASE", "kvstore_test")
PASSWORD = os.environ.get("KVSTORE_MYSQL_TEST_PASSWORD")


def mysql_command(sql: str) -> str:
    if shutil.which("mysql") is None:
        raise RuntimeError("mysql client is required for mysql_integration.py")
    environment = os.environ.copy()
    environment["MYSQL_PWD"] = PASSWORD or ""
    result = subprocess.run(
        ["mysql", "--batch", "--skip-column-names", "-h", HOST,
         "-P", str(PORT), "-u", USER, DATABASE, "-e", sql],
        env=environment, text=True, capture_output=True, check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"mysql failed: {result.stderr.strip()}")
    return result.stdout.strip()


def reset_database() -> None:
    if not DATABASE.endswith("_test"):
        raise RuntimeError("refusing to reset a database whose name does not end in _test")
    mysql_command(
        "DELETE FROM kv_change_log; DELETE FROM kv_writer_state; "
        "DELETE FROM kv_keys; UPDATE kv_schema_meta SET "
        "bootstrap_state='EMPTY',active_writer_uuid=NULL WHERE singleton_id=1"
    )


def exchange(*arguments: bytes):
    with socket.create_connection(("127.0.0.1", 9096), timeout=3.0) as client:
        client.settimeout(5.0)
        client.sendall(encode_command(*arguments))
        return RespReader(client).read()


def parse_info(payload: bytes) -> Dict[str, str]:
    fields: Dict[str, str] = {}
    for line in payload.decode("ascii").splitlines():
        name, value = line.split(":", 1)
        fields[name] = value
    return fields


def wait_for_writer(timeout: float = 10.0) -> Dict[str, str]:
    deadline = time.monotonic() + timeout
    last: Dict[str, str] = {}
    while time.monotonic() < deadline:
        response = exchange(b"INFO", b"MYSQL")
        assert response[0] == "bulk" and isinstance(response[1], bytes)
        last = parse_info(response[1])
        if last["mysql_submitted_sequence"] == last["mysql_applied_sequence"]:
            return last
        time.sleep(0.02)
    raise TimeoutError(f"MySQL writer did not catch up: {last}")


def start_server(aof_path: Path, extra: Optional[List[str]] = None):
    command = [
        "./kvstore", "--appendonly", "yes", "--appendfilename", str(aof_path),
        "--appendfsync", "always", "--mysql", "yes", "--mysql-host", HOST,
        "--mysql-port", str(PORT), "--mysql-user", USER,
        "--mysql-database", DATABASE, "--maxkeys", "2",
    ]
    if extra:
        command.extend(extra)
    environment = os.environ.copy()
    environment["KVSTORE_MYSQL_PASSWORD"] = PASSWORD or ""
    process = subprocess.Popen(command, env=environment, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True)
    try:
        wait_for_server(process, timeout=10.0)
    except BaseException:
        stdout, stderr = process.communicate(timeout=2.0)
        raise RuntimeError(f"server failed to start\nstdout={stdout}\nstderr={stderr}")
    return process


def stop_server(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=8.0)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2.0)
    stdout, stderr = process.communicate()
    if process.returncode not in (0, -signal.SIGTERM):
        raise RuntimeError(
            f"server exited with {process.returncode}\nstdout={stdout}\nstderr={stderr}"
        )


def first_bootstrap_and_types(aof_path: Path) -> None:
    process = start_server(aof_path)
    try:
        binary_key = b"string\x00key"
        binary_value = b"value\x00payload"
        assert exchange(b"GET", b"missing") == ("bulk", None)
        assert exchange(b"GET", b"missing") == ("bulk", None)
        assert exchange(b"SET", binary_key, binary_value) == ("simple", b"OK")
        assert exchange(b"HSET", b"hash", b"f\x00", b"v\x00") == ("integer", 1)
        assert exchange(b"ZADD", b"zset", b"1.5", b"m\x00") == ("integer", 1)
        wait_for_writer()

        # maxkeys=2 evicts an earlier hot key; the following reads must refill
        # complete typed objects from MySQL rather than return an old value.
        assert exchange(b"GET", binary_key) == ("bulk", binary_value)
        hash_value = exchange(b"HGET", b"hash", b"f\x00")
        assert hash_value == ("bulk", b"v\x00"), hash_value
        score = exchange(b"ZSCORE", b"zset", b"m\x00")
        assert score == ("bulk", b"1.5"), score

        assert exchange(b"PEXPIRE", b"hash", b"5000") == ("integer", 1)
        ttl = exchange(b"PTTL", b"hash")
        assert ttl[0] == "integer" and 0 < ttl[1] <= 5000
        info = wait_for_writer()
        assert int(info["mysql_loads"]) >= 3
        assert int(info["mysql_negative_cache_hits"]) >= 1
    finally:
        stop_server(process)

    assert mysql_command("SELECT bootstrap_state FROM kv_schema_meta WHERE singleton_id=1") == "READY"
    assert int(mysql_command("SELECT COUNT(*) FROM kv_keys")) >= 3
    assert int(mysql_command("SELECT COUNT(*) FROM kv_change_log")) >= 4


def database_ahead_and_cold_writes(aof_path: Path) -> None:
    # A new empty AOF with a READY database exercises the MySQL-ahead path.
    process = start_server(aof_path)
    try:
        assert exchange(b"HSET", b"hash", b"f\x00", b"updated") == ("integer", 0)
        assert exchange(b"ZREM", b"zset", b"m\x00") == ("integer", 1)
        assert exchange(b"GET", b"string\x00key") == ("bulk", b"value\x00payload")
        info = wait_for_writer()
        assert int(info["mysql_applied_sequence"]) > 0
    finally:
        stop_server(process)


def main() -> int:
    if PASSWORD is None:
        raise RuntimeError("set KVSTORE_MYSQL_TEST_PASSWORD before running this test")
    reset_database()
    with tempfile.TemporaryDirectory(prefix="kvstore-mysql-") as directory:
        root = Path(directory)
        first_bootstrap_and_types(root / "first.aof")
        database_ahead_and_cold_writes(root / "database-ahead.aof")
    print("mysql_integration: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

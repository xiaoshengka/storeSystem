#!/usr/bin/env python3
import concurrent.futures
import os
import shlex
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional

from reactor_integration import RespReader, encode_command, wait_for_server


HOST = os.environ.get("KVSTORE_MYSQL_TEST_HOST", "127.0.0.1")
PORT = int(os.environ.get("KVSTORE_MYSQL_TEST_PORT", "3306"))
USER = os.environ.get("KVSTORE_MYSQL_TEST_USER", "kvstore")
DATABASE = os.environ.get("KVSTORE_MYSQL_TEST_DATABASE", "kvstore_test")
PASSWORD = os.environ.get("KVSTORE_MYSQL_TEST_PASSWORD")
SERVER_PREFIX = shlex.split(os.environ.get("KVSTORE_MYSQL_SERVER_PREFIX", ""))


class TcpCutProxy:
    def __init__(self, upstream_host: str, upstream_port: int):
        self.upstream = (upstream_host, upstream_port)
        self.port = 0
        self.listener: Optional[socket.socket] = None
        self.accept_thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.connections = set()

    def start(self) -> None:
        if self.listener is not None:
            return
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", self.port))
        if self.port == 0:
            self.port = listener.getsockname()[1]
        listener.listen(32)
        listener.settimeout(0.2)
        self.listener = listener
        self.stop_event = threading.Event()
        self.accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self.accept_thread.start()

    def _accept_loop(self) -> None:
        listener = self.listener
        assert listener is not None
        while not self.stop_event.is_set():
            try:
                client, _ = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._handle, args=(client,), daemon=True).start()

    def _handle(self, client: socket.socket) -> None:
        try:
            upstream = socket.create_connection(self.upstream, timeout=3.0)
            upstream.settimeout(None)
            client.settimeout(None)
        except OSError:
            client.close()
            return
        pair = (client, upstream)
        with self.lock:
            self.connections.add(pair)
        left = threading.Thread(target=self._pump, args=(client, upstream, pair),
                                daemon=True)
        right = threading.Thread(target=self._pump, args=(upstream, client, pair),
                                 daemon=True)
        left.start()
        right.start()
        left.join()
        right.join()

    def _pump(self, source: socket.socket, target: socket.socket, pair) -> None:
        try:
            while not self.stop_event.is_set():
                chunk = source.recv(65536)
                if not chunk:
                    break
                target.sendall(chunk)
        except OSError:
            pass
        finally:
            with self.lock:
                self.connections.discard(pair)
            for connection in pair:
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                connection.close()

    def cut(self) -> None:
        self.stop_event.set()
        listener = self.listener
        self.listener = None
        if listener is not None:
            listener.close()
        with self.lock:
            pairs = list(self.connections)
            self.connections.clear()
        for pair in pairs:
            for connection in pair:
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                connection.close()
        if self.accept_thread is not None:
            self.accept_thread.join(timeout=2.0)
            self.accept_thread = None

    def restore(self) -> None:
        self.start()

    def close(self) -> None:
        self.cut()


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


def pipeline_exchange(commands: List[List[bytes]], timeout: float = 10.0):
    with socket.create_connection(("127.0.0.1", 9096), timeout=3.0) as client:
        client.settimeout(timeout)
        client.sendall(b"".join(encode_command(*command) for command in commands))
        reader = RespReader(client)
        return [reader.read() for _ in commands]


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
    process = subprocess.Popen(SERVER_PREFIX + command, env=environment, stdout=subprocess.PIPE,
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
        assert exchange(b"SET", b"large-string", b"x" * 4096) == ("simple", b"OK")

        commands: List[List[bytes]] = []
        for start in range(0, 5000, 50):
            command = [b"HSET", b"coalesced-hash"]
            for index in range(start, start + 50):
                command.extend((f"f{index}".encode("ascii"),
                                (f"value-{index}:".encode("ascii") + b"x" * 48)))
            commands.append(command)
        replies = pipeline_exchange(commands, timeout=30.0)
        assert all(reply == ("integer", 50) for reply in replies)
        wait_for_writer(timeout=60.0)

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
        wrongtype = exchange(b"GET", b"hash")
        assert wrongtype[0] == "error" and b"WRONGTYPE" in wrongtype[1]
        assert exchange(b"HSET", b"hash", b"f\x00", b"updated") == ("integer", 0)
        assert exchange(b"ZREM", b"zset", b"m\x00") == ("integer", 1)
        assert exchange(b"GET", b"string\x00key") == ("bulk", b"value\x00payload")
        info = wait_for_writer()
        assert int(info["mysql_applied_sequence"]) > 0
    finally:
        stop_server(process)


def coalesced_cold_load(aof_path: Path) -> None:
    process = start_server(aof_path, ["--mysql-read-workers", "1"])
    try:
        before = parse_info(exchange(b"INFO", b"MYSQL")[1])
        barrier = threading.Barrier(32)

        def load_same_key(_: int):
            with socket.create_connection(("127.0.0.1", 9096), timeout=3.0) as client:
                client.settimeout(10.0)
                barrier.wait(timeout=5.0)
                client.sendall(encode_command(b"HGET", b"coalesced-hash", b"f0"))
                return RespReader(client).read()

        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
            replies = list(executor.map(load_same_key, range(32)))
        expected = b"value-0:" + b"x" * 48
        assert all(reply == ("bulk", expected) for reply in replies)
        after = parse_info(exchange(b"INFO", b"MYSQL")[1])
        assert int(after["mysql_loads"]) - int(before["mysql_loads"]) == 1
        assert (int(after["mysql_coalesced_loads"]) -
                int(before["mysql_coalesced_loads"])) >= 31
    finally:
        stop_server(process)


def load_limit_error(aof_path: Path) -> None:
    process = start_server(aof_path, ["--mysql-load-max", "1KiB"])
    try:
        response = exchange(b"GET", b"large-string")
        assert response == ("error", b"ERR MySQL object exceeds load limit")
        info = parse_info(exchange(b"INFO", b"MYSQL")[1])
        assert int(info["mysql_load_errors"]) >= 1
    finally:
        stop_server(process)


def startup_unavailable_refuses_listener(aof_path: Path) -> None:
    command = [
        "./kvstore", "--appendonly", "yes", "--appendfilename", str(aof_path),
        "--appendfsync", "always", "--mysql", "yes",
        "--mysql-host", "127.0.0.1", "--mysql-port", "1",
        "--mysql-user", USER, "--mysql-database", DATABASE,
    ]
    environment = os.environ.copy()
    environment["KVSTORE_MYSQL_PASSWORD"] = PASSWORD or ""
    process = subprocess.Popen(
        SERVER_PREFIX + command, env=environment, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True,
    )
    stdout, stderr = process.communicate(timeout=20.0)
    assert process.returncode != 0, (stdout, stderr)
    try:
        socket.create_connection(("127.0.0.1", 9096), timeout=0.2)
    except OSError:
        pass
    else:
        raise AssertionError("server listened even though MySQL startup failed")


def runtime_disconnect_and_reconnect(aof_path: Path) -> None:
    proxy = TcpCutProxy(HOST, PORT)
    proxy.start()
    process = start_server(
        aof_path,
        ["--mysql-host", "127.0.0.1", "--mysql-port", str(proxy.port)],
    )
    try:
        hot_key = b"string\x00key"
        assert exchange(b"GET", hot_key) == ("bulk", b"value\x00payload")
        before = parse_info(exchange(b"INFO", b"MYSQL")[1])
        proxy.cut()

        failed = exchange(b"GET", b"large-string")
        assert failed == ("error", b"ERR MySQL backend unavailable"), failed
        assert exchange(b"GET", hot_key) == ("bulk", b"value\x00payload")

        proxy.restore()
        deadline = time.monotonic() + 15.0
        response = None
        while time.monotonic() < deadline:
            response = exchange(b"GET", b"large-string")
            if response == ("bulk", b"x" * 4096):
                break
            assert response == ("error", b"ERR MySQL backend unavailable")
            time.sleep(0.05)
        assert response == ("bulk", b"x" * 4096), response
        after = parse_info(exchange(b"INFO", b"MYSQL")[1])
        assert int(after["mysql_reconnects"]) > int(before["mysql_reconnects"])
        assert int(after["mysql_load_errors"]) > int(before["mysql_load_errors"])
    finally:
        stop_server(process)
        proxy.close()


def main() -> int:
    if PASSWORD is None:
        raise RuntimeError("set KVSTORE_MYSQL_TEST_PASSWORD before running this test")
    reset_database()
    with tempfile.TemporaryDirectory(prefix="kvstore-mysql-") as directory:
        root = Path(directory)
        first_bootstrap_and_types(root / "first.aof")
        database_ahead_and_cold_writes(root / "database-ahead.aof")
        coalesced_cold_load(root / "coalesced.aof")
        load_limit_error(root / "load-limit.aof")
        runtime_disconnect_and_reconnect(root / "runtime-failure.aof")
        startup_unavailable_refuses_listener(root / "unavailable.aof")
    print("mysql_integration: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

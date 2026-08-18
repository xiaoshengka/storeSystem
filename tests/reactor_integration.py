#!/usr/bin/env python3
import concurrent.futures
import os
import shlex
import signal
import socket
import subprocess
import sys
import time
from typing import Sequence, Tuple, Union


HOST = "127.0.0.1"
PORT = 9096
RespValue = Tuple[str, Union[bytes, int, None]]


def encode_command(*arguments: bytes) -> bytes:
    parts = [f"*{len(arguments)}\r\n".encode("ascii")]
    for argument in arguments:
        parts.append(f"${len(argument)}\r\n".encode("ascii"))
        parts.append(argument)
        parts.append(b"\r\n")
    return b"".join(parts)


class RespReader:
    def __init__(self, client: socket.socket):
        self.client = client
        self.buffer = bytearray()

    def _receive(self) -> None:
        chunk = self.client.recv(65536)
        if not chunk:
            raise ConnectionError("server closed before a complete RESP response")
        self.buffer.extend(chunk)

    def _line(self, start: int) -> Tuple[bytes, int]:
        while True:
            end = self.buffer.find(b"\r\n", start)
            if end >= 0:
                return bytes(self.buffer[start:end]), end + 2
            self._receive()

    def read(self) -> RespValue:
        while not self.buffer:
            self._receive()
        prefix = self.buffer[0]
        if prefix in (ord("+"), ord("-"), ord(":")):
            line, end = self._line(1)
            del self.buffer[:end]
            if prefix == ord("+"):
                return "simple", line
            if prefix == ord("-"):
                return "error", line
            return "integer", int(line)
        if prefix == ord("$"):
            line, payload_start = self._line(1)
            length = int(line)
            if length == -1:
                del self.buffer[:payload_start]
                return "bulk", None
            required = payload_start + length + 2
            while len(self.buffer) < required:
                self._receive()
            if self.buffer[payload_start + length:required] != b"\r\n":
                raise AssertionError("invalid bulk response terminator")
            payload = bytes(self.buffer[payload_start:payload_start + length])
            del self.buffer[:required]
            return "bulk", payload
        raise AssertionError(f"unexpected RESP response prefix: {prefix!r}")


def wait_for_server(process: subprocess.Popen, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"server exited with status {process.returncode}")
        try:
            with socket.create_connection((HOST, PORT), timeout=0.1):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError("server did not listen on port 9096")


def connect() -> socket.socket:
    client = socket.create_connection((HOST, PORT), timeout=3.0)
    client.settimeout(5.0)
    return client


def exchange(arguments: Sequence[bytes], half_close: bool = False) -> RespValue:
    with connect() as client:
        client.sendall(encode_command(*arguments))
        if half_close:
            client.shutdown(socket.SHUT_WR)
        return RespReader(client).read()


def expect_protocol_error(payload: bytes, half_close: bool = False) -> None:
    with connect() as client:
        client.sendall(payload)
        if half_close:
            client.shutdown(socket.SHUT_WR)
        reader = RespReader(client)
        assert reader.read() == ("error", b"ERR Protocol error")
        assert client.recv(1) == b""


def test_basic_and_binary() -> None:
    key = b"binary\x00key"
    value = b"value\x00one"
    replacement = b"value\x00two\x00"

    assert exchange([b"PING"]) == ("simple", b"PONG")
    assert exchange([b"ping", b"hello\x00world"]) == (
        "bulk", b"hello\x00world"
    )
    assert exchange([b"SET", key, value]) == ("simple", b"OK")
    assert exchange([b"GET", key]) == ("bulk", value)
    assert exchange([b"set", key, replacement]) == ("simple", b"OK")
    assert exchange([b"GET", key]) == ("bulk", replacement)
    assert exchange([b"DEL", key]) == ("integer", 1)
    assert exchange([b"DEL", key]) == ("integer", 0)
    assert exchange([b"GET", key]) == ("bulk", None)
    assert exchange([b"SET", b"", b""]) == ("simple", b"OK")
    assert exchange([b"GET", b""]) == ("bulk", b"")
    assert exchange([b"UNKNOWN"]) == ("error", b"ERR unknown command")
    assert exchange([b"GET"]) == ("error", b"ERR wrong number of arguments")
    assert exchange([b"HGET", b"key"]) == ("error", b"ERR unknown command")
    assert exchange([b"PING"], half_close=True) == ("simple", b"PONG")
    assert exchange([b"SET", b"ttl-key", b"ttl-value", b"PX", b"500"]) == (
        "simple", b"OK"
    )
    ttl = exchange([b"PTTL", b"ttl-key"])
    assert ttl[0] == "integer" and isinstance(ttl[1], int)
    assert 0 <= ttl[1] <= 500
    assert exchange([b"PERSIST", b"ttl-key"]) == ("integer", 1)
    assert exchange([b"TTL", b"ttl-key"]) == ("integer", -1)
    assert exchange([b"DEL", b"ttl-key"]) == ("integer", 1)
    info = exchange([b"INFO", b"CACHE"])
    assert info[0] == "bulk" and isinstance(info[1], bytes)
    assert b"hits:" in info[1] and b"evicted_keys:" in info[1]


def test_fragmentation_and_pipeline() -> None:
    fragmented = encode_command(b"SET", b"fragmented", b"works")
    with connect() as client:
        reader = RespReader(client)
        for byte in fragmented:
            client.sendall(bytes([byte]))
            time.sleep(0.0005)
        assert reader.read() == ("simple", b"OK")

    commands = [
        encode_command(b"GET", b"fragmented"),
        encode_command(b"PING"),
        encode_command(b"DEL", b"fragmented"),
        encode_command(b"GET", b"fragmented"),
    ]
    payload = b"".join(commands)
    with connect() as client:
        reader = RespReader(client)
        for start in range(0, len(payload), 7):
            client.sendall(payload[start:start + 7])
        assert [reader.read() for _ in commands] == [
            ("bulk", b"works"),
            ("simple", b"PONG"),
            ("integer", 1),
            ("bulk", None),
        ]


def test_large_pipeline_backpressure() -> None:
    key = b"large-pipeline"
    value = bytes(range(256)) * 128  # 32 KiB, including many NUL bytes.
    count = 40  # Responses exceed the Reactor's 1 MiB output high-water mark.

    assert exchange([b"SET", key, value]) == ("simple", b"OK")
    payload = encode_command(b"GET", key) * count
    with connect() as client:
        client.sendall(payload)
        reader = RespReader(client)
        for _ in range(count):
            assert reader.read() == ("bulk", value)
    assert exchange([b"DEL", key]) == ("integer", 1)


def concurrent_case(index: int) -> None:
    key = f"parallel-{index}".encode("ascii")
    value = f"value-{index}".encode("ascii")
    with connect() as client:
        commands = b"".join([
            encode_command(b"SET", key, value),
            encode_command(b"GET", key),
            encode_command(b"DEL", key),
        ])
        client.sendall(commands)
        reader = RespReader(client)
        assert reader.read() == ("simple", b"OK")
        assert reader.read() == ("bulk", value)
        assert reader.read() == ("integer", 1)


def test_concurrency() -> None:
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        list(executor.map(concurrent_case, range(200)))


def test_protocol_failures() -> None:
    expect_protocol_error(b"PING\r\n")
    expect_protocol_error(b"*1\r\n$-1\r\n")
    expect_protocol_error(b"*1\r\n$65537\r\n")
    expect_protocol_error(b"*1\r\n$4\r\nPI", half_close=True)


def run_tests() -> None:
    test_basic_and_binary()
    test_fragmentation_and_pipeline()
    test_large_pipeline_backpressure()
    test_concurrency()
    test_protocol_failures()


def main() -> int:
    server_command = shlex.split(
        os.environ.get("KVSTORE_SERVER_COMMAND", "./kvstore")
    )
    process = subprocess.Popen(
        server_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    test_error = None
    try:
        wait_for_server(process)
        run_tests()
        print(f"reactor_integration ({' '.join(server_command)}): PASS")
    except BaseException as error:
        test_error = error
    finally:
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

    if test_error is not None:
        raise test_error
    if process.returncode not in (0, -signal.SIGTERM):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

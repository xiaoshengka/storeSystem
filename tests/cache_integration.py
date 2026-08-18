#!/usr/bin/env python3
import os
import shlex
import signal
import subprocess
import sys
import time

from reactor_integration import RespReader, connect, encode_command, wait_for_server


def exchange(reader: RespReader, client, *arguments: bytes):
    client.sendall(encode_command(*arguments))
    return reader.read()


def run_tests() -> None:
    with connect() as client:
        reader = RespReader(client)
        assert exchange(reader, client, b"SET", b"a", b"one") == (
            "simple", b"OK"
        )
        assert exchange(reader, client, b"SET", b"b", b"two") == (
            "simple", b"OK"
        )
        assert exchange(reader, client, b"GET", b"a") == ("bulk", b"one")
        assert exchange(reader, client, b"SET", b"c", b"three") == (
            "simple", b"OK"
        )
        assert exchange(reader, client, b"GET", b"b") == ("bulk", None)
        assert exchange(reader, client, b"GET", b"a") == ("bulk", b"one")
        assert exchange(reader, client, b"GET", b"c") == ("bulk", b"three")

        assert exchange(reader, client, b"PEXPIRE", b"c", b"50") == (
            "integer", 1
        )
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if exchange(reader, client, b"PTTL", b"c") == ("integer", -2):
                break
            time.sleep(0.02)
        else:
            raise AssertionError("active expiration did not remove c")

        info = exchange(reader, client, b"INFO", b"CACHE")
        assert info[0] == "bulk" and isinstance(info[1], bytes)
        assert b"maxkeys:2\r\n" in info[1]
        assert b"evicted_keys:1\r\n" in info[1]
        assert b"expired_keys:1\r\n" in info[1]


def main() -> int:
    command = shlex.split(
        os.environ.get(
            "KVSTORE_CACHE_SERVER_COMMAND",
            "./kvstore --maxmemory 1MiB --maxkeys 2",
        )
    )
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    error = None
    try:
        wait_for_server(process)
        run_tests()
        print(f"cache_integration ({' '.join(command)}): PASS")
    except BaseException as caught:
        error = caught
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
    if error is not None:
        raise error
    return 0 if process.returncode in (0, -signal.SIGTERM) else 1


if __name__ == "__main__":
    raise SystemExit(main())

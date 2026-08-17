#!/usr/bin/env python3
import concurrent.futures
import os
import shlex
import signal
import socket
import subprocess
import sys
import time


HOST = "127.0.0.1"
PORT = 9096


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


def request(command: str, half_close: bool = False) -> str:
    with socket.create_connection((HOST, PORT), timeout=2.0) as client:
        client.settimeout(2.0)
        client.sendall(command.encode("ascii"))
        if half_close:
            client.shutdown(socket.SHUT_WR)
        return client.recv(4096).decode("ascii")


def concurrent_case(index: int) -> None:
    key = f"parallel-{index}"
    if request(f"HSET {key} value-{index}") != "SUCCESS":
        raise AssertionError(f"HSET failed for {key}")
    if request(f"HGET {key}") != f"value-{index}":
        raise AssertionError(f"HGET failed for {key}")
    if request(f"HDEL {key}") != "SUCCESS":
        raise AssertionError(f"HDEL failed for {key}")


def run_tests() -> None:
    assert request("SET integration value") == "SUCCESS"
    assert request("GET integration") == "value"
    assert request("GET") == "ERROR wrong number of arguments"
    assert request("UNKNOWN") == "ERROR unknown command"
    assert request("GET integration", half_close=True) == "value"

    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        list(executor.map(concurrent_case, range(200)))

    for _ in range(500):
        assert request("HCOUNT").isdigit()

    client = socket.create_connection((HOST, PORT), timeout=2.0)
    client.sendall(b"SET abandoned")
    client.close()


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
        print("reactor_integration: PASS")
    except BaseException as error:  # Preserve the original test failure after cleanup.
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

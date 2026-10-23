import socket
import subprocess
import sys
import time


def kv(*args: str, node: str, timeout: str = "2.0") -> subprocess.CompletedProcess:
    command = [sys.executable, "-m", "client.cli", "--node", node, "--timeout", timeout, *args]
    return subprocess.run(command, capture_output=True, text=True)


def unused_port() -> int:
    with socket.socket() as sock:
        sock.bind(("localhost", 0))
        return sock.getsockname()[1]


def test_put(node):
    result = kv("put", "a", "1", node=node.address)
    assert result.returncode == 0
    assert result.stdout.strip() == "OK"


def test_get(node):
    kv("put", "a", "1", node=node.address)
    result = kv("get", "a", node=node.address)
    assert result.returncode == 0
    assert result.stdout.strip() == "1"


def test_get_missing(node):
    result = kv("get", "missing", node=node.address)
    assert result.returncode == 1
    assert result.stdout.strip() == "NOT FOUND"


def test_delete_then_get(node):
    kv("put", "a", "1", node=node.address)
    assert kv("delete", "a", node=node.address).returncode == 0
    assert kv("get", "a", node=node.address).returncode == 1


def test_empty_key_is_invalid(node):
    result = kv("put", "", "x", node=node.address)
    assert result.returncode == 2
    assert "INVALID REQUEST" in result.stderr


def test_unreachable_node():
    address = f"localhost:{unused_port()}"
    start = time.monotonic()
    result = kv("get", "a", node=address, timeout="0.5")
    assert result.returncode == 3
    assert f"ERROR: cannot reach {address}" in result.stderr
    assert time.monotonic() - start < 3


def test_value_with_spaces(node):
    kv("put", "greeting", "hello world", node=node.address)
    result = kv("get", "greeting", node=node.address)
    assert result.stdout.strip() == "hello world"

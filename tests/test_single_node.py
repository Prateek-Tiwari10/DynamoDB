"""One real node with N = 1: the Phase 1 behaviour through gRPC, plus a real kill -9."""

import os
import signal
import socket
import subprocess
import sys
import time

import grpc
import pytest

from dynamo import dynamo_pb2, dynamo_pb2_grpc
from dynamo.config import ClusterConfig
from dynamo.server import build_node

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ONE_NODE = ClusterConfig(nodes={"node1": "localhost:50051"}, n=1, r=1, w=1)


@pytest.fixture
async def kv(tmp_path):
    node = build_node(ONE_NODE, "node1", 0, str(tmp_path))
    await node.start()
    channel = grpc.aio.insecure_channel(f"localhost:{node.port}")
    yield dynamo_pb2_grpc.KVStoreStub(channel)
    await channel.close()
    await node.stop()


async def test_put_get_delete(kv):
    put = await kv.Put(dynamo_pb2.PutRequest(key="cart:42", value=b"book"))
    assert put.status == dynamo_pb2.OK
    got = await kv.Get(dynamo_pb2.GetRequest(key="cart:42"))
    assert (got.status, list(got.values)) == (dynamo_pb2.OK, [b"book"])
    deleted = await kv.Delete(dynamo_pb2.DeleteRequest(key="cart:42", context=got.context))
    assert deleted.status == dynamo_pb2.OK
    gone = await kv.Get(dynamo_pb2.GetRequest(key="cart:42"))
    assert gone.status == dynamo_pb2.NOT_FOUND
    assert gone.context  # the tombstone's clock comes back


async def test_info_counts_keys_and_tombstones(kv):
    await kv.Put(dynamo_pb2.PutRequest(key="a", value=b"1"))
    await kv.Put(dynamo_pb2.PutRequest(key="b", value=b"2"))
    await kv.Delete(dynamo_pb2.DeleteRequest(key="b"))
    info = await kv.ClusterInfo(dynamo_pb2.Empty())
    assert (info.live_keys, info.tombstones) == (1, 1)


async def test_quorum_above_n_invalid(kv):
    assert (await kv.Put(dynamo_pb2.PutRequest(key="k", value=b"v", w=2))).status == dynamo_pb2.INVALID
    assert (await kv.Get(dynamo_pb2.GetRequest(key="k", r=2))).status == dynamo_pb2.INVALID


async def test_bad_context_invalid(kv):
    reply = await kv.Put(dynamo_pb2.PutRequest(key="k", value=b"v", context=b"\xff\xff\xff"))
    assert reply.status == dynamo_pb2.INVALID


# ---------- Phase 1 demo as a test: real process, real SIGKILL ----------

def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def start_node(env: dict) -> subprocess.Popen:
    process = subprocess.Popen([sys.executable, "-m", "dynamo.server"], env=env, cwd=REPO_ROOT,
                               stderr=subprocess.PIPE, text=True)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if "listening on" in process.stderr.readline():
            return process
    process.kill()
    raise AssertionError("node did not start")


def kv_cli(env: dict, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, "-m", "client.cli", *args], env=env, cwd=REPO_ROOT,
                          capture_output=True, text=True, timeout=10)


def test_phase1_kill9(tmp_path):
    port = free_port()
    config = tmp_path / "single.yaml"
    config.write_text(f"nodes:\n  node1: localhost:{port}\nn: 1\nr: 1\nw: 1\n")
    env = dict(os.environ, NODE_ID="node1", CONFIG=str(config), DATA_DIR=str(tmp_path / "data"),
               KV_NODE=f"localhost:{port}", KV_CONTEXT=str(tmp_path / "ctx.json"),
               PYTHONPATH=REPO_ROOT, PYTHONUNBUFFERED="1")

    node = start_node(env)
    try:
        put = kv_cli(env, "put", "demo:1", "hello")
        assert (put.returncode, put.stdout) == (0, "OK\n"), put.stderr
        node.send_signal(signal.SIGKILL)               # crash right after the acknowledged write
        node.wait(timeout=5)

        node = start_node(env)
        got = kv_cli(env, "get", "demo:1")
        assert (got.returncode, got.stdout) == (0, "hello\n"), got.stderr
    finally:
        if node.poll() is None:
            node.terminate()
            node.wait(timeout=5)

"""Tests for dynamo/server.py: wiring, flags, startup and a single running node."""

import os
import signal
import socket
import subprocess
import sys
import time

import grpc
import pytest

from dynamo import dynamo_pb2, dynamo_pb2_grpc, server
from dynamo.config import ClusterConfig
from dynamo.reqid import METADATA_KEY
from dynamo.server import build_node, parse_args, validate

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def make_config() -> ClusterConfig:
    return ClusterConfig(nodes={f"node{i}": f"localhost:{50060 + i}" for i in range(1, 6)})


# ---------- build_node ----------

async def test_build_node_no_io(tmp_path):
    port = free_port()
    node = build_node(make_config(), "node1", port, str(tmp_path))
    assert os.listdir(tmp_path) == []        # no data folder yet
    with socket.socket() as sock:             # port still free
        sock.bind(("0.0.0.0", port))
    assert node.node_id == "node1"
    assert node.port == port
    # grpc on Python 3.14 can crash when a never-started aio server is garbage collected,
    # so run the node once before letting go of it.
    await node.start()
    await node.stop()


async def test_build_node_unknown_id(tmp_path):
    with pytest.raises(ValueError):
        build_node(make_config(), "node9", 0, str(tmp_path))


# ---------- flags ----------

def test_flags_default_from_env(monkeypatch):
    monkeypatch.setenv("NODE_ID", "node3")
    monkeypatch.setenv("CONFIG", "cluster.yaml")
    monkeypatch.setenv("PORT", "6000")
    monkeypatch.setenv("DATA_DIR", "/data")
    args = parse_args([])
    assert (args.node_id, args.config, args.port, args.data_dir) == ("node3", "cluster.yaml", 6000, "/data")


def test_flags_override_env(monkeypatch):
    monkeypatch.setenv("NODE_ID", "node3")
    assert parse_args(["--node-id", "node1"]).node_id == "node1"


def test_defaults_without_env(monkeypatch):
    for name in ("CONFIG", "PORT", "DATA_DIR"):
        monkeypatch.delenv(name, raising=False)
    args = parse_args(["--node-id", "node1"])
    assert (args.config, args.port, args.data_dir) == ("cluster.local.yaml", None, "./data")


def test_startup_without_node_id(monkeypatch):
    monkeypatch.delenv("NODE_ID", raising=False)
    with pytest.raises(SystemExit):
        parse_args([])


async def test_startup_with_node_id_not_in_config(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "setup_logging", lambda: None)
    args = parse_args(["--node-id", "node9", "--config", os.path.join(REPO_ROOT, "cluster.local.yaml"),
                       "--data-dir", str(tmp_path)])
    with pytest.raises(SystemExit, match="not in config"):
        await server.serve(args)


async def test_startup_with_bad_yaml(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "setup_logging", lambda: None)
    bad = tmp_path / "bad.yaml"
    bad.write_text("nodes: [unclosed\n")
    args = parse_args(["--node-id", "node1", "--config", str(bad)])
    with pytest.raises(SystemExit, match="cannot load config"):
        await server.serve(args)


# ---------- validation ----------

def test_validate():
    assert validate("k", b"v", 0, 3)
    assert validate("k" * 256, b"x" * (1024 * 1024), 3, 3)
    assert not validate("", b"v", 0, 3)
    assert not validate("k" * 257, b"v", 0, 3)
    assert not validate("k", b"x" * (1024 * 1024 + 1), 0, 3)
    assert not validate("k", None, 4, 3)


# ---------- one running node ----------

@pytest.fixture
async def running(tmp_path):
    node = build_node(make_config(), "node1", 0, str(tmp_path))
    await node.start()
    channel = grpc.aio.insecure_channel(f"localhost:{node.port}")
    yield node, channel, tmp_path
    await channel.close()
    await node.stop()


async def test_start_binds_port_and_creates_data_folder(running):
    node, _, tmp_path = running
    assert node.port > 0
    assert os.path.isdir(tmp_path / "node1")


async def test_cluster_info_is_local(running):
    node, channel, _ = running
    info = await dynamo_pb2_grpc.KVStoreStub(channel).ClusterInfo(dynamo_pb2.Empty())
    assert info.node_id == "node1"
    assert (info.n, info.r, info.w, info.vnodes) == (3, 2, 2, 32)
    assert len(info.members) == 5
    assert info.ring_fingerprint == node.ring.fingerprint(3)
    assert (info.live_keys, info.tombstones) == (0, 0)


async def test_locate_uses_local_ring(running):
    node, channel, _ = running
    reply = await dynamo_pb2_grpc.KVStoreStub(channel).Locate(dynamo_pb2.LocateRequest(key="cart:42"))
    assert list(reply.nodes) == node.ring.preference_list("cart:42", 3)
    assert reply.position == node.ring.position("cart:42")


async def test_stub_rpcs_unimplemented(running):
    _, channel, _ = running
    kv = dynamo_pb2_grpc.KVStoreStub(channel)
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await kv.Put(dynamo_pb2.PutRequest(key="k", value=b"v"), metadata=((METADATA_KEY, "id-1"),))
    assert error.value.code() == grpc.StatusCode.UNIMPLEMENTED
    assert dict(error.value.trailing_metadata())[METADATA_KEY] == "id-1"
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await dynamo_pb2_grpc.NodeServiceStub(channel).Gossip(dynamo_pb2.GossipMessage())
    assert error.value.code() == grpc.StatusCode.UNIMPLEMENTED


async def test_invalid_request_answers_invalid(running):
    _, channel, _ = running
    reply = await dynamo_pb2_grpc.KVStoreStub(channel).Put(dynamo_pb2.PutRequest(key="", value=b"v"))
    assert reply.status == dynamo_pb2.INVALID


async def test_replica_put_then_get_and_local_get(running):
    _, channel, _ = running
    nodes = dynamo_pb2_grpc.NodeServiceStub(channel)
    kv = dynamo_pb2_grpc.KVStoreStub(channel)
    assert (await kv.LocalGet(dynamo_pb2.LocalGetRequest(key="k"))).status == dynamo_pb2.NOT_FOUND

    version = dynamo_pb2.Version(value=b"book", clock=dynamo_pb2.VectorClock(counters={"node1": 1}))
    reply = await nodes.ReplicaPut(dynamo_pb2.ReplicaPutRequest(sender="node2", key="k", version=version))
    assert (reply.ok, reply.node_id) == (True, "node1")

    stored = await nodes.ReplicaGet(dynamo_pb2.ReplicaGetRequest(sender="node2", key="k"))
    assert [v.value for v in stored.versions] == [b"book"]
    assert dict(stored.versions[0].clock.counters) == {"node1": 1}

    local = await kv.LocalGet(dynamo_pb2.LocalGetRequest(key="k"))
    assert local.status == dynamo_pb2.OK
    assert [v.value for v in local.versions] == [b"book"]


async def test_hinted_replica_put_unimplemented(running):
    _, channel, _ = running
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await dynamo_pb2_grpc.NodeServiceStub(channel).ReplicaPut(
            dynamo_pb2.ReplicaPutRequest(key="k", version=dynamo_pb2.Version(), hint_for="node2"))
    assert error.value.code() == grpc.StatusCode.UNIMPLEMENTED


async def test_restart_keeps_data(tmp_path):
    version = dynamo_pb2.Version(value=b"kept")
    node = build_node(make_config(), "node1", 0, str(tmp_path))
    await node.start()
    async with grpc.aio.insecure_channel(f"localhost:{node.port}") as channel:
        await dynamo_pb2_grpc.NodeServiceStub(channel).ReplicaPut(
            dynamo_pb2.ReplicaPutRequest(key="k", version=version))
    await node.stop()

    node = build_node(make_config(), "node1", 0, str(tmp_path))
    await node.start()
    async with grpc.aio.insecure_channel(f"localhost:{node.port}") as channel:
        local = await dynamo_pb2_grpc.KVStoreStub(channel).LocalGet(dynamo_pb2.LocalGetRequest(key="k"))
    await node.stop()
    assert [v.value for v in local.versions] == [b"kept"]


# ---------- real process ----------

def test_sigterm_stops_node(tmp_path):
    port = free_port()
    env = dict(os.environ, NODE_ID="node1", CONFIG=os.path.join(REPO_ROOT, "cluster.local.yaml"),
               PORT=str(port), DATA_DIR=str(tmp_path), PYTHONPATH=REPO_ROOT, PYTHONUNBUFFERED="1")
    process = subprocess.Popen([sys.executable, "-m", "dynamo.server"], env=env, cwd=REPO_ROOT,
                               stderr=subprocess.PIPE, text=True)
    try:
        deadline = time.monotonic() + 10
        started = False
        while time.monotonic() < deadline:
            line = process.stderr.readline()
            if "listening on" in line:
                started = True
                break
        assert started, "node did not start"
        assert "[node1]" in line and f"0.0.0.0:{port}" in line

        process.send_signal(signal.SIGTERM)
        assert process.wait(timeout=5) == 0
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()

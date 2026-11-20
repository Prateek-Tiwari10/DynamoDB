"""Put / Get / Delete through a real 5-node cluster in one test process (N=3, R=2, W=2).

A node "down" is a node stopped with node.stop(): its port is closed, so calls to it fail
with UNAVAILABLE. Uses a small local fixture until the shared one in conftest.py exists.
"""

import asyncio
import socket

import grpc
import pytest

from dynamo import dynamo_pb2, dynamo_pb2_grpc
from dynamo.config import ClusterConfig
from dynamo.ring import Ring
from dynamo.server import build_node
from dynamo.vclock import VectorClock
from dynamo.version import Version
from dynamo.wire import version_to_proto

NODE_IDS = [f"node{i}" for i in range(1, 6)]
RING = Ring(NODE_IDS, 32)        # the same ring every node builds


def free_ports(count: int) -> list[int]:
    sockets = [socket.socket() for _ in range(count)]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


class Cluster:
    def __init__(self, config: ClusterConfig, data_dir: str) -> None:
        self.config = config
        self.data_dir = data_dir
        self.nodes = {}
        self.channels = {}

    async def start(self) -> None:
        for node_id, address in self.config.nodes.items():
            port = int(address.rsplit(":", 1)[1])
            self.nodes[node_id] = build_node(self.config, node_id, port, self.data_dir)
            await self.nodes[node_id].start()
            self.channels[node_id] = grpc.aio.insecure_channel(address)

    async def down(self, *node_ids: str) -> None:
        for node_id in node_ids:
            await self.nodes.pop(node_id).stop()

    def kv(self, node_id: str) -> dynamo_pb2_grpc.KVStoreStub:
        return dynamo_pb2_grpc.KVStoreStub(self.channels[node_id])

    def node_service(self, node_id: str) -> dynamo_pb2_grpc.NodeServiceStub:
        return dynamo_pb2_grpc.NodeServiceStub(self.channels[node_id])

    async def local_values(self, node_id: str, key: str) -> list[bytes]:
        reply = await self.kv(node_id).LocalGet(dynamo_pb2.LocalGetRequest(key=key))
        return [v.value for v in reply.versions if not v.tombstone]

    async def stop(self) -> None:
        for channel in self.channels.values():
            await channel.close()
        for node in self.nodes.values():
            await node.stop()


@pytest.fixture
async def cluster(tmp_path):
    ports = free_ports(len(NODE_IDS))
    config = ClusterConfig(nodes={node_id: f"localhost:{port}" for node_id, port in zip(NODE_IDS, ports)})
    running = Cluster(config, str(tmp_path))
    await running.start()
    yield running
    await running.stop()


def replicas(key: str) -> list[str]:
    return RING.preference_list(key, 3)


def outsiders(key: str) -> list[str]:
    return [node_id for node_id in NODE_IDS if node_id not in replicas(key)]


async def eventually(check, timeout: float = 2.0) -> None:
    """Retry an async check until it is true (background replica writes)."""
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not await check():
        assert loop.time() < end, "condition not reached in time"
        await asyncio.sleep(0.02)


KEY = "cart:42"


# ---------- routing ----------

async def test_write_via_node1_read_via_node4(cluster):
    put = await cluster.kv("node1").Put(dynamo_pb2.PutRequest(key="user:7", value=b"alice"))
    assert put.status == dynamo_pb2.OK
    got = await cluster.kv("node4").Get(dynamo_pb2.GetRequest(key="user:7"))
    assert (got.status, list(got.values)) == (dynamo_pb2.OK, [b"alice"])


async def test_write_forwarded_to_first_replica(cluster):
    first, second, third = replicas(KEY)
    for entry in (second, outsiders(KEY)[0]):          # replica 2, and a non-replica
        put = await cluster.kv(entry).Put(dynamo_pb2.PutRequest(key=KEY, value=entry.encode()))
        assert put.status == dynamo_pb2.OK
        assert put.written_to[0] == first               # the first replica coordinated
        assert set(put.written_to) <= {first, second, third}


async def test_get_no_extra_hop(cluster):
    await cluster.kv("node1").Put(dynamo_pb2.PutRequest(key=KEY, value=b"v"))
    second = replicas(KEY)[1]
    got = await cluster.kv(second).Get(dynamo_pb2.GetRequest(key=KEY))
    assert got.read_from[0] == second                   # replica 2 coordinated the read itself


async def test_get_forwarded_from_non_replica(cluster):
    await cluster.kv("node1").Put(dynamo_pb2.PutRequest(key=KEY, value=b"v"))
    got = await cluster.kv(outsiders(KEY)[0]).Get(dynamo_pb2.GetRequest(key=KEY))
    assert got.read_from[0] == replicas(KEY)[0]
    assert list(got.values) == [b"v"]


async def test_forward_fallback(cluster):
    first, second, _ = replicas(KEY)
    await cluster.down(first)
    put = await cluster.kv(outsiders(KEY)[0]).Put(dynamo_pb2.PutRequest(key=KEY, value=b"v"))
    assert put.status == dynamo_pb2.OK
    assert put.written_to[0] == second                  # the next replica coordinated


async def test_forward_reaches_self(cluster):
    first, second, _ = replicas(KEY)
    await cluster.down(first)
    put = await cluster.kv(second).Put(dynamo_pb2.PutRequest(key=KEY, value=b"v"))
    assert put.status == dynamo_pb2.OK
    assert put.written_to[0] == second


async def test_forward_never_reforwarded(cluster):
    outsider = outsiders(KEY)[0]
    forward = dynamo_pb2.ForwardRequest(sender="node9", put=dynamo_pb2.PutRequest(key=KEY, value=b"v"))
    reply = await cluster.node_service(outsider).Forward(forward)
    assert reply.put.status == dynamo_pb2.OK
    assert reply.put.written_to[0] == outsider          # coordinated where it arrived


# ---------- quorum ----------

async def test_one_replica_down_still_works(cluster):
    await cluster.down(replicas(KEY)[2])
    entry = outsiders(KEY)[0]
    put = await cluster.kv(entry).Put(dynamo_pb2.PutRequest(key=KEY, value=b"v"))
    assert put.status == dynamo_pb2.OK
    got = await cluster.kv(entry).Get(dynamo_pb2.GetRequest(key=KEY))
    assert (got.status, list(got.values)) == (dynamo_pb2.OK, [b"v"])


async def test_two_replicas_down_quorum_failed(cluster):
    first, second, third = replicas(KEY)
    await cluster.down(second, third)
    put = await cluster.kv(first).Put(dynamo_pb2.PutRequest(key=KEY, value=b"v"))
    assert put.status == dynamo_pb2.QUORUM_FAILED
    assert await cluster.local_values(first, KEY) == [b"v"]      # no rollback
    got = await cluster.kv(first).Get(dynamo_pb2.GetRequest(key=KEY))
    assert got.status == dynamo_pb2.QUORUM_FAILED


async def test_w1_returns_after_local_write_and_late_writes_land(cluster):
    first, second, third = replicas(KEY)
    put = await cluster.kv(first).Put(dynamo_pb2.PutRequest(key=KEY, value=b"v", w=1))
    assert put.status == dynamo_pb2.OK
    assert list(put.written_to) == [first]

    async def everywhere() -> bool:
        values = [await cluster.local_values(n, KEY) for n in (second, third)]
        return values == [[b"v"], [b"v"]]
    await eventually(everywhere)


async def test_w3_one_down(cluster):
    first, second, third = replicas(KEY)
    await cluster.down(third)
    put = await cluster.kv(first).Put(dynamo_pb2.PutRequest(key=KEY, value=b"v", w=3))
    assert put.status == dynamo_pb2.QUORUM_FAILED
    assert await cluster.local_values(first, KEY) == [b"v"]
    assert await cluster.local_values(second, KEY) == [b"v"]


async def test_not_found_counts_toward_r(cluster):
    got = await cluster.kv(replicas("never-written")[0]).Get(dynamo_pb2.GetRequest(key="never-written"))
    assert got.status == dynamo_pb2.NOT_FOUND
    assert len(got.read_from) >= 2       # at least R; both others may answer in the same wake-up


# ---------- versions ----------

async def test_get_returns_siblings(cluster):
    first, second, _ = replicas(KEY)
    # two concurrent versions, each stored on one replica only
    for node_id, value in ((first, b"red"), (second, b"blue")):
        version = Version(value=value, clock=VectorClock(counters={node_id: 1}))
        await cluster.node_service(node_id).ReplicaPut(dynamo_pb2.ReplicaPutRequest(
            sender="test", key=KEY, version=version_to_proto(version)))
    got = await cluster.kv(first).Get(dynamo_pb2.GetRequest(key=KEY, r=3))
    assert got.status == dynamo_pb2.OK
    assert sorted(got.values) == [b"blue", b"red"]


async def test_get_after_delete_cluster(cluster):
    put = await cluster.kv("node2").Put(dynamo_pb2.PutRequest(key=KEY, value=b"v"))
    deleted = await cluster.kv("node3").Delete(dynamo_pb2.DeleteRequest(key=KEY, context=put.context))
    assert deleted.status == dynamo_pb2.OK
    gone = await cluster.kv("node4").Get(dynamo_pb2.GetRequest(key=KEY))
    assert gone.status == dynamo_pb2.NOT_FOUND
    assert gone.context
    back = await cluster.kv("node5").Put(dynamo_pb2.PutRequest(key=KEY, value=b"back", context=gone.context))
    assert back.status == dynamo_pb2.OK
    again = await cluster.kv("node1").Get(dynamo_pb2.GetRequest(key=KEY))
    assert list(again.values) == [b"back"]               # the put replaced the tombstone


async def test_put_with_context_replaces_value(cluster):
    first = await cluster.kv("node1").Put(dynamo_pb2.PutRequest(key=KEY, value=b"v1"))
    await cluster.kv("node2").Put(dynamo_pb2.PutRequest(key=KEY, value=b"v2", context=first.context))
    got = await cluster.kv("node3").Get(dynamo_pb2.GetRequest(key=KEY, r=3))
    assert list(got.values) == [b"v2"]


async def test_put_bad_context_invalid(cluster):
    put = await cluster.kv("node1").Put(dynamo_pb2.PutRequest(key=KEY, value=b"v", context=b"junk"))
    assert put.status == dynamo_pb2.INVALID
    for node_id in replicas(KEY):
        assert await cluster.local_values(node_id, KEY) == []

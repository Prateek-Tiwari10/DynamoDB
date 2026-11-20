"""The shared in-process test cluster: start/stop, kill and restart, stop vs kill, two clusters."""

import asyncio

import grpc
import pytest

from dynamo import dynamo_pb2
from dynamo.gossip import gossip_loop
from conftest import wait_until


async def put_on(cluster, node_id: str, key: str, value: bytes) -> None:
    """Store a version directly on one node, the way a coordinator's ReplicaPut would."""
    await cluster.replica(node_id).ReplicaPut(dynamo_pb2.ReplicaPutRequest(
        sender="node1", key=key, version=dynamo_pb2.Version(value=value)))


async def local_values(cluster, node_id: str, key: str) -> list[bytes]:
    reply = await cluster.client(node_id).LocalGet(dynamo_pb2.LocalGetRequest(key=key))
    return [v.value for v in reply.versions]


async def own_incarnation(cluster, node_id: str) -> int:
    info = await cluster.client(node_id).ClusterInfo(dynamo_pb2.Empty())
    return next(m.incarnation for m in info.members if m.node_id == node_id)


async def test_five_nodes_start_and_answer(cluster):
    assert sorted(cluster.nodes) == [f"node{i}" for i in range(1, 6)]
    for node_id in cluster.nodes:
        info = await cluster.client(node_id).ClusterInfo(dynamo_pb2.Empty())
        assert info.node_id == node_id
        assert len(info.members) == 5
    assert cluster.config.gossip_interval_ms == 100
    assert cluster.config.dead_after_ms == 1000


async def test_stopped_node_is_unreachable_others_still_answer(cluster):
    await cluster.stop_node("node2")
    assert "node2" not in cluster.nodes
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await cluster.client("node2").ClusterInfo(dynamo_pb2.Empty(), timeout=1)
    assert error.value.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)
    info = await cluster.client("node1").ClusterInfo(dynamo_pb2.Empty())
    assert info.node_id == "node1"


async def test_kill_and_restart_keeps_data_and_gets_new_incarnation(cluster):
    await put_on(cluster, "node3", "k", b"kept")
    before = await own_incarnation(cluster, "node3")
    await asyncio.sleep(0.002)   # incarnation is boot time in ms; make sure the clock moves

    await cluster.kill("node3")
    await cluster.restart("node3")

    assert await local_values(cluster, "node3", "k") == [b"kept"]
    assert cluster.nodes["node3"].port == cluster.ports["node3"]
    assert await own_incarnation(cluster, "node3") > before


async def test_stop_then_restart_keeps_data(cluster):
    await put_on(cluster, "node4", "k", b"kept")
    await cluster.stop_node("node4")
    await cluster.restart("node4")
    assert await local_values(cluster, "node4", "k") == [b"kept"]


async def test_stop_closes_storage_kill_does_not(cluster):
    stopped = cluster.nodes["node1"]
    killed = cluster.nodes["node2"]
    await cluster.stop_node("node1")
    await cluster.kill("node2")

    with pytest.raises(RuntimeError):
        stopped.storage.get("k")       # closed by a clean stop
    assert killed.storage.get("k") == []   # a crash never closes storage


async def test_restart_running_node_is_an_error(cluster):
    with pytest.raises(RuntimeError):
        await cluster.restart("node1")


async def test_peer_reaches_restarted_node(cluster):
    await cluster.kill("node3")
    await cluster.restart("node3")
    peer = cluster.nodes["node1"].peers.stub("node3")

    async def reachable() -> bool:
        try:
            await peer.ReplicaPut(dynamo_pb2.ReplicaPutRequest(
                sender="node1", key="k", version=dynamo_pb2.Version(value=b"v")), timeout=0.2)
            return True
        except grpc.aio.AioRpcError:
            return False

    await cluster.wait_until(reachable, timeout=3)


async def test_two_clusters_are_separate(make_cluster):
    first = await make_cluster(3)
    second = await make_cluster(3)
    assert not set(first.ports.values()) & set(second.ports.values())
    assert first.data_dir != second.data_dir

    await put_on(first, "node1", "k", b"first")
    assert await local_values(first, "node1", "k") == [b"first"]
    assert await local_values(second, "node1", "k") == []


async def test_small_cluster_shrinks_quorum(make_cluster):
    small = await make_cluster(2)
    assert (small.config.n, small.config.r, small.config.w) == (2, 2, 2)


async def test_wait_until_plain_and_async_conditions():
    calls = []

    def plain() -> bool:
        calls.append(1)
        return len(calls) >= 3

    async def async_true() -> bool:
        return True

    await wait_until(plain, timeout=1, interval=0.01)
    await wait_until(async_true, timeout=1)
    assert len(calls) == 3


async def test_wait_until_times_out():
    with pytest.raises(AssertionError):
        await wait_until(lambda: False, timeout=0.1, interval=0.01)


async def test_gossip_loop_is_a_stub():
    with pytest.raises(NotImplementedError):
        await gossip_loop(None)

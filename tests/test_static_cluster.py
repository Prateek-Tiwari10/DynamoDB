"""Five real nodes in one test process: they all build the same ring and see each other.

Uses a small local fixture until the shared cluster fixture in conftest.py exists.
"""

import socket

import grpc
import pytest

from dynamo import dynamo_pb2, dynamo_pb2_grpc
from dynamo.config import ClusterConfig
from dynamo.server import build_node

NODE_IDS = [f"node{i}" for i in range(1, 6)]


def free_ports(count: int) -> list[int]:
    sockets = [socket.socket() for _ in range(count)]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


@pytest.fixture
async def cluster(tmp_path):
    ports = dict(zip(NODE_IDS, free_ports(len(NODE_IDS))))
    config = ClusterConfig(nodes={node_id: f"localhost:{port}" for node_id, port in ports.items()})
    nodes = {node_id: build_node(config, node_id, ports[node_id], str(tmp_path)) for node_id in NODE_IDS}
    for node in nodes.values():
        await node.start()
    channels = {node_id: grpc.aio.insecure_channel(f"localhost:{port}") for node_id, port in ports.items()}
    yield {"config": config, "nodes": nodes, "channels": channels, "tmp_path": tmp_path}
    for channel in channels.values():
        await channel.close()
    for node in nodes.values():
        await node.stop()


def kv(cluster, node_id: str) -> dynamo_pb2_grpc.KVStoreStub:
    return dynamo_pb2_grpc.KVStoreStub(cluster["channels"][node_id])


async def test_cluster_agrees(cluster):
    infos = [await kv(cluster, node_id).ClusterInfo(dynamo_pb2.Empty()) for node_id in NODE_IDS]
    assert [info.node_id for info in infos] == NODE_IDS
    for info in infos:
        assert sorted(member.node_id for member in info.members) == NODE_IDS
        assert all(member.state == dynamo_pb2.ALIVE for member in info.members)
    assert len({info.ring_fingerprint for info in infos}) == 1


@pytest.mark.parametrize("key", ["cart:42", "user:7", "order:1001"])
async def test_locate_same_on_every_node(cluster, key):
    answers = {tuple((await kv(cluster, node_id).Locate(dynamo_pb2.LocateRequest(key=key))).nodes)
               for node_id in NODE_IDS}
    assert len(answers) == 1
    (replicas,) = answers
    assert len(set(replicas)) == 3


async def test_cluster_restart_keeps_node_data(cluster):
    node3 = cluster["nodes"]["node3"]
    port = node3.port
    replica = dynamo_pb2_grpc.NodeServiceStub(cluster["channels"]["node3"])
    await replica.ReplicaPut(dynamo_pb2.ReplicaPutRequest(
        sender="node1", key="k", version=dynamo_pb2.Version(value=b"kept")))

    await node3.stop()
    restarted = build_node(cluster["config"], "node3", port, str(cluster["tmp_path"]))
    await restarted.start()
    cluster["nodes"]["node3"] = restarted  # the fixture stops this one at teardown

    async with grpc.aio.insecure_channel(f"localhost:{port}") as channel:
        local = await dynamo_pb2_grpc.KVStoreStub(channel).LocalGet(dynamo_pb2.LocalGetRequest(key="k"))
    assert [v.value for v in local.versions] == [b"kept"]

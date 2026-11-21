"""Hinted handoff on a real 5-node in-process cluster (handoff_retry_ms 300 in test timers).

Until D8 (sloppy quorum) sends hinted writes itself, the tests send the hinted ReplicaPut
straight to a stand-in, as D8's coordinator will.
"""

from dynamo import dynamo_pb2
from dynamo.vclock import VectorClock
from dynamo.version import Version
from dynamo.wire import version_to_proto

KEY = "cart:42"


def owner_and_stand_in(cluster) -> tuple[str, str]:
    """The key's third replica (the owner we take down) and a node that is not a replica."""
    any_node = next(iter(cluster.nodes.values()))
    replicas = any_node.ring.preference_list(KEY, cluster.config.n)
    stand_in = next(node_id for node_id in cluster.config.nodes if node_id not in replicas)
    return replicas[2], stand_in


async def send_hint(cluster, stand_in: str, owner: str, value: bytes) -> None:
    version = Version(value=value, clock=VectorClock(counters={"node1": 1}))
    reply = await cluster.replica(stand_in).ReplicaPut(dynamo_pb2.ReplicaPutRequest(
        sender="test", key=KEY, version=version_to_proto(version), hint_for=owner), timeout=2)
    assert reply.ok


async def owner_has(cluster, owner: str, value: bytes) -> bool:
    reply = await cluster.client(owner).LocalGet(dynamo_pb2.LocalGetRequest(key=KEY), timeout=2)
    return [v.value for v in reply.versions] == [value]


async def test_hint_delivered_when_owner_returns(cluster):
    owner, stand_in = owner_and_stand_in(cluster)
    await cluster.kill(owner)
    await send_hint(cluster, stand_in, owner, b"book")
    assert list(cluster.nodes[stand_in].storage.hints()) == [owner]

    await cluster.restart(owner)
    await cluster.wait_until(lambda: owner_has(cluster, owner, b"book"), timeout=8)
    await cluster.wait_until(lambda: cluster.nodes[stand_in].storage.hints() == {})


async def test_hint_survives_stand_in_restart(cluster):
    owner, stand_in = owner_and_stand_in(cluster)
    await cluster.kill(owner)
    await send_hint(cluster, stand_in, owner, b"book")
    await cluster.kill(stand_in)                                  # crash before delivering
    await cluster.restart(stand_in)
    assert list(cluster.nodes[stand_in].storage.hints()) == [owner]   # replayed from the log

    await cluster.restart(owner)
    await cluster.wait_until(lambda: owner_has(cluster, owner, b"book"), timeout=8)
    await cluster.wait_until(lambda: cluster.nodes[stand_in].storage.hints() == {})


async def test_hint_not_sent_while_owner_is_down(cluster):
    owner, stand_in = owner_and_stand_in(cluster)
    await cluster.kill(owner)
    await cluster.wait_until(
        lambda: cluster.nodes[stand_in].membership.state(owner) == dynamo_pb2.DEAD)
    await send_hint(cluster, stand_in, owner, b"book")
    assert await cluster.nodes[stand_in].handoff.deliver_once() == 0
    assert list(cluster.nodes[stand_in].storage.hints()) == [owner]

"""Real-cluster test 9 (D10 §5): Merkle anti-entropy copies a key that only one replica has to
the other two, with no client read at all (anti-entropy every 10 s, round-robin partners)."""

from dynamo import dynamo_pb2
from dynamo.vclock import VectorClock
from dynamo.version import Version
from dynamo.wire import version_to_proto
from cluster.helpers import local_values, wait_until
from tools.cluster_control import CALL_TIMEOUT_S


async def test_9_anti_entropy_without_reads(cluster, fresh):
    key = fresh("t9")
    a, b, c = await cluster.owners(key)
    version = Version(value=b"one-copy", clock=VectorClock(counters={a: 1}, updated_ms={a: 1}))
    reply = await cluster.peer(a).ReplicaPut(dynamo_pb2.ReplicaPutRequest(
        sender="test", key=key, version=version_to_proto(version)), timeout=CALL_TIMEOUT_S)
    assert reply.ok
    assert await local_values(cluster, b, key) == []

    for owner in (b, c):
        await wait_until(lambda o=owner: _has(cluster, o, key), 60, f"anti-entropy to {owner}")


async def _has(cluster, node: str, key: str) -> bool:
    return await local_values(cluster, node, key) == [b"one-copy"]

"""Read repair and DEAD-aware routing on a real 5-node in-process cluster (N=3, R=2, W=2).

A replica is made stale by killing it, writing, and restarting it. Right after a restart the
coordinator's channel to it can need about 1 s (gRPC reconnect backoff) and gossip may still
call it DEAD, so the tests read again until the repair shows up instead of reading once.
"""

import logging

from dynamo import dynamo_pb2
from dynamo.vclock import VectorClock
from dynamo.version import Version
from dynamo.wire import version_to_proto

KEY = "cart:42"


def replicas(cluster, key: str = KEY) -> list[str]:
    any_node = next(iter(cluster.nodes.values()))
    return any_node.ring.preference_list(key, cluster.config.n)


async def held(cluster, node_id: str, key: str = KEY) -> list[tuple[bytes, bool]]:
    """What ``node_id`` itself stores for ``key``: (value, tombstone) per version, sorted."""
    reply = await cluster.client(node_id).LocalGet(dynamo_pb2.LocalGetRequest(key=key), timeout=2)
    return sorted((v.value, v.tombstone) for v in reply.versions)


async def put(cluster, node_id: str, value: bytes, context: bytes = b"") -> dynamo_pb2.PutResponse:
    reply = await cluster.client(node_id).Put(
        dynamo_pb2.PutRequest(key=KEY, value=value, context=context), timeout=3)
    assert reply.status == dynamo_pb2.OK
    return reply


async def get(cluster, node_id: str, r: int = 0) -> dynamo_pb2.GetResponse:
    return await cluster.client(node_id).Get(dynamo_pb2.GetRequest(key=KEY, r=r), timeout=3)


async def read_until_repaired(cluster, via: str, stale: str, expected, r: int = 0) -> None:
    """Read through ``via`` until ``stale`` holds ``expected``."""
    async def repaired() -> bool:
        await get(cluster, via, r)
        return await held(cluster, stale) == expected
    await cluster.wait_until(repaired, timeout=8)


async def make_stale(cluster, node_id: str, write) -> None:
    """Kill ``node_id``, run ``write`` (it misses it), start it again."""
    await cluster.kill(node_id)
    await write()
    await cluster.restart(node_id)


# ---------- read repair ----------

async def test_stale_replica_repaired(cluster):
    first, _, third = replicas(cluster)
    await put(cluster, first, b"book")
    await make_stale(cluster, third, lambda: put(cluster, first, b"pen"))
    assert await held(cluster, third) == [(b"book", False)]        # stale after the restart
    await read_until_repaired(cluster, first, third, [(b"pen", False)])


async def test_missing_key_repaired(cluster):
    first, _, third = replicas(cluster)
    await make_stale(cluster, third, lambda: put(cluster, first, b"book"))
    assert await held(cluster, third) == []                         # never saw the key
    await read_until_repaired(cluster, first, third, [(b"book", False)])


async def test_late_reply_repaired(cluster):
    # With R=1 the coordinator's own copy is enough, so it replies before any replica answers:
    # every repair here comes from a late answer.
    first, _, third = replicas(cluster)
    await put(cluster, first, b"book")
    await make_stale(cluster, third, lambda: put(cluster, first, b"pen"))
    await read_until_repaired(cluster, first, third, [(b"pen", False)], r=1)


async def test_coordinator_repairs_itself(cluster):
    first, _, third = replicas(cluster)
    await put(cluster, first, b"book")
    await make_stale(cluster, third, lambda: put(cluster, first, b"pen"))
    # The stale replica coordinates the read itself and fixes its own copy.
    await read_until_repaired(cluster, third, third, [(b"pen", False)])


async def test_tombstone_repaired(cluster):
    first, _, third = replicas(cluster)
    written = await put(cluster, first, b"book")

    async def delete():
        reply = await cluster.client(first).Delete(
            dynamo_pb2.DeleteRequest(key=KEY, context=written.context), timeout=3)
        assert reply.status == dynamo_pb2.OK

    await make_stale(cluster, third, delete)
    assert await held(cluster, third) == [(b"book", False)]
    await read_until_repaired(cluster, first, third, [(b"", True)])
    assert (await get(cluster, first)).status == dynamo_pb2.NOT_FOUND


async def test_siblings_spread_by_repair(cluster):
    first, second, third = replicas(cluster)
    a = Version(value=b"x", clock=VectorClock(counters={first: 1}))
    b = Version(value=b"y", clock=VectorClock(counters={second: 1}))   # concurrent with a
    await cluster.replica(first).ReplicaPut(
        dynamo_pb2.ReplicaPutRequest(sender="test", key=KEY, version=version_to_proto(a)))
    await cluster.replica(second).ReplicaPut(
        dynamo_pb2.ReplicaPutRequest(sender="test", key=KEY, version=version_to_proto(b)))

    reply = await get(cluster, first, r=3)
    assert sorted(reply.values) == [b"x", b"y"]
    both = [(b"x", False), (b"y", False)]

    async def everyone_has_both() -> bool:
        return all([await held(cluster, node_id) == both for node_id in (first, second, third)])
    await cluster.wait_until(everyone_has_both)


# ---------- DEAD-aware routing ----------

async def wait_dead(cluster, watcher: str, node_id: str) -> None:
    await cluster.wait_until(
        lambda: cluster.nodes[watcher].membership.state(node_id) == dynamo_pb2.DEAD)


async def test_dead_replica_skipped(cluster, caplog):
    first, second, third = replicas(cluster)
    await cluster.kill(third)
    await wait_dead(cluster, first, third)
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="dynamo.coordinator"):
        reply = await put(cluster, first, b"book")
        assert (await get(cluster, first)).values == [b"book"]
    assert sorted(reply.written_to) == sorted([first, second])
    assert third not in caplog.text                                 # no call was even tried


async def test_forward_skips_dead_first_replica(cluster):
    first, second, third = replicas(cluster)
    outsider = next(node_id for node_id in cluster.nodes if node_id not in (first, second, third))
    await cluster.kill(first)
    await wait_dead(cluster, outsider, first)
    reply = await put(cluster, outsider, b"book")
    assert first not in reply.written_to

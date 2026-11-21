"""Real-cluster tests 3 and 4 (D10 §5): two of a key's three replicas down, with sloppy quorum
off (the write must fail) and on (stand-ins take it, and hand it over after the restart)."""

from dynamo import dynamo_pb2
from cluster.helpers import local_values, put, wait_until


async def test_3_two_replicas_down_strict_quorum_fails(strict, fresh):
    cluster = strict
    key = fresh("t3")
    a, b, c = await cluster.owners(key)
    await cluster.kill(b)
    await cluster.kill(c)
    reply = await put(cluster, a, key, b"v")
    assert reply.status == dynamo_pb2.QUORUM_FAILED


async def test_4_two_replicas_down_sloppy_quorum_hands_off(cluster, fresh):
    key = fresh("t4")
    a, b, c = await cluster.owners(key)
    await cluster.kill(b)
    await cluster.kill(c)

    async def both_not_alive() -> bool:   # stand-ins are only used for owners that are not ALIVE (D8)
        states = await cluster.states_seen_by(a)
        return states[b] != dynamo_pb2.ALIVE and states[c] != dynamo_pb2.ALIVE
    await wait_until(both_not_alive, 15, f"{a} sees {b} and {c} not ALIVE")

    reply = await put(cluster, a, key, b"held")
    assert reply.status == dynamo_pb2.OK
    stand_ins = set(reply.written_to) - {a, b, c}
    assert stand_ins, f"written_to {list(reply.written_to)} has no stand-in"

    await cluster.start(b)
    await cluster.start(c)
    for owner in (b, c):                                # no client Get: only handoff can deliver it
        await wait_until(lambda o=owner: _has(cluster, o, key), 30, f"hint delivered to {owner}")


async def _has(cluster, node: str, key: str) -> bool:
    return await local_values(cluster, node, key) == [b"held"]

"""Real-cluster tests 1, 2, 5, 6, 7 (D10 §5): routing, one node down, siblings, read repair,
failure detector. Real timers: SUSPECT after 5 s, DEAD after 10 s."""

import asyncio
import random
import time

from dynamo import dynamo_pb2
from cluster.helpers import elapsed, get, local_values, put, wait_until
from tools.cluster_control import ClusterError


async def test_1_write_node1_read_node4(cluster, fresh):
    key = fresh("t1")
    assert (await put(cluster, "node1", key, b"v")).status == dynamo_pb2.OK
    reply = await get(cluster, "node4", key)
    assert reply.status == dynamo_pb2.OK
    assert list(reply.values) == [b"v"]


async def test_2_one_node_down_everything_works(cluster, fresh):
    keys = [fresh("t2") for _ in range(20)]
    victim = (await cluster.owners(keys[0]))[0]
    contexts = {key: b"" for key in keys}
    await cluster.kill(victim)

    start, rounds = time.monotonic(), 0
    while time.monotonic() - start < 15:                 # ALIVE -> SUSPECT -> DEAD in others' views
        rounds += 1
        for key in keys:
            value = f"{key}:{rounds}".encode()
            reply = await put(cluster, random.choice(cluster.running()), key, value, contexts[key])
            assert reply.status == dynamo_pb2.OK, f"put {key} at {elapsed(start)} s"
            contexts[key] = reply.context
            got = await get(cluster, random.choice(cluster.running()), key)
            assert got.status == dynamo_pb2.OK, f"get {key} at {elapsed(start)} s"
            assert list(got.values) == [value]
    assert rounds >= 2

    await cluster.start(victim)
    await cluster.wait_all_alive()


async def test_5_same_old_context_gives_siblings(cluster, fresh):
    key = fresh("t5")
    a, b, _ = await cluster.owners(key)
    assert (await put(cluster, a, key, b"draft")).status == dynamo_pb2.OK
    old_context = (await get(cluster, a, key)).context
    assert (await put(cluster, a, key, b"x", old_context)).status == dynamo_pb2.OK
    # The next write must be coordinated by another node: through the same coordinator the second
    # write would get a higher counter and look newer than x (DEC-02).
    await cluster.kill(a)
    assert (await put(cluster, b, key, b"y", old_context)).status == dynamo_pb2.OK

    both = await get(cluster, b, key)
    assert both.status == dynamo_pb2.OK
    assert sorted(both.values) == [b"x", b"y"]

    assert (await put(cluster, b, key, b"xy", both.context)).status == dynamo_pb2.OK
    assert list((await get(cluster, b, key)).values) == [b"xy"]


async def test_6_read_repair_updates_stale_replica(cluster, fresh):
    for _ in range(3):                                  # anti-entropy may fix c first: new key
        key = fresh("t6")
        a, _, c = await cluster.owners(key)
        first = await put(cluster, a, key, b"v1")
        assert first.status == dynamo_pb2.OK
        await wait_until(lambda: _has(cluster, c, key, [b"v1"]), 5, f"{c} has v1")
        await cluster.kill(c)
        # At once: c is still ALIVE in a's view, so no stand-in; W=2 from a and b.
        assert (await put(cluster, a, key, b"v2", first.context)).status == dynamo_pb2.OK
        await cluster.start(c)
        await wait_until(lambda: _seen_alive(cluster, a, c), 15, f"{a} sees {c} ALIVE")
        if await local_values(cluster, c, key) == [b"v1"]:   # still stale: test read repair now
            break
    else:
        raise AssertionError("c was repaired before the read three times in a row")

    # Read repair only reaches replicas that answered the read. Just after c's restart, a's own
    # channel to c can still be in gRPC's reconnect backoff (up to 1 s, peers.py) even though
    # gossip already shows c ALIVE; then c is left out of that read. So read again, up to 3 times.
    for attempt in range(1, 4):
        got = await get(cluster, a, key)
        assert list(got.values) == [b"v2"]
        try:
            await wait_until(lambda: _has(cluster, c, key, [b"v2"]), 2, f"read repair of {c}")
            break
        except ClusterError:
            if attempt == 3:
                raise


async def test_7_failure_detector_times(cluster):
    victim = "node3"
    observers = [node for node in cluster.running() if node != victim]
    await cluster.kill(victim)
    start = time.monotonic()
    first_seen: dict[str, dict[str, float]] = {"SUSPECT": {}, "DEAD": {}}
    while len(first_seen["DEAD"]) < len(observers) and time.monotonic() - start < 20:
        for observer in observers:
            state = dynamo_pb2.MemberState.Name(await cluster.state_seen_by(observer, victim))
            if state in first_seen and observer not in first_seen[state]:
                first_seen[state][observer] = elapsed(start)
        await asyncio.sleep(0.2)
    print(f"\nnode3 crashed: SUSPECT seen at {first_seen['SUSPECT']}, DEAD at {first_seen['DEAD']}")
    assert set(first_seen["SUSPECT"]) == set(observers)
    assert max(first_seen["SUSPECT"].values()) <= 8
    assert set(first_seen["DEAD"]) == set(observers)
    assert max(first_seen["DEAD"].values()) <= 14

    await cluster.start(victim)
    took = await cluster.wait_all_alive(timeout=5)
    print(f"node3 restarted: ALIVE in every view after {took:.1f} s")


async def _has(cluster, node: str, key: str, values: list[bytes]) -> bool:
    return await local_values(cluster, node, key) == values


async def _seen_alive(cluster, observer: str, node: str) -> bool:
    return await cluster.state_seen_by(observer, node) == dynamo_pb2.ALIVE

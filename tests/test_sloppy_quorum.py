"""Sloppy quorum on a real 5-node in-process cluster (SPEC cluster tests 3 and 4).

Two of the three owners of a key are killed. With sloppy quorum off the write fails; with it on,
stand-ins take the write as hints and hand it to the owners when they come back (D6b).
"""

from dynamo import dynamo_pb2

KEY = "cart:42"


def owners(cluster) -> list[str]:
    any_node = next(iter(cluster.nodes.values()))
    return any_node.ring.preference_list(KEY, cluster.config.n)


async def kill_two_owners(cluster) -> tuple[str, str, str]:
    """Kill the 2nd and 3rd owner; wait until the 1st no longer sees them as ALIVE."""
    first, second, third = owners(cluster)
    await cluster.kill(second)
    await cluster.kill(third)
    view = cluster.nodes[first].membership
    await cluster.wait_until(lambda: view.state(second) != dynamo_pb2.ALIVE
                             and view.state(third) != dynamo_pb2.ALIVE)
    return first, second, third


async def put(cluster, node_id: str, value: bytes) -> dynamo_pb2.PutResponse:
    return await cluster.client(node_id).Put(dynamo_pb2.PutRequest(key=KEY, value=value), timeout=3)


async def holds(cluster, node_id: str, value: bytes) -> bool:
    reply = await cluster.client(node_id).LocalGet(dynamo_pb2.LocalGetRequest(key=KEY), timeout=2)
    return [v.value for v in reply.versions] == [value]


async def test_strict_write_fails_with_two_owners_down(make_cluster):
    cluster = await make_cluster(5, sloppy_quorum=False)
    first, _, _ = await kill_two_owners(cluster)
    assert (await put(cluster, first, b"book")).status == dynamo_pb2.QUORUM_FAILED


async def test_sloppy_write_succeeds_and_owners_get_it_back(cluster):
    first, second, third = await kill_two_owners(cluster)
    reply = await put(cluster, first, b"book")
    assert reply.status == dynamo_pb2.OK
    stand_in = reply.written_to[1]
    assert reply.written_to[0] == first and stand_in not in (first, second, third)
    assert not await holds(cluster, stand_in, b"book")             # a hint, not its own data
    await cluster.wait_until(lambda: any(cluster.nodes[node].storage.hints()
                                         for node in cluster.nodes if node != first))

    await cluster.restart(second)
    await cluster.restart(third)
    await cluster.wait_until(lambda: holds(cluster, second, b"book"), timeout=8)
    await cluster.wait_until(lambda: holds(cluster, third, b"book"), timeout=8)
    await cluster.wait_until(lambda: all(not node.storage.hints() for node in cluster.nodes.values()))


async def test_reads_stay_strict(cluster):
    first, _, _ = await kill_two_owners(cluster)
    assert (await put(cluster, first, b"book")).status == dynamo_pb2.OK
    client = cluster.client(first)
    assert (await client.Get(dynamo_pb2.GetRequest(key=KEY), timeout=3)).status \
        == dynamo_pb2.QUORUM_FAILED                                # R=2, only 1 owner up
    one = await client.Get(dynamo_pb2.GetRequest(key=KEY, r=1), timeout=3)
    assert (one.status, list(one.values)) == (dynamo_pb2.OK, [b"book"])

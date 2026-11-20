"""Unit tests for dynamo/coordinator.py: time budget, background tasks, single-replica path."""

import asyncio

import pytest

from dynamo import dynamo_pb2
from dynamo.config import ClusterConfig
from dynamo.coordinator import Coordinator, _encode_context, _parse_context, request_budget
from dynamo.ring import Ring
from dynamo.storage import Version

ONE_NODE = ClusterConfig(nodes={"node1": "localhost:50051"}, n=1, r=1, w=1)
FIVE_NODES = ClusterConfig(nodes={f"node{i}": f"localhost:5005{i}" for i in range(1, 6)})


class FakeStorage:
    """Keeps every applied version (so tests can also fake siblings)."""

    def __init__(self) -> None:
        self.data: dict[str, list[Version]] = {}

    def get(self, key: str) -> list[Version]:
        return list(self.data.get(key, []))

    async def apply(self, key: str, version: Version) -> bool:
        self.data[key] = [version]  # newest replaces, like storage before D3
        return True


class FakeContext:
    def __init__(self, remaining: float | None) -> None:
        self._remaining = remaining

    def time_remaining(self) -> float | None:
        return self._remaining


def make_coordinator(config: ClusterConfig = ONE_NODE) -> Coordinator:
    ring = Ring(list(config.nodes), config.vnodes)
    return Coordinator("node1", config, ring, FakeStorage(), peers=None, membership=None)


def clock_of(context: bytes) -> dict[str, int]:
    return _parse_context(context)


# ---------- time budget ----------

def test_budget_without_deadline():
    assert request_budget(FakeContext(None), ONE_NODE) == 1.5


def test_budget_from_shorter_deadline():
    assert request_budget(FakeContext(0.4), ONE_NODE) == 0.4


def test_budget_capped_by_request_timeout():
    assert request_budget(FakeContext(30.0), ONE_NODE) == 1.5


# ---------- context helpers ----------

def test_context_round_trip_and_merge():
    versions = [Version(b"a", {"node1": 3, "node2": 1}), Version(b"b", {"node1": 1, "node2": 4})]
    assert _parse_context(_encode_context(versions)) == {"node1": 3, "node2": 4}


def test_empty_context_is_empty_clock():
    assert _parse_context(b"") == {}


def test_bad_context_raises_value_error():
    with pytest.raises(ValueError):
        _parse_context(b"\xff\xff\xff")


# ---------- single-replica path (N = 1) ----------

async def test_put_first_clock():
    reply = await make_coordinator().put(dynamo_pb2.PutRequest(key="k", value=b"v"), 1.0)
    assert reply.status == dynamo_pb2.OK
    assert clock_of(reply.context) == {"node1": 1}
    assert list(reply.written_to) == ["node1"]


async def test_put_with_context_counter():
    coordinator = make_coordinator()
    first = await coordinator.put(dynamo_pb2.PutRequest(key="k", value=b"v1"), 1.0)
    second = await coordinator.put(
        dynamo_pb2.PutRequest(key="k", value=b"v2", context=first.context), 1.0)
    assert clock_of(second.context) == {"node1": 2}


async def test_same_context_different_counters():
    coordinator = make_coordinator()
    first = await coordinator.put(dynamo_pb2.PutRequest(key="k", value=b"v1"), 1.0)
    a, b = await asyncio.gather(
        coordinator.put(dynamo_pb2.PutRequest(key="k", value=b"a", context=first.context), 1.0),
        coordinator.put(dynamo_pb2.PutRequest(key="k", value=b"b", context=first.context), 1.0),
    )
    assert sorted([clock_of(a.context)["node1"], clock_of(b.context)["node1"]]) == [2, 3]


async def test_context_entries_of_other_nodes_are_kept():
    context = _encode_context([Version(b"", {"node7": 5})])
    reply = await make_coordinator().put(
        dynamo_pb2.PutRequest(key="k", value=b"v", context=context), 1.0)
    assert clock_of(reply.context) == {"node7": 5, "node1": 1}


async def test_put_bad_context_invalid():
    reply = await make_coordinator().put(
        dynamo_pb2.PutRequest(key="k", value=b"v", context=b"\xff\xff\xff"), 1.0)
    assert reply.status == dynamo_pb2.INVALID


async def test_get_unknown():
    reply = await make_coordinator().get(dynamo_pb2.GetRequest(key="nope"), 1.0)
    assert reply.status == dynamo_pb2.NOT_FOUND
    assert reply.context == b""
    assert list(reply.read_from) == ["node1"]


async def test_get_after_put():
    coordinator = make_coordinator()
    await coordinator.put(dynamo_pb2.PutRequest(key="k", value=b"book"), 1.0)
    reply = await coordinator.get(dynamo_pb2.GetRequest(key="k"), 1.0)
    assert reply.status == dynamo_pb2.OK
    assert list(reply.values) == [b"book"]
    assert clock_of(reply.context) == {"node1": 1}


async def test_get_after_delete():
    coordinator = make_coordinator()
    put = await coordinator.put(dynamo_pb2.PutRequest(key="k", value=b"v"), 1.0)
    delete = await coordinator.delete(dynamo_pb2.DeleteRequest(key="k", context=put.context), 1.0)
    assert delete.status == dynamo_pb2.OK
    gone = await coordinator.get(dynamo_pb2.GetRequest(key="k"), 1.0)
    assert gone.status == dynamo_pb2.NOT_FOUND
    assert clock_of(gone.context) == {"node1": 2}         # the tombstone's clock
    again = await coordinator.put(
        dynamo_pb2.PutRequest(key="k", value=b"back", context=gone.context), 1.0)
    assert clock_of(again.context) == {"node1": 3}        # newer than the tombstone
    assert list((await coordinator.get(dynamo_pb2.GetRequest(key="k"), 1.0)).values) == [b"back"]


async def test_delete_bad_context_invalid():
    reply = await make_coordinator().delete(
        dynamo_pb2.DeleteRequest(key="k", context=b"\xff\xff\xff"), 1.0)
    assert reply.status == dynamo_pb2.INVALID


async def test_get_siblings():
    coordinator = make_coordinator()
    coordinator.storage.data["k"] = [Version(b"a", {"node1": 2}), Version(b"b", {"node2": 1}),
                                     Version(b"", {"node3": 4}, tombstone=True)]
    reply = await coordinator.get(dynamo_pb2.GetRequest(key="k"), 1.0)
    assert reply.status == dynamo_pb2.OK
    assert list(reply.values) == [b"a", b"b"]
    assert clock_of(reply.context) == {"node1": 2, "node2": 1, "node3": 4}


async def test_not_replica_unimplemented():
    coordinator = make_coordinator(FIVE_NODES)  # N = 3: other replicas needed
    with pytest.raises(NotImplementedError):
        await coordinator.put(dynamo_pb2.PutRequest(key="k", value=b"v"), 1.0)
    with pytest.raises(NotImplementedError):
        await coordinator.get(dynamo_pb2.GetRequest(key="k"), 1.0)
    with pytest.raises(NotImplementedError):
        await coordinator.coordinate(dynamo_pb2.ForwardRequest(), 1.0)


# ---------- background tasks and locks ----------

async def test_drain_waits_for_quick_tasks_and_cancels_slow_ones():
    coordinator = make_coordinator()
    finished = []

    async def quick():
        await asyncio.sleep(0.01)
        finished.append("quick")

    async def slow():
        await asyncio.sleep(10)
        finished.append("slow")

    coordinator._spawn(quick())
    coordinator._spawn(slow())
    await coordinator.drain(0.2)
    assert finished == ["quick"]
    assert not coordinator._tasks


async def test_drain_with_no_tasks_returns_at_once():
    await make_coordinator().drain(5)


def test_key_lock_is_one_lock_per_key():
    coordinator = make_coordinator()
    assert coordinator._key_lock("a") is coordinator._key_lock("a")
    assert coordinator._key_lock("a") is not coordinator._key_lock("b")

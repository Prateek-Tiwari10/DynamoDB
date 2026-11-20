"""Unit tests for dynamo/coordinator.py: time budget, quorum loop, routing, single-replica path."""

import asyncio
import time

import pytest

from dynamo import dynamo_pb2
from dynamo.config import ClusterConfig
from dynamo.coordinator import Coordinator, request_budget
from dynamo.ring import Ring
from dynamo.vclock import VectorClock
from dynamo.version import Version, context_of, merge_into, parse_context

ONE_NODE = ClusterConfig(nodes={"node1": "localhost:50051"}, n=1, r=1, w=1)
FIVE_NODES = ClusterConfig(nodes={f"node{i}": f"localhost:5005{i}" for i in range(1, 6)})
WITH_NODE7 = ClusterConfig(nodes={"node1": "localhost:50051", "node7": "localhost:50057"},
                           n=1, r=1, w=1)
KNOWN = {"node1", "node2", "node3", "node7"}


def ver(value: bytes, counters: dict[str, int], tombstone: bool = False) -> Version:
    return Version(value=value, clock=VectorClock(counters=counters), tombstone=tombstone)


class FakeStorage:
    """Keeps every applied version (so tests can also fake siblings)."""

    def __init__(self) -> None:
        self.data: dict[str, list[Version]] = {}

    def get(self, key: str) -> list[Version]:
        return list(self.data.get(key, []))

    async def apply(self, key: str, version: Version) -> bool:
        self.data[key], changed = merge_into(self.data.get(key, []), version)   # same rule as Storage
        return changed


class FakeMembership:
    """Everyone ALIVE unless a test says otherwise."""

    def __init__(self, states: dict[str, int] | None = None) -> None:
        self.states = states or {}

    def state(self, node_id: str) -> int:
        return self.states.get(node_id, dynamo_pb2.ALIVE)


class FakeContext:
    def __init__(self, remaining: float | None) -> None:
        self._remaining = remaining

    def time_remaining(self) -> float | None:
        return self._remaining


def make_coordinator(config: ClusterConfig = ONE_NODE) -> Coordinator:
    ring = Ring(list(config.nodes), config.vnodes)
    return Coordinator("node1", config, ring, FakeStorage(), peers=None, membership=FakeMembership())


def clock_of(context: bytes) -> dict[str, int]:
    return parse_context(context, KNOWN).counters


def key_owned_by(coordinator: Coordinator, node_id: str) -> str:
    """A key whose only replica (N=1) is ``node_id``."""
    return next(key for key in (f"k{i}" for i in range(1000))
                if coordinator.ring.preference_list(key, 1) == [node_id])


# ---------- time budget ----------

def test_budget_without_deadline():
    assert request_budget(FakeContext(None), ONE_NODE) == 1.5


def test_budget_from_shorter_deadline():
    assert request_budget(FakeContext(0.4), ONE_NODE) == 0.4


def test_budget_capped_by_request_timeout():
    assert request_budget(FakeContext(30.0), ONE_NODE) == 1.5


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
    coordinator = make_coordinator(WITH_NODE7)
    context = context_of([ver(b"", {"node7": 5})])
    reply = await coordinator.put(
        dynamo_pb2.PutRequest(key=key_owned_by(coordinator, "node1"), value=b"v", context=context),
        1.0)
    assert clock_of(reply.context) == {"node7": 5, "node1": 1}


async def test_put_context_with_unknown_node_invalid():
    context = context_of([ver(b"", {"node7": 5})])          # node7 is not in ONE_NODE
    reply = await make_coordinator().put(
        dynamo_pb2.PutRequest(key="k", value=b"v", context=context), 1.0)
    assert reply.status == dynamo_pb2.INVALID


async def test_put_stamps_updated_ms():
    coordinator = make_coordinator()
    await coordinator.put(dynamo_pb2.PutRequest(key="k", value=b"v"), 1.0)
    assert coordinator.storage.data["k"][0].clock.updated_ms["node1"] > 1_700_000_000_000


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
    coordinator.storage.data["k"] = [ver(b"a", {"node1": 2}), ver(b"b", {"node2": 1}),
                                     ver(b"", {"node3": 4}, tombstone=True)]
    reply = await coordinator.get(dynamo_pb2.GetRequest(key="k"), 1.0)
    assert reply.status == dynamo_pb2.OK
    assert list(reply.values) == [b"a", b"b"]
    assert clock_of(reply.context) == {"node1": 2, "node2": 1, "node3": 4}


async def test_get_same_value_siblings_sent_once():
    coordinator = make_coordinator()
    coordinator.storage.data["k"] = [ver(b"a", {"node1": 1}), ver(b"b", {"node2": 1}),
                                     ver(b"a", {"node3": 1})]
    reply = await coordinator.get(dynamo_pb2.GetRequest(key="k"), 1.0)
    assert list(reply.values) == [b"a", b"b"]
    assert clock_of(reply.context) == {"node1": 1, "node2": 1, "node3": 1}


async def test_coordinate_runs_the_forwarded_request_here():
    coordinator = make_coordinator()
    reply = await coordinator.coordinate(
        dynamo_pb2.ForwardRequest(sender="node2", put=dynamo_pb2.PutRequest(key="k", value=b"v")), 1.0)
    assert reply.put.status == dynamo_pb2.OK
    assert list(reply.put.written_to) == ["node1"]


async def test_coordinate_empty_forward_is_an_error():
    with pytest.raises(ValueError):
        await make_coordinator().coordinate(dynamo_pb2.ForwardRequest(), 1.0)


# ---------- routing and fast fail ----------

def test_route_order_alive_then_suspect_then_dead():
    coordinator = make_coordinator(FIVE_NODES)
    coordinator.membership = FakeMembership({"node2": dynamo_pb2.DEAD, "node3": dynamo_pb2.SUSPECT})
    assert coordinator._route_order(["node2", "node3", "node4"]) == ["node4", "node3", "node2"]
    assert coordinator._route_order(["node5", "node4"]) == ["node5", "node4"]   # list order kept


async def test_fast_fail_when_not_enough_alive():
    coordinator = make_coordinator(FIVE_NODES)
    key = next(k for k in (f"k{i}" for i in range(1000))
               if coordinator._replicas(k)[0] == "node1")
    others = coordinator._replicas(key)[1:]
    coordinator.membership = FakeMembership({node: dynamo_pb2.DEAD for node in others})
    started = time.monotonic()
    put = await coordinator.put(dynamo_pb2.PutRequest(key=key, value=b"v"), 1.0)
    get = await coordinator.get(dynamo_pb2.GetRequest(key=key), 1.0)
    assert (put.status, get.status) == (dynamo_pb2.QUORUM_FAILED, dynamo_pb2.QUORUM_FAILED)
    assert time.monotonic() - started < 0.1      # no waiting for timeouts
    assert coordinator.storage.data == {}        # nothing written


# ---------- the quorum wait loop ----------

async def answer(after: float, result):
    await asyncio.sleep(after)
    return result


def tasks_for(*answers) -> dict:
    return {asyncio.create_task(answer(after, result)): f"node{i + 2}"
            for i, (after, result) in enumerate(answers)}


async def test_wait_stops_at_the_quorum_and_keeps_the_rest_running():
    coordinator = make_coordinator()
    tasks = tasks_for((0.01, True), (0.3, True))
    got = await coordinator._wait_for(tasks, have=1, needed=2, deadline=coordinator._deadline(1.0))
    assert got == [("node2", True)]
    assert len(coordinator._tasks) == 1          # the slow one runs on in the background
    await coordinator.drain(1.0)
    assert all(task.done() and task.result() is True for task in tasks)


async def test_wait_fails_early_when_quorum_is_out_of_reach():
    coordinator = make_coordinator()
    tasks = tasks_for((0.01, None), (5.0, True))   # one failure, the other is very slow
    started = time.monotonic()
    got = await coordinator._wait_for(tasks, have=1, needed=3, deadline=coordinator._deadline(10))
    assert got == []
    assert time.monotonic() - started < 0.5       # did not wait for the slow one
    await coordinator.drain(0)


async def test_wait_respects_the_deadline():
    coordinator = make_coordinator()
    tasks = tasks_for((5.0, True), (5.0, True))
    started = time.monotonic()
    got = await coordinator._wait_for(tasks, have=1, needed=2, deadline=coordinator._deadline(0.2))
    assert got == []
    assert 0.15 < time.monotonic() - started < 0.6
    await coordinator.drain(0)


async def test_wait_with_quorum_already_met_returns_at_once():
    coordinator = make_coordinator()
    tasks = tasks_for((5.0, True))
    assert await coordinator._wait_for(tasks, have=1, needed=1,
                                       deadline=coordinator._deadline(1.0)) == []
    await coordinator.drain(0)


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

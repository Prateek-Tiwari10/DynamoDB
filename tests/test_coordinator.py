"""Unit tests for dynamo/coordinator.py: time budget, quorum loop, routing, single-replica path,
read repair."""

import asyncio
import logging
import time

import grpc
import pytest

from dynamo import dynamo_pb2
from dynamo.config import ClusterConfig
from dynamo.coordinator import Coordinator, repair_plan, request_budget
from dynamo.ring import Ring
from dynamo.vclock import VectorClock
from dynamo.version import Version, context_of, merge_into, parse_context
from dynamo.wire import version_from_proto, version_to_proto

ONE_NODE = ClusterConfig(nodes={"node1": "localhost:50051"}, n=1, r=1, w=1)
FIVE_NODES = ClusterConfig(nodes={f"node{i}": f"localhost:5005{i}" for i in range(1, 6)})
FIVE_STRICT = ClusterConfig(nodes=FIVE_NODES.nodes, sloppy_quorum=False)   # D5 / D7 routing
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
    coordinator = make_coordinator(FIVE_STRICT)
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


def cancel(tasks) -> None:
    for task in tasks:
        task.cancel()


def tasks_for(*answers) -> dict:
    return {asyncio.create_task(answer(after, result)): f"node{i + 2}"
            for i, (after, result) in enumerate(answers)}


async def test_wait_stops_at_the_quorum_and_keeps_the_rest_running():
    coordinator = make_coordinator()
    tasks = tasks_for((0.01, True), (0.3, True))
    got, late = await coordinator._wait_for(tasks, have=1, needed=2,
                                            deadline=coordinator._deadline(1.0))
    assert got == [("node2", True)]
    assert list(late.values()) == ["node3"]       # the slow one is handed back, still running
    await asyncio.wait(late)
    assert all(task.done() and task.result() is True for task in tasks)


async def test_wait_fails_early_when_quorum_is_out_of_reach():
    coordinator = make_coordinator()
    tasks = tasks_for((0.01, None), (5.0, True))   # one failure, the other is very slow
    started = time.monotonic()
    got, late = await coordinator._wait_for(tasks, have=1, needed=3,
                                            deadline=coordinator._deadline(10))
    assert got == []
    assert time.monotonic() - started < 0.5       # did not wait for the slow one
    cancel(late)


async def test_wait_respects_the_deadline():
    coordinator = make_coordinator()
    tasks = tasks_for((5.0, True), (5.0, True))
    started = time.monotonic()
    got, late = await coordinator._wait_for(tasks, have=1, needed=2,
                                            deadline=coordinator._deadline(0.2))
    assert got == []
    assert 0.15 < time.monotonic() - started < 0.6
    cancel(late)


async def test_wait_with_quorum_already_met_returns_at_once():
    coordinator = make_coordinator()
    tasks = tasks_for((5.0, True))
    got, late = await coordinator._wait_for(tasks, have=1, needed=1,
                                            deadline=coordinator._deadline(1.0))
    assert got == [] and len(late) == 1
    cancel(late)


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


# ---------- read repair (D7) ----------

class FakePeers:
    """Records every ReplicaPut and ReplicaGet and keeps what was put in ``stored`` (which also
    answers gets); ``failing`` nodes answer UNAVAILABLE."""

    def __init__(self, stored: dict[str, list[Version]] | None = None,
                 failing: set[str] = frozenset()) -> None:
        self.stored = stored or {}
        self.failing = failing
        self.puts: list[tuple[str, Version]] = []
        self.gets: list[str] = []

    def stub(self, node_id: str) -> "FakeStub":
        return FakeStub(self, node_id)


class FakeStub:
    def __init__(self, peers: FakePeers, node_id: str) -> None:
        self.peers, self.node_id = peers, node_id

    def _check(self) -> None:
        if self.node_id in self.peers.failing:
            raise grpc.aio.AioRpcError(grpc.StatusCode.UNAVAILABLE, grpc.aio.Metadata(),
                                       grpc.aio.Metadata())

    async def ReplicaPut(self, request, timeout, metadata):
        self._check()
        version = version_from_proto(request.version)
        self.peers.puts.append((self.node_id, version))
        stored = self.peers.stored.get(self.node_id, [])
        self.peers.stored[self.node_id], _ = merge_into(stored, version)   # like a real replica
        return dynamo_pb2.ReplicaPutResponse(ok=True, node_id=self.node_id)

    async def Forward(self, request, timeout, metadata):
        self._check()
        raise AssertionError("tests only forward to failing nodes")

    async def ReplicaGet(self, request, timeout, metadata):
        self._check()
        self.peers.gets.append(self.node_id)
        versions = [version_to_proto(v) for v in self.peers.stored.get(self.node_id, [])]
        return dynamo_pb2.ReplicaGetResponse(versions=versions)


OLD = ver(b"old", {"node1": 1})
NEW = ver(b"new", {"node1": 2})
SIB = ver(b"sib", {"node1": 1, "node2": 1})


def test_repair_plan_sends_only_what_is_missing():
    plan = repair_plan([NEW], {"node1": [NEW], "node2": [OLD], "node3": []})
    assert plan == {"node2": [NEW], "node3": [NEW]}       # node1 has it: left out


def test_repair_plan_siblings_go_to_both_sides():
    plan = repair_plan([NEW, SIB], {"node1": [NEW], "node2": [SIB]})
    assert plan == {"node1": [SIB], "node2": [NEW]}


def test_repair_plan_tombstones_are_repaired_too():
    gone = ver(b"", {"node1": 3}, tombstone=True)
    assert repair_plan([gone], {"node2": [NEW]}) == {"node2": [gone]}


def test_repair_plan_nothing_to_do():
    assert repair_plan([NEW], {"node1": [NEW], "node2": [NEW]}) == {}


def repair_coordinator(peers: FakePeers) -> Coordinator:
    coordinator = make_coordinator(FIVE_NODES)
    coordinator.peers = peers
    return coordinator


async def test_repair_sends_missing_versions_and_repairs_itself():
    peers = FakePeers()
    coordinator = repair_coordinator(peers)
    await coordinator._repair("k", [OLD], [("node2", [NEW]), ("node3", [])], {})
    assert sorted(peers.puts, key=lambda put: put[0]) == [("node3", NEW)]
    assert coordinator.storage.get("k") == [NEW]          # the coordinator's own copy


async def test_no_repair_when_in_sync():
    peers = FakePeers()
    coordinator = repair_coordinator(peers)
    await coordinator._repair("k", [NEW], [("node2", [NEW]), ("node3", [NEW])], {})
    assert peers.puts == []


async def test_repair_waits_for_late_answers():
    peers = FakePeers()
    coordinator = repair_coordinator(peers)
    late = {asyncio.create_task(answer(0.05, [OLD])): "node3",
            asyncio.create_task(answer(0.01, None)): "node4"}   # node4 never answered
    await coordinator._repair("k", [NEW], [("node2", [NEW])], late)
    assert peers.puts == [("node3", NEW)]                 # the late one, not node4


async def test_repair_not_duplicated():
    peers = FakePeers()
    coordinator = repair_coordinator(peers)
    coordinator._repairing.add(("k", "node2"))            # a repair of (k, node2) is running
    await coordinator._repair("k", [NEW], [("node2", [OLD]), ("node3", [OLD])], {})
    assert peers.puts == [("node3", NEW)]
    assert coordinator._repairing == {("k", "node2")}     # node3's entry removed when done


async def test_repair_failure_logged_not_retried(caplog):
    peers = FakePeers(failing={"node2"})
    coordinator = repair_coordinator(peers)
    with caplog.at_level(logging.WARNING, logger="dynamo.coordinator"):
        await coordinator._repair("k", [NEW], [("node2", [OLD]), ("node3", [OLD])], {})
    assert peers.puts == [("node3", NEW)]
    assert "ReplicaPut k to node2 failed: UNAVAILABLE" in caplog.text
    assert coordinator._repairing == set()


def test_reachable_skips_only_dead():
    coordinator = make_coordinator(FIVE_NODES)
    coordinator.membership = FakeMembership({"node2": dynamo_pb2.DEAD, "node3": dynamo_pb2.SUSPECT})
    assert coordinator._reachable(["node2", "node3", "node4"]) == ["node3", "node4"]


async def test_get_and_put_skip_a_dead_replica():
    coordinator = make_coordinator(FIVE_STRICT)
    key = next(k for k in (f"k{i}" for i in range(1000))
               if coordinator._replicas(k)[0] == "node1")
    dead, alive = coordinator._replicas(key)[1:]
    coordinator.membership = FakeMembership({dead: dynamo_pb2.DEAD})
    coordinator.peers = FakePeers()
    put = await coordinator.put(dynamo_pb2.PutRequest(key=key, value=b"v"), 1.0)
    get = await coordinator.get(dynamo_pb2.GetRequest(key=key), 1.0)
    await coordinator.drain(1.0)
    assert (put.status, get.status) == (dynamo_pb2.OK, dynamo_pb2.OK)
    assert [node for node, _ in coordinator.peers.puts] == [alive]
    assert coordinator.peers.gets == [alive]


async def test_fast_fail_counts_only_alive():
    # Meet's D7 Q5 decision: a SUSPECT replica does not count towards W / R.
    coordinator = make_coordinator(FIVE_STRICT)
    key = next(k for k in (f"k{i}" for i in range(1000))
               if coordinator._replicas(k)[0] == "node1")
    second, third = coordinator._replicas(key)[1:]
    coordinator.membership = FakeMembership({second: dynamo_pb2.SUSPECT, third: dynamo_pb2.DEAD})
    coordinator.peers = FakePeers()
    put = await coordinator.put(dynamo_pb2.PutRequest(key=key, value=b"v"), 1.0)
    assert put.status == dynamo_pb2.QUORUM_FAILED
    assert coordinator.peers.puts == []


# ---------- sloppy quorum (D8) ----------

def owners_and_outsiders(coordinator: Coordinator) -> tuple[str, list[str], list[str]]:
    """A key whose first owner is node1, its owners, and the other nodes in ring order."""
    key = next(k for k in (f"k{i}" for i in range(1000))
               if coordinator._replicas(k)[0] == "node1")
    owners = coordinator._replicas(key)
    outsiders = [node for node in coordinator.ring.walk(key) if node not in owners]
    return key, owners, outsiders


def sloppy_coordinator(states: dict[str, int]) -> Coordinator:
    coordinator = make_coordinator(FIVE_NODES)
    assert coordinator.config.sloppy_quorum
    coordinator.membership = FakeMembership(states)
    coordinator.peers = FakePeers()
    return coordinator


def test_stand_ins_follow_the_ring_and_skip_non_alive():
    coordinator = sloppy_coordinator({})
    key, owners, (first_out, second_out) = owners_and_outsiders(coordinator)
    assert coordinator._stand_ins(key, owners, 1) == [first_out]
    assert coordinator._stand_ins(key, owners, 2) == [first_out, second_out]
    coordinator.membership.states[first_out] = dynamo_pb2.SUSPECT
    assert coordinator._stand_ins(key, owners, 2) == [second_out]     # only as many as are ALIVE


def test_one_dead_owner_one_stand_in():
    coordinator = sloppy_coordinator({})
    key, (_, second, third), (first_out, _) = owners_and_outsiders(coordinator)
    coordinator.membership.states[third] = dynamo_pb2.DEAD
    assert coordinator._targets(key, coordinator._replicas(key)) == [(second, ""), (first_out, third)]


def test_two_dead_owners_two_stand_ins_in_preference_order():
    coordinator = sloppy_coordinator({})
    key, (_, second, third), (first_out, second_out) = owners_and_outsiders(coordinator)
    coordinator.membership.states.update({second: dynamo_pb2.DEAD, third: dynamo_pb2.DEAD})
    assert coordinator._targets(key, coordinator._replicas(key)) == [(first_out, second),
                                                                     (second_out, third)]


def test_suspect_owner_gets_a_stand_in():
    coordinator = sloppy_coordinator({})
    key, (_, second, third), (first_out, _) = owners_and_outsiders(coordinator)
    coordinator.membership.states[second] = dynamo_pb2.SUSPECT
    assert coordinator._targets(key, coordinator._replicas(key)) == [(third, ""), (first_out, second)]


def test_flag_off_keeps_d7_routing():
    coordinator = make_coordinator(FIVE_STRICT)
    key, (_, second, third), _ = owners_and_outsiders(coordinator)
    coordinator.membership = FakeMembership({second: dynamo_pb2.SUSPECT, third: dynamo_pb2.DEAD})
    assert coordinator._targets(key, coordinator._replicas(key)) == [(second, "")]


async def test_sloppy_write_sends_hints_and_counts_them():
    coordinator = sloppy_coordinator({})
    key, (_, second, third), (first_out, second_out) = owners_and_outsiders(coordinator)
    coordinator.membership.states.update({second: dynamo_pb2.DEAD, third: dynamo_pb2.SUSPECT})
    sent = []
    real_put = coordinator._replica_put

    async def spy(node_id, key, version, deadline, hint_for=""):
        sent.append((node_id, hint_for))
        return await real_put(node_id, key, version, deadline, hint_for)

    coordinator._replica_put = spy
    reply = await coordinator.put(dynamo_pb2.PutRequest(key=key, value=b"v"), 1.0)
    await coordinator.drain(1.0)
    assert reply.status == dynamo_pb2.OK
    assert reply.written_to[0] == "node1" and set(reply.written_to[1:]) <= {first_out, second_out}
    assert sorted(sent) == sorted([(first_out, second), (second_out, third)])


async def test_fast_fail_counts_stand_ins():
    coordinator = sloppy_coordinator({})
    key, (_, second, third), outsiders = owners_and_outsiders(coordinator)
    coordinator.membership.states.update({second: dynamo_pb2.DEAD, third: dynamo_pb2.DEAD})
    coordinator.membership.states.update({node: dynamo_pb2.DEAD for node in outsiders})
    put = await coordinator.put(dynamo_pb2.PutRequest(key=key, value=b"v"), 1.0)
    assert put.status == dynamo_pb2.QUORUM_FAILED              # 1 ALIVE owner, no stand-in
    assert coordinator.peers.puts == [] and coordinator.storage.data == {}


async def test_all_owners_dead_fails():
    # node1 is not an owner here; all 3 owners are down. Q6: no stand-in coordinates, so the
    # forward attempts fail and the client gets QUORUM_FAILED, even with sloppy quorum on.
    coordinator = make_coordinator(FIVE_NODES)
    key = next(k for k in (f"k{i}" for i in range(1000)) if "node1" not in coordinator._replicas(k))
    owners = coordinator._replicas(key)
    coordinator.membership = FakeMembership({node: dynamo_pb2.DEAD for node in owners})
    coordinator.peers = FakePeers(failing=set(owners))
    put = await coordinator.put(dynamo_pb2.PutRequest(key=key, value=b"v"), 1.0)
    assert put.status == dynamo_pb2.QUORUM_FAILED
    assert coordinator.peers.puts == [] and coordinator.storage.data == {}


async def test_reads_stay_strict():
    coordinator = sloppy_coordinator({})
    key, (_, second, third), _ = owners_and_outsiders(coordinator)
    coordinator.membership.states.update({second: dynamo_pb2.DEAD, third: dynamo_pb2.DEAD})
    get = await coordinator.get(dynamo_pb2.GetRequest(key=key), 1.0)
    assert get.status == dynamo_pb2.QUORUM_FAILED
    assert coordinator.peers.gets == []                        # no stand-in asked

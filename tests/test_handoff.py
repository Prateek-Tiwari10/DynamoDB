"""Unit tests for dynamo/handoff.py: real storage, fake membership and peers."""

import asyncio
import logging

import grpc
import pytest

from dynamo import dynamo_pb2
from dynamo.config import ClusterConfig
from dynamo.handoff import Handoff
from dynamo.storage import Storage
from dynamo.vclock import VectorClock
from dynamo.version import Version, merge_into
from dynamo.wire import version_from_proto

CONFIG = ClusterConfig(nodes={f"node{i}": f"localhost:5005{i}" for i in range(1, 6)},
                       handoff_retry_ms=5000)


def ver(value: bytes, counters: dict[str, int]) -> Version:
    return Version(value=value, clock=VectorClock(counters=counters,
                                                  updated_ms={node: 0 for node in counters}))


class FakeMembership:
    def __init__(self, states: dict[str, int] | None = None) -> None:
        self.states = states or {}
        self.callbacks = []

    def state(self, node_id: str) -> int:
        return self.states.get(node_id, dynamo_pb2.ALIVE)

    def subscribe(self, callback) -> None:
        self.callbacks.append(callback)


class FakePeers:
    """Owners store what they receive (like real replicas); ``failing`` owners are unreachable;
    ``fail_after`` lets an owner take that many puts and then fail."""

    def __init__(self, failing: set[str] = frozenset(), fail_after: dict[str, int] | None = None):
        self.failing = set(failing)
        self.fail_after = fail_after or {}
        self.received: dict[str, dict[str, list[Version]]] = {}
        self.calls: list[tuple[str, str]] = []

    def stub(self, node_id: str):
        peers = self

        class Stub:
            async def ReplicaPut(self, request, timeout):
                peers.calls.append((node_id, request.key))
                taken = sum(1 for node, _ in peers.calls if node == node_id)
                if node_id in peers.failing or taken > peers.fail_after.get(node_id, 10**9):
                    raise grpc.aio.AioRpcError(grpc.StatusCode.UNAVAILABLE, grpc.aio.Metadata(),
                                               grpc.aio.Metadata())
                assert request.hint_for == ""                    # delivered as a normal write
                held = peers.received.setdefault(node_id, {})
                held[request.key], _ = merge_into(held.get(request.key, []),
                                                  version_from_proto(request.version))
                return dynamo_pb2.ReplicaPutResponse(ok=True, node_id=node_id)
        return Stub()


@pytest.fixture
def storage(tmp_path):
    storage = Storage(str(tmp_path), CONFIG)
    storage.load()
    return storage


def make_handoff(storage, membership=None, peers=None) -> Handoff:
    return Handoff("node1", CONFIG, storage, membership or FakeMembership(), peers or FakePeers())


A = ver(b"a", {"node1": 1})
B = ver(b"b", {"node1": 2})


async def test_deliver_once_sends_and_drops(storage):
    await storage.put_hint("node3", "a", A)
    await storage.put_hint("node3", "b", B)
    handoff = make_handoff(storage)
    assert await handoff.deliver_once() == 2
    assert handoff.peers.received == {"node3": {"a": [A], "b": [B]}}
    assert storage.hints() == {}


async def test_dead_and_suspect_owners_are_skipped(storage):
    await storage.put_hint("node3", "a", A)
    await storage.put_hint("node4", "a", A)
    membership = FakeMembership({"node3": dynamo_pb2.DEAD, "node4": dynamo_pb2.SUSPECT})
    handoff = make_handoff(storage, membership)
    assert await handoff.deliver_once() == 0
    assert handoff.peers.calls == []
    assert set(storage.hints()) == {"node3", "node4"}            # kept for later


async def test_failure_stops_only_that_owner(storage, caplog):
    await storage.put_hint("node3", "a", A)
    await storage.put_hint("node3", "b", B)
    await storage.put_hint("node5", "a", A)
    peers = FakePeers(fail_after={"node3": 1})                   # node3 takes one, then goes away
    handoff = make_handoff(storage, peers=peers)
    with caplog.at_level(logging.WARNING, logger="dynamo.handoff"):
        assert await handoff.deliver_once() == 2
    assert [call for call in peers.calls if call[0] == "node3"] == [("node3", "a"), ("node3", "b")]
    assert storage.hints() == {"node3": [("b", B)]}               # the failed one waits
    assert "hint b for node3 not delivered: UNAVAILABLE" in caplog.text


async def test_duplicate_delivery_is_harmless(storage):
    await storage.put_hint("node3", "a", A)
    handoff = make_handoff(storage)
    await handoff.deliver_once()
    await storage.put_hint("node3", "a", A)                      # as if the drop had been lost
    await handoff.deliver_once()
    assert handoff.peers.received == {"node3": {"a": [A]}}       # the owner still has one version
    assert storage.hints() == {}


async def test_alive_owner_wakes_the_loop(storage):
    await storage.put_hint("node3", "a", A)
    membership = FakeMembership({"node3": dynamo_pb2.DEAD})
    handoff = make_handoff(storage, membership)
    await handoff.start()
    try:
        await asyncio.sleep(0.05)
        assert storage.hints() != {}                             # owner DEAD: nothing sent
        membership.states["node3"] = dynamo_pb2.ALIVE
        for callback in membership.callbacks:
            callback("node3", dynamo_pb2.DEAD, dynamo_pb2.ALIVE)
        for _ in range(50):                                      # far less than the 5 s period
            if storage.hints() == {}:
                break
            await asyncio.sleep(0.01)
        assert storage.hints() == {}
    finally:
        await handoff.stop()


async def test_stop_is_safe_twice_and_before_start(storage):
    handoff = make_handoff(storage)
    await handoff.stop()
    await handoff.start()
    await handoff.stop()
    await handoff.stop()

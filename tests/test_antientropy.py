"""Tests for dynamo/antientropy.py (D6c).

Unit part: two AntiEntropy objects with real storage and a real ring, connected by a fake stub
that calls the other side's handlers directly (no network). Cluster part: a real 5-node cluster
with anti-entropy every 200 ms; differences are put straight into one replica's storage, and no
client ever reads, so nothing but anti-entropy can repair them.
"""

import asyncio
import itertools
import logging
import random

import grpc
import pytest

from dynamo import dynamo_pb2, merkle
from dynamo.antientropy import MAX_BUCKETS_PER_ROUND, AntiEntropy
from dynamo.config import ClusterConfig
from dynamo.ring import Ring
from dynamo.storage import Storage
from dynamo.vclock import VectorClock
from dynamo.version import Version, fingerprint
from dynamo.wire import version_to_proto

NODES = [f"node{i}" for i in range(1, 6)]
CONFIG = ClusterConfig(nodes={node: f"localhost:5005{i}" for i, node in enumerate(NODES, 1)})
RING = Ring(NODES, CONFIG.vnodes)


def ver(value: bytes, counters: dict[str, int], ms: int = 100, tombstone: bool = False) -> Version:
    return Version(value=value, tombstone=tombstone,
                   clock=VectorClock(counters=dict(counters),
                                     updated_ms={node: ms for node in counters}))


def keys_owned_by(*nodes: str, count: int, prefix: str = "k") -> list[str]:
    """The first ``count`` keys whose replicas include all of ``nodes``."""
    found = []
    for i in itertools.count():
        key = f"{prefix}{i}"
        if all(node in RING.preference_list(key, CONFIG.n) for node in nodes):
            found.append(key)
            if len(found) == count:
                return found


def key_not_owned_by(node: str, also_owned_by: str) -> str:
    """A key that ``also_owned_by`` owns but ``node`` does not."""
    for i in itertools.count():
        owners = RING.preference_list(f"x{i}", CONFIG.n)
        if also_owned_by in owners and node not in owners:
            return f"x{i}"


# ------------------------------------------------------------------ unit: fakes

class FakeMembership:
    def __init__(self, alive: set[str]) -> None:
        self.alive = alive

    def is_alive(self, node_id: str) -> bool:
        return node_id in self.alive


class LinkedPeers:
    """Stub for a peer that calls the peer's AntiEntropy handlers directly; counts the calls."""

    def __init__(self) -> None:
        self.others: dict[str, AntiEntropy] = {}
        self.compares = 0
        self.exchanges: list[list[int]] = []
        self.down = False

    def stub(self, node_id: str):
        peers, other = self, self.others[node_id]

        class Stub:
            async def CompareTree(self, request, timeout):
                if peers.down:
                    raise grpc.aio.AioRpcError(grpc.StatusCode.UNAVAILABLE, grpc.aio.Metadata(),
                                               grpc.aio.Metadata())
                peers.compares += 1
                return other.on_compare(request)

            async def ExchangeBuckets(self, request, timeout):
                peers.exchanges.append(list(request.buckets))
                return await other.on_exchange(request)
        return Stub()


@pytest.fixture
def pair(tmp_path):
    """node1 and node2, each with its own storage; only each other is ALIVE, so node1's
    round-robin always picks node2."""
    sides = {}
    for me, other in (("node1", "node2"), ("node2", "node1")):
        storage = Storage(str(tmp_path / me), CONFIG)
        storage.load()
        sides[me] = AntiEntropy(me, CONFIG, storage, RING, FakeMembership({other}), LinkedPeers())
    sides["node1"].peers.others["node2"] = sides["node2"]
    sides["node2"].peers.others["node1"] = sides["node1"]
    return sides["node1"], sides["node2"]


def same_data(a: AntiEntropy, b: AntiEntropy, keys) -> bool:
    return all(fingerprint(a.storage.get(k)) == fingerprint(b.storage.get(k)) for k in keys)


# ------------------------------------------------------------------ unit: partner choice

def test_next_partner_round_robin():
    ae = AntiEntropy("node3", CONFIG, None, RING, FakeMembership(set(NODES)), None)
    assert [ae._next_partner() for _ in range(5)] == ["node1", "node2", "node4", "node5", "node1"]


def test_next_partner_skips_not_alive():
    ae = AntiEntropy("node1", CONFIG, None, RING, FakeMembership({"node2", "node5"}), None)
    assert [ae._next_partner() for _ in range(3)] == ["node2", "node5", "node2"]


def test_next_partner_none_when_nobody_alive():
    ae = AntiEntropy("node1", CONFIG, None, RING, FakeMembership(set()), None)
    assert ae._next_partner() is None


# ------------------------------------------------------------------ unit: one round

async def test_identical_only_root(pair):
    a, b = pair
    for key in keys_owned_by("node1", "node2", count=20):
        await a.storage.apply(key, ver(b"v", {"node1": 1}))
        await b.storage.apply(key, ver(b"v", {"node1": 1}))
    assert await a.sync_once() is False
    assert (a.peers.compares, a.peers.exchanges) == (1, [])


async def test_missing_key_both_directions(pair):
    a, b = pair
    k1, k2 = keys_owned_by("node1", "node2", count=2)
    await a.storage.apply(k1, ver(b"from-a", {"node1": 1}))
    await b.storage.apply(k2, ver(b"from-b", {"node2": 1}))
    assert await a.sync_once() is True
    assert same_data(a, b, [k1, k2])
    assert b.storage.get(k1)[0].value == b"from-a" and a.storage.get(k2)[0].value == b"from-b"
    assert await a.sync_once() is False                  # next round: roots equal


async def test_siblings_converge(pair):
    a, b = pair
    (key,) = keys_owned_by("node1", "node2", count=1)
    await a.storage.apply(key, ver(b"x", {"node1": 1}))
    await b.storage.apply(key, ver(b"y", {"node2": 1}))
    await a.sync_once()
    assert sorted(v.value for v in a.storage.get(key)) == [b"x", b"y"]
    assert same_data(a, b, [key])


async def test_delete_spreads(pair):
    a, b = pair
    (key,) = keys_owned_by("node1", "node2", count=1)
    await a.storage.apply(key, ver(b"v", {"node1": 1}))
    await b.storage.apply(key, ver(b"v", {"node1": 1}))
    await a.storage.apply(key, ver(b"", {"node1": 2}, tombstone=True))
    await a.sync_once()
    assert [v.tombstone for v in b.storage.get(key)] == [True]   # the old value is dropped


async def test_updated_ms_difference_converges(pair):
    """Same counters, different updated_ms: exchanged once, both keep the later time, then equal."""
    a, b = pair
    (key,) = keys_owned_by("node1", "node2", count=1)
    await a.storage.apply(key, ver(b"v", {"node1": 1}, ms=100))
    await b.storage.apply(key, ver(b"v", {"node1": 1}, ms=200))
    assert await a.sync_once() is True
    assert same_data(a, b, [key])
    assert await a.sync_once() is False


async def test_non_owned_key_ignored(pair):
    a, b = pair
    await a.storage.apply(key_not_owned_by("node2", also_owned_by="node1"), ver(b"v", {"node1": 1}))
    assert await a.sync_once() is False                  # not in the node1-node2 tree


async def test_hints_not_in_tree(pair):
    a, b = pair
    (key,) = keys_owned_by("node1", "node2", count=1)
    await a.storage.put_hint("node3", key, ver(b"held", {"node1": 1}))
    assert await a.sync_once() is False
    assert b.storage.get(key) == []


async def test_large_drift_converges_in_steps(pair):
    a, b = pair
    keys = keys_owned_by("node1", "node2", count=1000)
    for key in keys:
        await a.storage.apply(key, ver(b"v", {"node1": 1}))
    rounds = 0
    while await a.sync_once():
        rounds += 1
        assert rounds < 20
    assert all(len(buckets) <= MAX_BUCKETS_PER_ROUND for buckets in a.peers.exchanges)
    assert rounds > 1
    assert same_data(a, b, keys)


async def test_partner_down_round_ends(pair):
    a, b = pair
    a.peers.down = True
    assert await a.sync_once() is False


async def test_loop_survives_errors(tmp_path, monkeypatch):
    config = ClusterConfig(nodes=CONFIG.nodes, anti_entropy_ms=10)
    ae = AntiEntropy("node1", config, None, RING, FakeMembership(set()), None)
    calls = []

    async def broken():
        calls.append(1)
        raise RuntimeError("boom")
    monkeypatch.setattr(ae, "sync_once", broken)
    await ae.start()
    await asyncio.sleep(0.1)
    assert len(calls) >= 3 and not ae._task.done()
    await ae.stop()
    await ae.stop()                                       # safe twice


async def test_stop_before_start():
    await AntiEntropy("node1", CONFIG, None, RING, FakeMembership(set()), None).stop()


# ------------------------------------------------------------------ unit: server side

def test_on_compare_ring_mismatch(pair, caplog):
    a, b = pair
    request = dynamo_pb2.TreeRoot(sender="node1", ring_fingerprint="other", root=b"x")
    with caplog.at_level(logging.ERROR):
        assert b.on_compare(request).same is True
        assert b.on_compare(request).same is True
    assert sum("differs from mine" in r.message for r in caplog.records) == 1


async def test_on_compare_different_sends_all_buckets(pair):
    a, b = pair
    (key,) = keys_owned_by("node1", "node2", count=1)
    await b.storage.apply(key, ver(b"v", {"node2": 1}))
    request = dynamo_pb2.TreeRoot(sender="node1", ring_fingerprint=a.ring_fingerprint,
                                  root=merkle.build({}).root)
    reply = b.on_compare(request)
    assert reply.same is False and len(reply.buckets) == merkle.BUCKETS


async def test_on_exchange_skips_unshared(pair, caplog):
    a, b = pair
    unshared = key_not_owned_by("node2", also_owned_by="node1")
    entry = dynamo_pb2.KeyVersions(key=unshared, versions=[version_to_proto(ver(b"v", {"node1": 1}))])
    request = dynamo_pb2.BucketData(sender="node1", buckets=[merkle.bucket_of(unshared)],
                                    entries=[entry])
    with caplog.at_level(logging.WARNING):
        await b.on_exchange(request)
        await b.on_exchange(request)
    assert b.storage.get(unshared) == []
    assert sum("do not share" in r.message for r in caplog.records) == 1


async def test_on_exchange_answers_own_entries_capped(pair):
    a, b = pair
    keys = keys_owned_by("node1", "node2", count=300)
    for key in keys:
        await b.storage.apply(key, ver(b"v", {"node2": 1}))
    reply = await b.on_exchange(dynamo_pb2.BucketData(sender="node1", buckets=list(range(100))))
    assert list(reply.buckets) == list(range(MAX_BUCKETS_PER_ROUND))
    assert reply.entries
    assert all(merkle.bucket_of(e.key) < MAX_BUCKETS_PER_ROUND for e in reply.entries)


# ------------------------------------------------------------------ cluster

FAST = {"anti_entropy_ms": 200, "sloppy_quorum": False}


def owners(cluster, key: str) -> list[str]:
    return next(iter(cluster.nodes.values())).ring.preference_list(key, cluster.config.n)


async def put_on(cluster, node: str, key: str, version: Version) -> None:
    """Write one version straight to one replica: no coordinator, no read repair, no hints."""
    reply = await cluster.replica(node).ReplicaPut(dynamo_pb2.ReplicaPutRequest(
        sender="test", key=key, version=version_to_proto(version)), timeout=2)
    assert reply.ok


def converged(cluster, keys) -> bool:
    """Every running owner of every key stores the same versions (a local read: no repair)."""
    for key in keys:
        prints = {frozenset(fingerprint(cluster.nodes[n].storage.get(key)))
                  for n in owners(cluster, key) if n in cluster.nodes}
        if len(prints) != 1:
            return False
    return True


def all_have(cluster, keys) -> bool:
    return converged(cluster, keys) and all(
        cluster.nodes[owners(cluster, k)[0]].storage.get(k) for k in keys)


async def test_cluster_identical_only_root(make_cluster, monkeypatch):
    cluster = await make_cluster(5, **FAST)
    calls = {"compare": 0, "exchange": 0}
    real_compare, real_exchange = AntiEntropy.on_compare, AntiEntropy.on_exchange

    def counting_compare(self, request):
        calls["compare"] += 1
        return real_compare(self, request)

    async def counting_exchange(self, request):
        calls["exchange"] += 1
        return await real_exchange(self, request)
    monkeypatch.setattr(AntiEntropy, "on_compare", counting_compare)
    monkeypatch.setattr(AntiEntropy, "on_exchange", counting_exchange)

    keys = [f"same{i}" for i in range(20)]
    for key in keys:
        reply = await cluster.client("node1").Put(dynamo_pb2.PutRequest(key=key, value=b"v"), timeout=2)
        assert reply.status == dynamo_pb2.OK
    await cluster.wait_until(lambda: all_have(cluster, keys), timeout=3)   # third copy lands too
    # A round that compared before the third copy landed may still exchange; let it finish.
    await asyncio.sleep(0.5)
    calls.update(compare=0, exchange=0)
    await asyncio.sleep(1.0)                                # ~5 rounds per node
    assert calls["compare"] >= 5
    assert calls["exchange"] == 0


async def test_cluster_missing_key(make_cluster):
    cluster = await make_cluster(5, **FAST)
    key = "cart:42"
    await put_on(cluster, owners(cluster, key)[0], key, ver(b"book", {"node1": 1}))
    await cluster.wait_until(
        lambda: all(cluster.nodes[n].storage.get(key) for n in owners(cluster, key)), timeout=3)
    assert converged(cluster, [key])


async def test_cluster_stopped_replica_catches_up(make_cluster):
    cluster = await make_cluster(5, **FAST)
    down = "node3"
    keys = [k for k in (f"user:{i}" for i in range(200)) if down in owners(cluster, k)][:20]
    await cluster.stop_node(down)
    entry = next(n for n in cluster.nodes)
    for key in keys:
        reply = await cluster.client(entry).Put(dynamo_pb2.PutRequest(key=key, value=b"v"), timeout=2)
        assert reply.status == dynamo_pb2.OK                # W = 2 of the two running owners
    await cluster.restart(down)
    assert not any(cluster.nodes[down].storage.get(k) for k in keys)
    await cluster.wait_until(lambda: all(cluster.nodes[down].storage.get(k) for k in keys), timeout=5)
    assert converged(cluster, keys)


async def test_cluster_delete_spreads(make_cluster):
    cluster = await make_cluster(5, **FAST)
    key = "cart:7"
    for node in owners(cluster, key):
        await put_on(cluster, node, key, ver(b"v", {"node1": 1}))
    await put_on(cluster, owners(cluster, key)[1], key, ver(b"", {"node1": 2}, tombstone=True))
    await cluster.wait_until(lambda: all(
        [v.tombstone for v in cluster.nodes[n].storage.get(key)] == [True]
        for n in owners(cluster, key)), timeout=3)


async def test_cluster_siblings_converge(make_cluster):
    cluster = await make_cluster(5, **FAST)
    key = "cart:9"
    first, second, _ = owners(cluster, key)
    await put_on(cluster, first, key, ver(b"x", {"node1": 1}))
    await put_on(cluster, second, key, ver(b"y", {"node2": 1}))
    await cluster.wait_until(lambda: all(
        sorted(v.value for v in cluster.nodes[n].storage.get(key)) == [b"x", b"y"]
        for n in owners(cluster, key)), timeout=3)


async def test_cluster_hints_untouched(make_cluster):
    cluster = await make_cluster(5, **FAST)
    key = "cart:42"
    owner = owners(cluster, key)[2]
    stand_in = next(n for n in cluster.config.nodes if n not in owners(cluster, key))
    await cluster.kill(owner)                       # so hinted handoff cannot deliver the hint
    reply = await cluster.replica(stand_in).ReplicaPut(dynamo_pb2.ReplicaPutRequest(
        sender="test", key=key, version=version_to_proto(ver(b"held", {"node1": 1})),
        hint_for=owner), timeout=2)
    assert reply.ok
    before = cluster.nodes[stand_in].storage.hints()
    await asyncio.sleep(1.0)                                # ~5 anti-entropy rounds per node
    assert cluster.nodes[stand_in].storage.hints() == before
    assert key not in cluster.nodes[stand_in].storage.keys()


async def test_cluster_random_convergence(make_cluster):
    cluster = await make_cluster(5, **FAST)
    rng = random.Random(11)
    keys = [f"r{i}" for i in range(50)]
    for _ in range(300):
        key = rng.choice(keys)
        counters = {node: rng.randint(1, 4) for node in rng.sample(NODES, rng.randint(1, 2))}
        tombstone = rng.random() < 0.15
        value = b"" if tombstone else repr(sorted(counters.items())).encode()
        version = ver(value, counters, ms=rng.randint(1, 1000), tombstone=tombstone)
        await put_on(cluster, rng.choice(owners(cluster, key)), key, version)
    await cluster.wait_until(lambda: converged(cluster, keys), timeout=10)


async def test_cluster_large_drift(make_cluster, monkeypatch):
    cluster = await make_cluster(5, **FAST)
    sizes = []
    real_exchange = AntiEntropy.on_exchange

    async def recording_exchange(self, request):
        sizes.append(len(request.buckets))
        return await real_exchange(self, request)
    monkeypatch.setattr(AntiEntropy, "on_exchange", recording_exchange)

    keys = [k for k in (f"big{i}" for i in range(3000)) if "node1" in owners(cluster, k)][:1000]
    storage = cluster.nodes["node1"].storage
    for key in keys:
        await storage.apply(key, ver(b"v", {"node1": 1}))
    await cluster.wait_until(lambda: all_have(cluster, keys), timeout=15)
    assert sizes and max(sizes) <= MAX_BUCKETS_PER_ROUND

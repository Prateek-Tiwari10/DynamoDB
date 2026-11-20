"""Gossip and failure detection in a real in-process cluster (test timers: gossip 100 ms,
SUSPECT after 500 ms, DEAD after 1000 ms).

States can flap by design, so tests never check "all ALIVE at this instant": they wait for a
condition with ``wait_until``.
"""

import asyncio

import grpc

from dynamo import dynamo_pb2, gossip, membership as membership_module
from dynamo.config import ClusterConfig
from dynamo.dynamo_pb2 import MemberState
from dynamo.gossip import gossip_once
from dynamo.membership import Membership

ALIVE, SUSPECT, DEAD = MemberState.ALIVE, MemberState.SUSPECT, MemberState.DEAD
NODES = [f"node{i}" for i in range(1, 6)]


def unavailable() -> grpc.aio.AioRpcError:
    return grpc.aio.AioRpcError(grpc.StatusCode.UNAVAILABLE, grpc.aio.Metadata(),
                                grpc.aio.Metadata(), details="link cut by the test")


async def states_seen_by(cluster, observer: str) -> dict[str, int]:
    """What ``observer`` reports through ClusterInfo (the call behind ``kv info``)."""
    info = await cluster.client(observer).ClusterInfo(dynamo_pb2.Empty())
    return {member.node_id: member.state for member in info.members}


def everyone_sees(cluster, node_id: str, state: int):
    """Condition: every running node except ``node_id`` reports ``node_id`` in ``state``."""
    async def check() -> bool:
        for observer in cluster.nodes:
            if observer != node_id and (await states_seen_by(cluster, observer))[node_id] != state:
                return False
        return True
    return check


def own_row(cluster, node_id: str) -> dynamo_pb2.Member:
    members = cluster.nodes[node_id].membership.members()
    return next(m for m in members if m.node_id == node_id)


def record_changes(cluster, about: str) -> dict[str, list[tuple[int, int]]]:
    """Subscribe on every running node; collect the (old, new) changes each sees for ``about``."""
    seen: dict[str, list[tuple[int, int]]] = {}
    for observer, node in cluster.nodes.items():
        if observer == about:
            continue
        seen[observer] = []
        node.membership.subscribe(
            lambda node_id, old, new, log=seen[observer]:
                log.append((old, new)) if node_id == about else None)
    return seen


# ---------------------------------------------------------------- cluster tests

async def test_all_nodes_see_all_alive(cluster):
    async def all_alive() -> bool:
        for observer in NODES:
            if set((await states_seen_by(cluster, observer)).values()) != {ALIVE}:
                return False
        return True
    await cluster.wait_until(all_alive, timeout=3)


async def test_kill_suspect_then_dead_everywhere(cluster):
    changes = record_changes(cluster, about="node3")
    await cluster.kill("node3")

    await cluster.wait_until(
        lambda: all(seen[:2] == [(ALIVE, SUSPECT), (SUSPECT, DEAD)] for seen in changes.values()),
        timeout=3)
    await cluster.wait_until(everyone_sees(cluster, "node3", DEAD), timeout=3)


async def test_kill_shows_suspect_in_cluster_info(cluster):
    await cluster.kill("node3")

    async def node1_sees_suspect() -> bool:
        return (await states_seen_by(cluster, "node1"))["node3"] == SUSPECT
    await cluster.wait_until(node1_sees_suspect, timeout=3)


async def test_restart_alive_again(cluster):
    await cluster.kill("node3")
    await cluster.wait_until(everyone_sees(cluster, "node3", DEAD), timeout=3)
    await cluster.restart("node3")
    await cluster.wait_until(everyone_sees(cluster, "node3", ALIVE), timeout=3)


async def test_fast_restart_same_incarnation(cluster, monkeypatch):
    """node3 comes back with the same boot time and heartbeat 0: it must refute its old row."""
    await cluster.wait_until(lambda: own_row(cluster, "node3").heartbeat >= 20, timeout=5)
    old = own_row(cluster, "node3").incarnation
    await cluster.kill("node3")
    monkeypatch.setattr(membership_module, "boot_ms", lambda: old)
    await cluster.restart("node3")

    await cluster.wait_until(lambda: own_row(cluster, "node3").incarnation > old, timeout=3)
    await cluster.wait_until(everyone_sees(cluster, "node3", ALIVE), timeout=3)

    def everyone_has_new_life() -> bool:
        for observer in ["node1", "node2", "node4", "node5"]:
            rows = cluster.nodes[observer].membership.members()
            if next(m for m in rows if m.node_id == "node3").incarnation <= old:
                return False
        return True
    await cluster.wait_until(everyone_has_new_life, timeout=3)


async def test_partition_heals(cluster, monkeypatch):
    left, right = {"node1", "node2"}, {"node3", "node4", "node5"}
    real_send = gossip.send_gossip

    async def cut_send(membership, peer_id, msg):
        if (membership.self_id in left) != (peer_id in left):
            raise unavailable()
        return await real_send(membership, peer_id, msg)

    monkeypatch.setattr(gossip, "send_gossip", cut_send)

    async def node1_sees_node3_dead() -> bool:
        return (await states_seen_by(cluster, "node1"))["node3"] == DEAD
    await cluster.wait_until(node1_sees_node3_dead, timeout=3)
    assert (await states_seen_by(cluster, "node1"))["node2"] == ALIVE      # own side still fine

    monkeypatch.setattr(gossip, "send_gossip", real_send)                  # heal

    async def all_alive_everywhere() -> bool:
        for observer in NODES:
            if set((await states_seen_by(cluster, observer)).values()) != {ALIVE}:
                return False
        return True
    await cluster.wait_until(all_alive_everywhere, timeout=5)


async def test_hanging_peer_does_not_stall_ticks(cluster, monkeypatch):
    real_send = gossip.send_gossip

    async def hang_on_node2(membership, peer_id, msg):
        if peer_id == "node2":
            await asyncio.sleep(10)
        return await real_send(membership, peer_id, msg)

    monkeypatch.setattr(gossip, "send_gossip", hang_on_node2)
    before = own_row(cluster, "node1").heartbeat
    await asyncio.sleep(1.0)
    after = own_row(cluster, "node1").heartbeat
    assert after - before >= 7                     # about one tick per 100 ms despite the hang


async def test_subscribe_in_cluster(cluster):
    calls = []
    cluster.nodes["node1"].membership.subscribe(
        lambda node_id, old, new: calls.append((node_id, old, new)))
    await cluster.kill("node3")
    await cluster.wait_until(lambda: ("node3", SUSPECT, DEAD) in calls, timeout=3)
    node3_calls = [c for c in calls if c[0] == "node3"]
    assert node3_calls[:2] == [("node3", ALIVE, SUSPECT), ("node3", SUSPECT, DEAD)]


# ------------------------------------------------------------ one round, no cluster

class FakePeers:
    def stub(self, node_id):
        raise AssertionError("the test patches send_gossip; no real stub is used")


async def test_rpc_failure_changes_nothing(monkeypatch):
    config = ClusterConfig(nodes={"node1": "localhost:1", "node2": "localhost:2"})
    m = Membership("node1", config, peers=FakePeers(), ring_fingerprint="fp")

    async def fail(membership, peer_id, msg):
        raise unavailable()

    monkeypatch.setattr(gossip, "send_gossip", fail)
    checked = []
    real_check = m.check_states
    monkeypatch.setattr(m, "check_states", lambda now: (checked.append(now), real_check(now)))

    await gossip_once(m, timeout_s=0.1)
    assert m.state("node2") == ALIVE                            # a failed RPC marks nothing
    assert next(x for x in m.members() if x.node_id == "node1").heartbeat == 1   # still ticked
    assert len(checked) == 1                                    # states still re-checked


async def test_slow_rpc_is_cut_by_timeout(monkeypatch):
    config = ClusterConfig(nodes={"node1": "localhost:1", "node2": "localhost:2"})
    m = Membership("node1", config, peers=FakePeers(), ring_fingerprint="fp")

    async def hang(membership, peer_id, msg):
        await asyncio.sleep(10)

    monkeypatch.setattr(gossip, "send_gossip", hang)
    loop = asyncio.get_running_loop()
    start = loop.time()
    await gossip_once(m, timeout_s=0.1)
    assert loop.time() - start < 1.0

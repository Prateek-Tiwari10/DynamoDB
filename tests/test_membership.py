"""Membership: static table from the config, and the gossip logic (merge, refutation, states).

The gossip logic gets ``now`` passed in, so these tests need no network and no waiting.
"""

import logging
import os

import pytest

from dynamo.config import ClusterConfig, load_config
from dynamo.dynamo_pb2 import GossipMessage, Member, MemberState
from dynamo.handoff import Handoff
from dynamo.membership import Membership

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ALIVE, SUSPECT, DEAD = MemberState.ALIVE, MemberState.SUSPECT, MemberState.DEAD
T = 1_732_000_000_000          # an incarnation (boot time in ms)

THREE = ClusterConfig(nodes={f"node{i}": f"localhost:5005{i}" for i in range(1, 4)},
                      suspect_after_ms=5000, dead_after_ms=10000)


@pytest.fixture
def config():
    return load_config(os.path.join(REPO_ROOT, "cluster.local.yaml"))


@pytest.fixture
def membership(config):
    return Membership("node2", config, peers=None, ring_fingerprint="abcd1234abcd1234")


def three(fingerprint: str = "fp") -> Membership:
    """node1's view of a 3-node cluster: own row (T, 3), every timer started at 0.0."""
    m = Membership("node1", THREE, peers=None, ring_fingerprint=fingerprint)
    m._members["node1"].incarnation = T
    m._members["node1"].heartbeat = 3
    m._last_seen = {node_id: 0.0 for node_id in THREE.nodes}
    return m


def row(node_id: str, incarnation: int, heartbeat: int, **fields) -> Member:
    return Member(node_id=node_id, incarnation=incarnation, heartbeat=heartbeat, **fields)


def gossip(*rows: Member) -> GossipMessage:
    return GossipMessage(sender="node2", members=list(rows))


def pair(m: Membership, node_id: str) -> tuple[int, int]:
    member = next(x for x in m.members() if x.node_id == node_id)
    return member.incarnation, member.heartbeat


# ----------------------------------------------------------- static table (D0)

def test_static_table(membership):
    assert [m.node_id for m in membership.members()] == [f"node{i}" for i in range(1, 6)]
    assert all(m.state == MemberState.ALIVE for m in membership.members())
    assert membership.alive_nodes() == [f"node{i}" for i in range(1, 6)]
    assert membership.is_alive("node4")
    assert membership.address("node3") == "localhost:50053"


def test_self_always_alive(membership):
    assert membership.state("node2") == MemberState.ALIVE


def test_unknown_member(membership):
    with pytest.raises(KeyError):
        membership.state("node9")
    with pytest.raises(KeyError):
        membership.address("node9")


def test_unknown_self_id(config):
    with pytest.raises(KeyError):
        Membership("node9", config, peers=None, ring_fingerprint="")


def test_members_returns_copies(membership):
    membership.members()[0].state = MemberState.DEAD
    assert membership.state("node1") == MemberState.ALIVE


async def test_start_sets_own_incarnation(membership):
    await membership.start()
    by_id = {m.node_id: m for m in membership.members()}
    assert by_id["node2"].incarnation > 0
    assert by_id["node2"].ring_fingerprint == "abcd1234abcd1234"
    assert by_id["node1"].incarnation == 0
    await membership.stop()


async def test_all_alive_after_start(membership):
    await membership.start()
    membership.check_states(membership._last_seen["node1"] + 0.1)
    assert membership.alive_nodes() == [f"node{i}" for i in range(1, 6)]
    await membership.stop()


async def test_stop_idempotent(membership):
    await membership.stop()                      # before start
    await membership.start()
    await membership.stop()
    await membership.stop()                      # twice


async def test_handoff_starts_and_stops(config, membership):
    handoff = Handoff("node2", config, storage=None, membership=membership, peers=None)
    await handoff.start()
    await handoff.stop()


# ------------------------------------------------------------------ merge rule

MERGE_CASES = [
    # mine, received, kept, timer restarted
    ((5, 10), (6, 0), (6, 0), True),      # higher incarnation wins, even with a lower heartbeat
    ((5, 10), (5, 11), (5, 11), True),    # same life, newer heartbeat
    ((5, 10), (5, 10), (5, 10), False),   # equal: nothing new
    ((5, 10), (5, 9), (5, 10), False),    # older heartbeat
    ((5, 10), (4, 99), (5, 10), False),   # older life, whatever its heartbeat
]


@pytest.mark.parametrize("mine, received, kept, restarted", MERGE_CASES,
                         ids=["higher-inc", "higher-hb", "equal", "lower-hb", "lower-inc"])
def test_merge_rule(mine, received, kept, restarted):
    m = three()
    m._members["node2"].incarnation, m._members["node2"].heartbeat = mine
    m.merge(gossip(row("node2", *received)), now=7.0)
    assert pair(m, "node2") == kept
    assert m._last_seen["node2"] == (7.0 if restarted else 0.0)


def test_merge_higher_incarnation_wins():
    m = three()
    m.merge(gossip(row("node2", T, 50)), now=1.0)
    m.merge(gossip(row("node2", T + 9, 0)), now=2.0)          # node2 restarted
    assert pair(m, "node2") == (T + 9, 0)


def test_merge_stale_ignored():
    m = three()
    m.merge(gossip(row("node2", T, 50)), now=1.0)
    m.merge(gossip(row("node2", T, 40)), now=2.0)
    assert pair(m, "node2") == (T, 50)
    assert m._last_seen["node2"] == 1.0


def test_merge_ignores_state():
    m = three()
    m.merge(gossip(row("node2", T, 1, state=DEAD)), now=1.0)
    m.check_states(now=1.5)
    assert m.state("node2") == ALIVE


def test_merge_unknown_node(caplog):
    m = three()
    with caplog.at_level(logging.WARNING):
        m.merge(gossip(row("node9", T, 1)), now=1.0)
        m.merge(gossip(row("node9", T, 2)), now=2.0)
    assert [r.levelno for r in caplog.records].count(logging.WARNING) == 1
    assert "node9" not in [x.node_id for x in m.members()]


def test_fingerprint_mismatch_logged_once(caplog):
    m = three(fingerprint="mine")
    with caplog.at_level(logging.WARNING):
        for heartbeat in range(1, 6):
            m.merge(gossip(row("node2", T, heartbeat, ring_fingerprint="other")), now=heartbeat)
    assert [r.levelno for r in caplog.records].count(logging.ERROR) == 1
    assert pair(m, "node2") == (T, 5)                         # rows still merged


def test_empty_fingerprint_is_not_a_mismatch(caplog):
    m = three(fingerprint="mine")
    with caplog.at_level(logging.WARNING):
        m.merge(gossip(row("node2", T, 1)), now=1.0)
    assert not caplog.records


# ------------------------------------------------------------------ refutation

@pytest.mark.parametrize("received, incarnation_after", [
    ((T, 57), T + 1),          # an older life of mine in the same ms, with a higher heartbeat
    ((T + 5, 0), T + 6),       # a higher incarnation from before my clock went backwards
    ((T, 3), T),               # my own current row echoed back: the normal case
    ((T, 2), T),               # older than mine
])
def test_refutation(received, incarnation_after):
    m = three()
    m.merge(gossip(row("node1", *received)), now=1.0)
    assert pair(m, "node1") == (incarnation_after, 3)          # own heartbeat never overwritten


def test_equal_own_row_no_refute():
    m = three()
    for _ in range(10):
        m.merge(gossip(row("node1", T, 3)), now=1.0)
    assert pair(m, "node1") == (T, 3)


def test_refute_higher_incarnation():
    m = three()
    m.merge(gossip(row("node1", T + 5, 0)), now=1.0)
    assert pair(m, "node1")[0] == T + 6
    m.merge(gossip(row("node1", T + 5, 0)), now=2.0)            # same stale row again: no new bump
    assert pair(m, "node1")[0] == T + 6


def test_own_row_never_overwritten_or_suspected():
    m = three()
    m.merge(gossip(row("node1", T + 5, 99, state=DEAD)), now=1.0)
    m.check_states(now=100.0)
    assert m.state("node1") == ALIVE
    assert pair(m, "node1") == (T + 6, 3)


# --------------------------------------------------------------------- states

@pytest.mark.parametrize("age_s, expected", [(4.9, ALIVE), (5.1, SUSPECT), (10.1, DEAD)])
def test_state_from_heartbeat_age(age_s, expected):
    m = three()
    m.check_states(now=age_s)
    assert m.state("node2") == expected


def test_dead_to_alive_direct():
    m = three()
    calls = []
    m.check_states(now=11.0)
    m.subscribe(lambda node_id, old, new: calls.append((node_id, old, new)))
    m.merge(gossip(row("node2", T, 1)), now=12.0)
    m.check_states(now=12.0)
    assert m.state("node2") == ALIVE
    assert calls == [("node2", DEAD, ALIVE)]


def test_callbacks_one_call_per_change():
    m = three()
    calls = []
    m.subscribe(lambda node_id, old, new: calls.append((node_id, old, new)))
    m.check_states(now=1.0)                       # no change
    m.check_states(now=6.0)                       # ALIVE -> SUSPECT
    m.check_states(now=7.0)                       # still SUSPECT
    m.check_states(now=11.0)                      # SUSPECT -> DEAD
    assert [c for c in calls if c[0] == "node2"] == [("node2", ALIVE, SUSPECT),
                                                    ("node2", SUSPECT, DEAD)]
    assert len(calls) == 4                         # node2 and node3, two changes each


def test_callback_error_isolated(caplog):
    m = three()
    seen = []

    def broken(node_id, old, new):
        raise RuntimeError("bug in a subscriber")

    m.subscribe(broken)
    m.subscribe(lambda node_id, old, new: seen.append(node_id))
    with caplog.at_level(logging.ERROR):
        m.check_states(now=6.0)
    assert sorted(seen) == ["node2", "node3"]      # the second callback still ran
    assert m.state("node2") == SUSPECT             # the state still changed
    assert any(r.levelno == logging.ERROR for r in caplog.records)


# -------------------------------------------------------------- table / gossip

def test_tick_raises_own_heartbeat():
    m = three()
    m.tick()
    m.tick()
    assert pair(m, "node1") == (T, 5)


def test_table_has_sender_and_every_member():
    m = three()
    table = m.table()
    assert table.sender == "node1"
    assert [x.node_id for x in table.members] == ["node1", "node2", "node3"]


def test_on_gossip_merges_then_replies_with_our_table():
    m = three()
    reply = m.on_gossip(gossip(row("node2", T, 8)))
    assert pair(m, "node2") == (T, 8)
    assert reply.sender == "node1"
    assert (T, 8) in [(x.incarnation, x.heartbeat) for x in reply.members if x.node_id == "node2"]

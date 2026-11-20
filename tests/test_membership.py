"""Tests for the D0 membership and handoff stubs: static table, every member ALIVE."""

import os

import pytest

from dynamo.config import load_config
from dynamo.dynamo_pb2 import GossipMessage, MemberState
from dynamo.handoff import Handoff
from dynamo.membership import Membership

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture
def config():
    return load_config(os.path.join(REPO_ROOT, "cluster.local.yaml"))


@pytest.fixture
def membership(config):
    return Membership("node2", config, peers=None, ring_fingerprint="abcd1234abcd1234")


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


def test_gossip_not_built_yet(membership):
    with pytest.raises(NotImplementedError):
        membership.on_gossip(GossipMessage(sender="node1"))


def test_subscribe_accepts_callback(membership):
    membership.subscribe(lambda node_id, old, new: None)


async def test_handoff_starts_and_stops(config, membership):
    handoff = Handoff("node2", config, storage=None, membership=membership, peers=None)
    await handoff.start()
    await handoff.stop()

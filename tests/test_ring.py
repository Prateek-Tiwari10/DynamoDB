"""Unit tests for dynamo/ring.py."""

import itertools

from dynamo.ring import Ring, hash32

NODES = [f"node{i}" for i in range(1, 6)]
KEYS = [f"key{i}" for i in range(10_000)]


def test_same_input_gives_same_ring():
    a, b = Ring(NODES, 32), Ring(list(reversed(NODES)), 32)
    for key in KEYS[:1000]:
        assert a.preference_list(key, 3) == b.preference_list(key, 3)


def test_preference_list_has_n_distinct_nodes():
    ring = Ring(NODES, 32)
    for key in KEYS[:1000]:
        nodes = ring.preference_list(key, 3)
        assert len(nodes) == 3
        assert len(set(nodes)) == 3


def test_walk_visits_every_node_once():
    walk = list(Ring(NODES, 32).walk("cart:42"))
    assert sorted(walk) == NODES


def test_n_larger_than_cluster_returns_every_node():
    assert sorted(Ring(NODES, 32).preference_list("k", 10)) == NODES


def test_removing_a_node_moves_only_its_keys_about_a_fifth():
    full, smaller = Ring(NODES, 32), Ring(NODES[:-1], 32)
    moved = [key for key in KEYS
             if full.preference_list(key, 1) != smaller.preference_list(key, 1)]
    assert all(full.preference_list(key, 1) == ["node5"] for key in moved)
    assert 0.10 < len(moved) / len(KEYS) < 0.30


def test_key_after_the_last_vnode_wraps_to_the_first():
    entries = sorted((hash32(f"{node}#{i}"), node) for node in NODES for i in range(32))
    last_position = entries[-1][0]
    key = next(k for k in (f"k{i}" for i in itertools.count()) if hash32(k) > last_position)
    assert Ring(NODES, 32).preference_list(key, 1) == [entries[0][1]]


def test_key_exactly_on_a_vnode_belongs_to_that_vnode():
    # "node3#7" hashes to exactly the position of node3's vnode 7 (bisect_left)
    assert Ring(NODES, 32).preference_list("node3#7", 1) == ["node3"]


def test_position_is_the_key_hash():
    assert Ring(NODES, 32).position("cart:42") == hash32("cart:42")


def test_fingerprint_is_stable_and_short():
    a, b = Ring(NODES, 32), Ring(list(reversed(NODES)), 32)
    assert a.fingerprint(3) == b.fingerprint(3)
    assert len(a.fingerprint(3)) == 16


def test_fingerprint_changes_with_n_nodes_and_vnodes():
    base = Ring(NODES, 32).fingerprint(3)
    assert Ring(NODES, 32).fingerprint(2) != base
    assert Ring(NODES[:-1], 32).fingerprint(3) != base
    assert Ring(NODES, 16).fingerprint(3) != base

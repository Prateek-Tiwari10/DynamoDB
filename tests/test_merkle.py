"""Tests for dynamo/merkle.py: pure tree functions, no network."""

import random

import pytest

from dynamo import merkle
from dynamo.vclock import VectorClock
from dynamo.version import Version


def ver(value: bytes, counters: dict[str, int], ms: int = 100, tombstone: bool = False) -> Version:
    return Version(value=value, tombstone=tombstone,
                   clock=VectorClock(counters=dict(counters),
                                     updated_ms={node: ms for node in counters}))


def sample() -> dict[str, list[Version]]:
    return {
        "a": [ver(b"1", {"node1": 1})],
        "b": [ver(b"x", {"node1": 1}), ver(b"y", {"node2": 1})],     # siblings
        "c": [ver(b"", {"node3": 2}, tombstone=True)],
    }


def test_order_never_matters():
    entries = sample()
    reordered = {key: list(reversed(entries[key])) for key in reversed(list(entries))}
    assert merkle.build(entries) == merkle.build(reordered)


def test_changed_value_marks_only_its_bucket():
    entries = sample()
    before = merkle.build(entries)
    entries["a"] = [ver(b"2", {"node1": 1})]
    after = merkle.build(entries)
    assert before.root != after.root
    assert merkle.diff(before.buckets, after.buckets) == [merkle.bucket_of("a")]


def test_removed_key_marks_only_its_bucket():
    entries = sample()
    before = merkle.build(entries)
    del entries["b"]
    assert merkle.diff(before.buckets, merkle.build(entries).buckets) == [merkle.bucket_of("b")]


def test_tombstone_differs_from_no_key():
    with_tombstone = {"k": [ver(b"", {"node1": 1}, tombstone=True)]}
    assert merkle.build(with_tombstone).root != merkle.build({}).root


def test_tombstone_differs_from_value():
    assert (merkle.key_digest("k", [ver(b"", {"node1": 1}, tombstone=True)])
            != merkle.key_digest("k", [ver(b"", {"node1": 1})]))


def test_digest_includes_updated_ms():
    assert (merkle.key_digest("k", [ver(b"v", {"node1": 1}, ms=100)])
            != merkle.key_digest("k", [ver(b"v", {"node1": 1}, ms=200)]))


def test_digest_includes_key():
    versions = [ver(b"v", {"node1": 1})]
    assert merkle.key_digest("k1", versions) != merkle.key_digest("k2", versions)


def test_bucket_of_range_and_stable():
    for i in range(1000):
        assert 0 <= merkle.bucket_of(f"key{i}") < merkle.BUCKETS
    assert merkle.bucket_of("cart:42") == merkle.bucket_of("cart:42")


def test_empty_trees_equal():
    tree = merkle.build({})
    assert tree == merkle.build({})
    assert len(tree.buckets) == merkle.BUCKETS


def test_diff_raises_on_length_mismatch():
    with pytest.raises(ValueError, match="bucket count"):
        merkle.diff([b"a"] * 256, [b"a"] * 255)


def test_random_insertion_orders_give_equal_trees():
    rng = random.Random(7)
    entries = {}
    for i in range(200):
        versions = [ver(rng.randbytes(rng.randint(0, 8)),
                        {f"node{rng.randint(1, 5)}": rng.randint(1, 9)},
                        ms=rng.randint(0, 10**6), tombstone=rng.random() < 0.1)
                    for _ in range(rng.randint(1, 3))]
        entries[f"key{i}"] = versions
    keys = list(entries)
    rng.shuffle(keys)
    shuffled = {key: rng.sample(entries[key], len(entries[key])) for key in keys}
    assert merkle.build(entries) == merkle.build(shuffled)

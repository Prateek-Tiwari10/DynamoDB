"""Version-set rules (reconcile, merge_into) and the client context."""

import logging
import random

import pytest

from dynamo.vclock import Order, VectorClock, compare, merge
from dynamo.version import (Version, context_of, fingerprint, merge_into, parse_context,
                            reconcile)

NODES = ["A", "B", "C"]


def clock(counters: dict[str, int], ms: int = 0) -> VectorClock:
    return VectorClock(counters=dict(counters), updated_ms={node: ms for node in counters})


def v(value: bytes, counters: dict[str, int], ms: int = 0, tombstone: bool = False) -> Version:
    return Version(value=value, clock=clock(counters, ms), tombstone=tombstone)


def apply_one_by_one(versions: list[Version]) -> list[Version]:
    stored: list[Version] = []
    for version in versions:
        stored, _ = merge_into(stored, version)
    return stored


def random_version(rng: random.Random) -> Version:
    nodes = rng.sample(NODES, rng.randint(1, 3))
    counters = {node: rng.randint(0, 3) for node in nodes}
    return Version(value=rng.choice([b"x", b"y", b""]),
                   clock=VectorClock(counters=counters,
                                     updated_ms={node: rng.randint(0, 9) for node in nodes}),
                   tombstone=rng.random() < 0.2)


# --------------------------------------------------------------- reconcile

def test_reconcile_result_is_pairwise_concurrent():
    rng = random.Random(7)
    for _ in range(500):
        kept = reconcile([random_version(rng) for _ in range(rng.randint(0, 8))])
        for i, a in enumerate(kept):
            for b in kept[i + 1:]:
                assert compare(a.clock, b.clock) == Order.CONCURRENT


def test_any_order_with_duplicates_gives_the_same_set():
    rng = random.Random(11)
    for _ in range(300):
        versions = [random_version(rng) for _ in range(rng.randint(1, 6))]
        expected = fingerprint(reconcile(versions))
        for _ in range(5):
            shuffled = versions + rng.choices(versions, k=rng.randint(0, 3))   # random duplicates
            rng.shuffle(shuffled)
            assert fingerprint(apply_one_by_one(shuffled)) == expected


def test_no_collapse_counterexample_same_in_both_orders():
    """With collapsing, these two orders would end differently (design section 7)."""
    x_a, x_b, z_a = v(b"x", {"a": 2}), v(b"x", {"b": 2}), v(b"z", {"a": 3})
    first = apply_one_by_one([x_a, x_b, z_a])
    second = apply_one_by_one([x_a, z_a, x_b])
    assert fingerprint(first) == fingerprint(second)
    assert fingerprint(first) == fingerprint([x_b, z_a])          # x{a:2} is BEFORE z{a:3}


def test_same_value_siblings_are_kept_apart():
    kept = reconcile([v(b"x", {"A": 1}), v(b"x", {"B": 1})])
    assert len(kept) == 2


def test_tombstone_sibling_next_to_value_sibling():
    kept = reconcile([v(b"x", {"A": 1}), v(b"", {"B": 1}, tombstone=True)])
    assert sorted(version.tombstone for version in kept) == [False, True]


def test_equal_clock_same_value_merges_updated_ms():
    kept = reconcile([v(b"x", {"A": 2}, ms=10), v(b"x", {"A": 2}, ms=30)])
    assert len(kept) == 1
    assert kept[0].clock.updated_ms == {"A": 30}


def test_equal_clock_different_value_same_winner_and_warning(caplog):
    one, two = v(b"one", {"A": 2}), v(b"two", {"A": 2})
    with caplog.at_level(logging.WARNING):
        first = reconcile([one, two])
        second = reconcile([two, one])
    assert [x.value for x in first] == [x.value for x in second] == [b"one"]
    assert any(record.levelno == logging.WARNING for record in caplog.records)


def test_equal_clock_value_wins_over_tombstone():
    kept = reconcile([v(b"", {"A": 2}, tombstone=True), v(b"z", {"A": 2})])
    assert [(x.value, x.tombstone) for x in kept] == [(b"z", False)]


def test_zero_entries_count_as_missing():
    kept = reconcile([v(b"x", {"A": 1, "B": 0}), v(b"x", {"A": 1})])
    assert len(kept) == 1


def test_reconcile_output_order_is_fixed():
    a, b, c = v(b"a", {"A": 1}), v(b"b", {"B": 1}), v(b"c", {"C": 1})
    assert reconcile([c, a, b]) == reconcile([b, c, a])


# -------------------------------------------------------------- merge_into

def test_merge_into_duplicate_is_not_a_change():
    stored, _ = merge_into([], v(b"x", {"A": 1}, ms=5))
    again, changed = merge_into(stored, v(b"x", {"A": 1}, ms=5))
    assert changed is False
    assert again == stored


def test_merge_into_stale_write_is_not_a_change():
    stored = [v(b"new", {"A": 2})]
    result, changed = merge_into(stored, v(b"old", {"A": 1}))
    assert changed is False
    assert result == stored


def test_merge_into_new_sibling_is_a_change():
    result, changed = merge_into([v(b"x", {"A": 1})], v(b"y", {"B": 1}))
    assert changed is True
    assert len(result) == 2


def test_merge_into_dominating_write_is_a_change():
    result, changed = merge_into([v(b"x", {"A": 1}), v(b"y", {"B": 1})],
                                 v(b"z", {"A": 1, "B": 1, "C": 1}))
    assert changed is True
    assert [x.value for x in result] == [b"z"]


def test_merge_into_newer_updated_ms_only_is_a_change():
    result, changed = merge_into([v(b"x", {"A": 1}, ms=5)], v(b"x", {"A": 1}, ms=9))
    assert changed is True
    assert result[0].clock.updated_ms == {"A": 9}


# ----------------------------------------------------------------- context

KNOWN = {"A", "B", "C"}


def test_context_of_empty_list():
    assert context_of([]) == b""


def test_context_of_exact_bytes():
    versions = [Version(b"x", VectorClock({"B": 1}, {"B": 1732090005000})),
                Version(b"", VectorClock({"A": 3}, {"A": 1732090000000}), tombstone=True)]
    assert context_of(versions) == b'{"A":[3,1732090000000],"B":[1,1732090005000]}'


def test_context_round_trip_is_the_merge_of_all_clocks():
    rng = random.Random(3)
    for _ in range(200):
        versions = [random_version(rng) for _ in range(rng.randint(0, 5))]
        expected = VectorClock()
        for version in versions:
            expected = merge(expected, version.clock)
        assert parse_context(context_of(versions), KNOWN) == expected


def test_parse_empty_context():
    assert parse_context(b"", KNOWN) == VectorClock()


def test_parse_valid_context():
    assert parse_context(b'{"A":[3,100]}', KNOWN) == VectorClock({"A": 3}, {"A": 100})


@pytest.mark.parametrize("data", [
    b"\xff\xfe",                       # not UTF-8
    b"not json",
    b"[1, 2]",                         # not an object
    b'{"Z":[1,0]}',                    # unknown node id
    b'{"A":3}',                        # not a list
    b'{"A":[3]}',                      # one number
    b'{"A":[3,1,2]}',                  # three numbers
    b'{"A":[-1,0]}',                   # negative
    b'{"A":[true,0]}',                 # bool
    b'{"A":[1.0,0]}',                  # float
    b"[" * 100_000,                    # nested too deep for the parser
])
def test_parse_bad_context(data):
    with pytest.raises(ValueError):
        parse_context(data, KNOWN)


def test_parse_random_garbage():
    rng = random.Random(5)
    for _ in range(500):
        data = bytes(rng.randrange(256) for _ in range(rng.randint(1, 30)))
        with pytest.raises(ValueError):
            parse_context(data, KNOWN)

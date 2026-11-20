"""Vector clock functions: compare, merge, advance, prune and the JSON form."""

import random

import pytest

from dynamo.vclock import Order, VectorClock, advance, compare, from_json, merge, prune, to_json


def clock(counters: dict[str, int], ms: int = 0) -> VectorClock:
    """A clock with every entry stamped at the same time ``ms``."""
    return VectorClock(counters=dict(counters), updated_ms={node: ms for node in counters})


def stamped(entries: dict[str, tuple[int, int]]) -> VectorClock:
    """A clock from ``{node: (counter, updated_ms)}``."""
    return VectorClock(counters={n: c for n, (c, _) in entries.items()},
                       updated_ms={n: ms for n, (_, ms) in entries.items()})


# ----------------------------------------------------------------- compare

COMPARE_CASES = [
    # a, b, expected, what it checks
    ({}, {}, Order.EQUAL, "both empty"),
    ({}, {"A": 1}, Order.BEFORE, "empty is before everything"),
    ({"A": 1}, {}, Order.AFTER, "reverse"),
    ({"A": 2}, {"A": 2}, Order.EQUAL, "same counters"),
    ({"A": 1}, {"A": 2}, Order.BEFORE, "same key, smaller"),
    ({"A": 2, "B": 1}, {"A": 2}, Order.AFTER, "b missing a key"),
    ({"A": 2}, {"A": 2, "B": 1}, Order.BEFORE, "a missing a key"),
    ({"A": 2}, {"B": 1}, Order.CONCURRENT, "disjoint keys"),
    ({"A": 2, "B": 1}, {"A": 1, "B": 2}, Order.CONCURRENT, "each ahead somewhere"),
    ({"A": 3}, {"A": 2, "B": 1}, Order.CONCURRENT, "ahead on A, behind on B"),
    ({"A": 3, "B": 1}, {"A": 2, "B": 1}, Order.AFTER, "ahead on A, tie on B"),
    ({"A": 0}, {}, Order.EQUAL, "explicit 0 same as missing"),
]


@pytest.mark.parametrize("a, b, expected, checks", COMPARE_CASES,
                         ids=[case[3] for case in COMPARE_CASES])
def test_compare(a, b, expected, checks):
    assert compare(clock(a), clock(b)) == expected


def test_compare_ignores_updated_ms():
    assert compare(clock({"A": 2}, ms=100), clock({"A": 2}, ms=999)) == Order.EQUAL


def random_clock(rng: random.Random) -> VectorClock:
    nodes = rng.sample(["A", "B", "C", "D"], rng.randint(0, 4))
    return clock({node: rng.randint(0, 3) for node in nodes}, ms=rng.randint(0, 50))


def test_compare_is_symmetric():
    rng = random.Random(1)
    mirror = {Order.BEFORE: Order.AFTER, Order.AFTER: Order.BEFORE,
              Order.EQUAL: Order.EQUAL, Order.CONCURRENT: Order.CONCURRENT}
    for _ in range(2000):
        a, b = random_clock(rng), random_clock(rng)
        assert compare(b, a) == mirror[compare(a, b)]


# ------------------------------------------------------------------- merge

def test_merge_disjoint():
    assert merge(clock({"A": 1}, 10), clock({"B": 2}, 20)) == stamped({"A": (1, 10), "B": (2, 20)})


def test_merge_overlapping_takes_max_counter_and_time():
    a = stamped({"A": (3, 10), "B": (1, 50)})
    b = stamped({"A": (2, 40), "B": (4, 20)})
    assert merge(a, b) == stamped({"A": (3, 40), "B": (4, 50)})


def test_merge_leaves_inputs_unchanged_and_is_symmetric():
    a = stamped({"A": (3, 10)})
    b = stamped({"A": (1, 90), "C": (2, 5)})
    a_before, b_before = a.copy(), b.copy()
    assert merge(a, b) == merge(b, a)
    assert a == a_before and b == b_before


# ----------------------------------------------------------------- advance

def test_advance_sets_counter_and_time_on_a_copy():
    original = stamped({"A": (1, 10), "B": (2, 20)})
    out = advance(original, "A", 5, now_ms=99)
    assert out == stamped({"A": (5, 99), "B": (2, 20)})
    assert original == stamped({"A": (1, 10), "B": (2, 20)})     # input unchanged


def test_advance_adds_a_new_node():
    assert advance(VectorClock(), "C", 1, now_ms=7) == stamped({"C": (1, 7)})


def test_advance_uses_current_time_by_default():
    out = advance(VectorClock(), "A", 1)
    assert out.updated_ms["A"] > 1_700_000_000_000                # a real epoch time in ms


# ------------------------------------------------------------------- prune

def eleven_nodes() -> VectorClock:
    """n00..n10, where n00 has the oldest updated_ms."""
    return stamped({f"n{i:02d}": (1, 100 + i) for i in range(11)})


def test_prune_ten_entries_unchanged():
    ten = stamped({f"n{i:02d}": (1, 100 + i) for i in range(10)})
    assert prune(ten, "n09") == ten


def test_prune_drops_oldest():
    out = prune(eleven_nodes(), "n10")
    assert len(out.counters) == 10
    assert "n00" not in out.counters and "n00" not in out.updated_ms


def test_prune_tie_drops_smaller_node_id():
    tied = eleven_nodes()
    tied.updated_ms["n05"] = 100                                   # ties with n00 on oldest time
    out = prune(tied, "n10")
    assert "n00" not in out.counters
    assert "n05" in out.counters


def test_prune_never_drops_self():
    out = prune(eleven_nodes(), "n00")                             # self has the oldest time
    assert "n00" in out.counters
    assert "n01" not in out.counters                               # next oldest goes instead


def test_prune_leaves_input_unchanged():
    original = eleven_nodes()
    prune(original, "n10")
    assert original == eleven_nodes()


def test_prune_uses_max_entries():
    out = prune(eleven_nodes(), "n10", max_entries=3)
    assert sorted(out.counters) == ["n08", "n09", "n10"]


# --------------------------------------------------------------- JSON form

def test_json_round_trip():
    original = stamped({"B": (1, 1732090005000), "A": (3, 1732090000000)})
    assert to_json(original) == {"A": [3, 1732090000000], "B": [1, 1732090005000]}
    assert from_json(to_json(original)) == original


@pytest.mark.parametrize("raw", [
    [],                                   # not an object
    {"A": 3},                             # not a list
    {"A": [3]},                           # one number
    {"A": [3, 1, 2]},                     # three numbers
    {"A": [-1, 0]},                       # negative counter
    {"A": [1, -5]},                       # negative time
    {"A": [True, 0]},                     # bool is not an int here
    {"A": [1.5, 0]},                      # float
    {"A": ["1", 0]},                      # string
])
def test_from_json_rejects(raw):
    with pytest.raises(ValueError):
        from_json(raw)


def test_from_json_checks_known_nodes_only_when_given():
    assert from_json({"zz": [1, 0]}).counters == {"zz": 1}
    with pytest.raises(ValueError):
        from_json({"zz": [1, 0]}, known_nodes={"A", "B"})

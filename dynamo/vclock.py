"""Vector clocks: pure functions, no I/O.

A clock belongs to one version of one key. It has one entry per node that coordinated a write
to that key. Only ``counters`` decide the order of two versions; ``updated_ms`` (wall-clock time
of the last bump) is used only by ``prune`` to choose which entry to drop, because node clocks
can be skewed.
"""

from __future__ import annotations

import time
from collections.abc import Collection
from dataclasses import dataclass, field
from enum import Enum


@dataclass
class VectorClock:
    """Counters per node, plus when each entry was last bumped. ``==`` compares both dicts."""

    counters: dict[str, int] = field(default_factory=dict)     # node_id -> counter
    updated_ms: dict[str, int] = field(default_factory=dict)   # node_id -> last bump, for prune only

    def copy(self) -> VectorClock:
        """An independent copy: changing it never changes this clock."""
        return VectorClock(counters=dict(self.counters), updated_ms=dict(self.updated_ms))


class Order(Enum):
    """How version ``a`` stands relative to version ``b``."""

    BEFORE = "BEFORE"            # a is older; b includes it
    AFTER = "AFTER"              # a is newer; a includes b
    EQUAL = "EQUAL"
    CONCURRENT = "CONCURRENT"    # neither includes the other: siblings


def compare(a: VectorClock, b: VectorClock) -> Order:
    """Compare two clocks by their counters only. A missing node counts as 0."""
    a_ahead = False
    b_ahead = False
    for node in set(a.counters) | set(b.counters):
        x = a.counters.get(node, 0)
        y = b.counters.get(node, 0)
        if x > y:
            a_ahead = True
        elif y > x:
            b_ahead = True
    if a_ahead and b_ahead:
        return Order.CONCURRENT
    if a_ahead:
        return Order.AFTER
    if b_ahead:
        return Order.BEFORE
    return Order.EQUAL


def merge(a: VectorClock, b: VectorClock) -> VectorClock:
    """A new clock with, per node, the higher counter and the later ``updated_ms``."""
    out = a.copy()
    for node, counter in b.counters.items():
        out.counters[node] = max(out.counters.get(node, 0), counter)
    for node, ms in b.updated_ms.items():
        out.updated_ms[node] = max(out.updated_ms.get(node, 0), ms)
    return out


def advance(clock: VectorClock, node_id: str, counter: int,
            now_ms: int | None = None) -> VectorClock:
    """A copy with ``node_id``'s entry set to ``counter``, stamped with ``now_ms``.

    The coordinator computes ``counter`` (DEC-02); this function only stores it.
    ``now_ms`` defaults to the current wall-clock time; tests pass a fixed value.
    """
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    out = clock.copy()
    out.counters[node_id] = counter
    out.updated_ms[node_id] = now_ms
    return out


def prune(clock: VectorClock, self_id: str, max_entries: int = 10) -> VectorClock:
    """A copy with at most ``max_entries`` entries, dropping the oldest ``updated_ms`` first.

    Ties drop the smaller node id, so every node makes the same choice. ``self_id`` (the
    coordinator that just bumped its entry) is never dropped. Pruning can only make two ordered
    versions look concurrent (an extra sibling), never lose a write. Used only by the
    coordinator right after ``advance``.
    """
    out = clock.copy()
    while len(out.counters) > max_entries:
        candidates = [node for node in out.counters if node != self_id]
        victim = min(candidates, key=lambda node: (out.updated_ms.get(node, 0), node))
        del out.counters[victim]
        out.updated_ms.pop(victim, None)
    return out


# ------------------------------------------------------------ JSON form (context and WAL)

def to_json(clock: VectorClock) -> dict[str, list[int]]:
    """``{node: [counter, updated_ms]}``; used by the client context and the storage files."""
    return {node: [counter, clock.updated_ms.get(node, 0)]
            for node, counter in sorted(clock.counters.items())}


def from_json(raw: object, known_nodes: Collection[str] | None = None) -> VectorClock:
    """Build a clock from the ``to_json`` form, checking every part. Raises ValueError.

    With ``known_nodes``, a node id outside it is also rejected (client contexts); without it
    (storage files) any node id is accepted.
    """
    if not isinstance(raw, dict):
        raise ValueError("clock must be a JSON object")
    clock = VectorClock()
    for node, entry in raw.items():
        if known_nodes is not None and node not in known_nodes:
            raise ValueError(f"unknown node id in clock: {node!r}")
        if not isinstance(entry, list) or len(entry) != 2:
            raise ValueError(f"clock entry for {node!r} must be [counter, updated_ms]")
        counter, ms = entry
        for number in (counter, ms):
            if type(number) is not int:          # rejects bool and float, which isinstance would not
                raise ValueError(f"clock entry for {node!r} must hold two integers")
            if number < 0:
                raise ValueError(f"clock entry for {node!r} is negative")
        clock.counters[node] = counter
        clock.updated_ms[node] = ms
    return clock

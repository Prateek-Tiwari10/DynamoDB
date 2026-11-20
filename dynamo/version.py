"""Versions of a key and the rules for keeping a set of them.

The stored set for a key is the versions that no other received version is newer than.
Concurrent versions are kept side by side as siblings. Same-value siblings are NOT collapsed
into one: collapsing would make the result depend on the order the versions arrived in.
Without it, the result depends only on which versions were received.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Collection
from dataclasses import dataclass, field

from dynamo import vclock
from dynamo.vclock import Order, VectorClock

logger = logging.getLogger(__name__)


@dataclass
class Version:
    """One stored version of a key."""

    value: bytes = b""                                          # empty for a tombstone
    clock: VectorClock = field(default_factory=VectorClock)
    tombstone: bool = False                                     # True = deleted marker


def clock_key(clock: VectorClock) -> tuple[tuple[str, int], ...]:
    """The counters as a sorted tuple, zero entries left out: two clocks are EQUAL exactly
    when their keys are the same."""
    return tuple(sorted((node, counter) for node, counter in clock.counters.items()
                        if counter > 0))


def reconcile(versions: list[Version]) -> list[Version]:
    """Keep only the versions that no other version is newer than.

    1. Versions with EQUAL clocks become one: same value and tombstone flag -> one version with
       the clocks merged; different values (only after a bug or a bad retry) -> the one with
       the smaller (tombstone, value) is kept, so every replica picks the same one, and a
       WARNING is logged.
    2. Every version that is BEFORE another one is dropped.
    3. The rest is sorted by clock, so the output never depends on the input order.
    The result is pairwise CONCURRENT.
    """
    best: dict[tuple[tuple[str, int], ...], Version] = {}
    for version in versions:
        key = clock_key(version.clock)
        old = best.get(key)
        if old is None:
            best[key] = version
        elif (old.tombstone, old.value) == (version.tombstone, version.value):
            best[key] = Version(value=old.value, tombstone=old.tombstone,
                                clock=vclock.merge(old.clock, version.clock))
        else:
            logger.warning("same clock, different value: %s", key)
            best[key] = min(old, version, key=lambda v: (v.tombstone, v.value))
    candidates = list(best.values())
    kept = [v for v in candidates
            if not any(vclock.compare(v.clock, other.clock) == Order.BEFORE for other in candidates)]
    return sorted(kept, key=lambda v: clock_key(v.clock))


def fingerprint(versions: list[Version]) -> frozenset:
    """Everything that makes a set of versions what it is, ignoring list order.

    Two lists with the same fingerprint store exactly the same thing (counters, ``updated_ms``,
    value and tombstone flag of every version).
    """
    return frozenset((clock_key(v.clock), tuple(sorted(v.clock.updated_ms.items())),
                      v.value, v.tombstone) for v in versions)


def merge_into(stored: list[Version], new: Version) -> tuple[list[Version], bool]:
    """What a replica stores after receiving ``new``, and whether that differs from ``stored``.

    Versions BEFORE ``new`` are dropped; ``new`` is ignored if a stored version is AFTER or
    EQUAL to it; otherwise it becomes a sibling. ``changed`` is False for a duplicate or a stale
    write, so storage writes no log line for it. A merge that only changes a stored clock counts
    as a change.
    """
    result = reconcile(stored + [new])
    return result, fingerprint(result) != fingerprint(stored)


def context_of(versions: list[Version]) -> bytes:
    """The merged clock of all versions (tombstones included) as JSON bytes; ``b""`` if none.

    Not pruned: merging pruned clocks may give more than ``vclock_max_entries`` entries.
    """
    if not versions:
        return b""
    merged = VectorClock()
    for version in versions:
        merged = vclock.merge(merged, version.clock)
    return json.dumps(vclock.to_json(merged), sort_keys=True, separators=(",", ":")).encode("utf-8")


def parse_context(data: bytes, known_nodes: Collection[str]) -> VectorClock:
    """A client's context back to a clock. ``b""`` -> empty clock.

    Raises ValueError (the server answers INVALID) for anything else that is not exactly what
    ``context_of`` makes: bad UTF-8 or JSON, not an object, a node id not in ``known_nodes``,
    an entry that is not two non-negative integers (bools rejected).
    """
    if not data:
        return VectorClock()
    try:
        raw = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as error:   # deep [[[[...
        raise ValueError("context is not valid JSON") from error
    return vclock.from_json(raw, known_nodes)

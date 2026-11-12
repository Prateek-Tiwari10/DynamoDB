"""Consistent hash ring with virtual nodes. Pure code: no I/O, no network.

Every node builds the same ring from the node list in the config, so any node can compute the
replicas of a key without asking anyone.
"""

import hashlib
import itertools
from bisect import bisect_left
from collections.abc import Iterator

RING_SIZE = 2**32


def hash32(text: str) -> int:
    """Ring position of a string: MD5, reduced to 32 bits. Same on every node and every run."""
    return int(hashlib.md5(text.encode()).hexdigest(), 16) % RING_SIZE


class Ring:
    """A ring of ``vnodes`` positions per physical node."""

    def __init__(self, node_ids: list[str], vnodes: int) -> None:
        if not node_ids or vnodes < 1:
            raise ValueError("a ring needs at least one node and one vnode per node")
        unique_ids = sorted(set(node_ids))
        # (position, node_id), sorted; ties on position are broken by node_id
        self._entries = sorted(
            (hash32(f"{node_id}#{i}"), node_id) for node_id in unique_ids for i in range(vnodes)
        )
        self._positions = [position for position, _ in self._entries]
        self._node_count = len(unique_ids)

    def position(self, key: str) -> int:
        """Ring position of a key."""
        return hash32(key)

    def walk(self, key: str) -> Iterator[str]:
        """Every physical node once, clockwise from the key's position."""
        start = bisect_left(self._positions, hash32(key))
        seen: set[str] = set()
        for step in range(len(self._entries)):
            _, node_id = self._entries[(start + step) % len(self._entries)]
            if node_id in seen:
                continue
            seen.add(node_id)
            yield node_id
            if len(seen) == self._node_count:
                return

    def preference_list(self, key: str, n: int) -> list[str]:
        """The first ``n`` distinct nodes clockwise from the key: the key's replicas."""
        return list(itertools.islice(self.walk(key), n))

    def fingerprint(self, n: int) -> str:
        """Short ID of this ring and N. Two nodes with different fingerprints disagree on placement."""
        digest = hashlib.sha256()
        for position, node_id in self._entries:
            digest.update(f"{position}:{node_id}\n".encode())
        digest.update(f"n={n}".encode())
        return digest.hexdigest()[:16]

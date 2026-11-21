"""Merkle (hash) tree over a set of keys, used by anti-entropy to find keys two replicas disagree on.

Pure functions: no I/O, no asyncio. The tree has a fixed shape of three levels:

    root  = sha256(bucket hash 0 + bucket hash 1 + ... + bucket hash 255)
    bucket hash = sha256(digests of the keys in that bucket, sorted by key)
    key digest  = sha256(key + every stored version of it)

Two replicas with the same keys and versions get the same root. If the roots differ, comparing the
256 bucket hashes tells which buckets hold the keys that differ, so only those are exchanged.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from dynamo.storage import version_to_json
from dynamo.version import Version

BUCKETS = 256


@dataclass(frozen=True)
class Tree:
    """The root hash and the hash of every bucket (list index = bucket id)."""

    root: bytes
    buckets: list[bytes]


def bucket_of(key: str) -> int:
    """The bucket a key belongs to: first 2 bytes of sha256(key), mod BUCKETS. Same on every node."""
    return int.from_bytes(hashlib.sha256(key.encode()).digest()[:2], "big") % BUCKETS


def key_digest(key: str, versions: list[Version]) -> bytes:
    """Hash of a key and its versions. Same key + same set of versions (any order) -> same digest.

    Covers value, tombstone flag, counters and ``updated_ms`` of every version.
    """
    encoded = sorted(json.dumps(version_to_json(v), sort_keys=True) for v in versions)
    return hashlib.sha256(json.dumps([key, encoded]).encode()).digest()


def build(entries: dict[str, list[Version]]) -> Tree:
    """Build the tree for ``entries`` (key -> versions). Insertion order never matters."""
    per_bucket: list[list[tuple[str, bytes]]] = [[] for _ in range(BUCKETS)]
    for key, versions in entries.items():
        per_bucket[bucket_of(key)].append((key, key_digest(key, versions)))
    buckets = [hashlib.sha256(b"".join(digest for _, digest in sorted(items))).digest()
               for items in per_bucket]
    return Tree(root=hashlib.sha256(b"".join(buckets)).digest(), buckets=buckets)


def diff(mine: list[bytes], theirs: list[bytes]) -> list[int]:
    """Bucket ids whose hashes differ. Raises ValueError if the two lists differ in length."""
    if len(mine) != len(theirs):
        raise ValueError(f"bucket count {len(mine)} != {len(theirs)}")
    return [i for i, (a, b) in enumerate(zip(mine, theirs)) if a != b]

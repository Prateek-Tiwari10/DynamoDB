"""Merkle anti-entropy: a background loop that makes replicas agree without any client read.

Hinted handoff and read repair fix most differences, but not all: a hint can be lost with its
stand-in, a key nobody reads is never read-repaired, and a write can reach fewer than N replicas.
So every ``anti_entropy_ms`` this node picks one partner (round-robin over the other nodes,
skipping any that is not ALIVE) and compares the keys both of them own:

1. Both sides build a Merkle tree (``merkle.py``) over their own copy of those keys.
2. ``CompareTree``: send the root. Equal roots (the normal case) end the round after one RPC.
3. Otherwise the partner answers with its 256 bucket hashes; the buckets that differ (at most
   ``MAX_BUCKETS_PER_ROUND``) are swapped with ``ExchangeBuckets``: each side sends its keys and
   versions in those buckets, and each side merges what it gets with ``storage.apply``.

``apply`` is idempotent and order-free (DEC-11), so a round that runs twice, or two rounds of the
same pair at the same time, are harmless. Hints are never part of a tree: they live apart from
this node's own data.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING

import grpc

from dynamo import merkle
from dynamo.dynamo_pb2 import BucketData, KeyVersions, TreeBuckets, TreeRoot
from dynamo.wire import version_from_proto, version_to_proto

if TYPE_CHECKING:
    from dynamo.config import ClusterConfig
    from dynamo.membership import Membership
    from dynamo.peers import Peers
    from dynamo.ring import Ring
    from dynamo.storage import Storage
    from dynamo.version import Version

logger = logging.getLogger(__name__)

MAX_BUCKETS_PER_ROUND = 32   # keeps one ExchangeBuckets message small; a big drift takes more rounds


class AntiEntropy:
    """This node's side of anti-entropy: the client loop and the two server-side handlers."""

    def __init__(self, self_id: str, config: ClusterConfig, storage: Storage, ring: Ring,
                 membership: Membership, peers: Peers) -> None:
        """Remember the parts it needs. Does no I/O."""
        self.self_id = self_id
        self.config = config
        self.storage = storage
        self.ring = ring
        self.membership = membership
        self.peers = peers
        self.ring_fingerprint = ring.fingerprint(config.n)
        self._cursor = 0                               # round-robin position in the partner list
        self._warned: set[tuple[str, str]] = set()
        self._task: asyncio.Task | None = None

    # ----- lifecycle

    async def start(self) -> None:
        """Start the background loop. The first round runs one interval after start."""
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Stop the loop. Safe to call twice, and before ``start()``."""
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    # ----- client side: one round with the next partner

    async def _run(self) -> None:
        interval_s = self.config.anti_entropy_ms / 1000
        while True:
            await asyncio.sleep(interval_s)
            try:
                await self.sync_once()
            except Exception:
                logger.warning("anti-entropy round failed", exc_info=True)   # the loop never dies

    def _next_partner(self) -> str | None:
        """Round-robin over the other nodes (sorted), skipping those that are not ALIVE."""
        others = sorted(node_id for node_id in self.config.nodes if node_id != self.self_id)
        for _ in range(len(others)):
            peer = others[self._cursor % len(others)]
            self._cursor += 1
            if self.membership.is_alive(peer):
                return peer
        return None

    async def sync_once(self) -> bool:
        """One round with the next partner. True if buckets were exchanged, False otherwise
        (no partner ALIVE, trees equal, or an RPC failed)."""
        peer = self._next_partner()
        if peer is None:
            return False
        timeout = self.config.rpc_timeout_ms / 1000
        stub = self.peers.stub(peer)
        tree = merkle.build(self._shared(peer))
        try:
            reply = await stub.CompareTree(
                TreeRoot(sender=self.self_id, root=tree.root, ring_fingerprint=self.ring_fingerprint),
                timeout=timeout)
            if reply.same:
                return False
            differing = merkle.diff(tree.buckets, list(reply.buckets))[:MAX_BUCKETS_PER_ROUND]
            if not differing:
                return False      # the data changed between the two builds; the next round sees it
            answer = await stub.ExchangeBuckets(
                BucketData(sender=self.self_id, buckets=differing,
                           entries=self._entries(peer, differing)),
                timeout=timeout)
        except grpc.aio.AioRpcError as error:
            logger.debug("anti-entropy with %s failed: %s", peer, error.code())
            return False
        changed = await self._apply_entries(peer, answer.entries)
        logger.info("anti-entropy with %s: %d buckets differed, got %d keys, %d versions changed here",
                    peer, len(differing), len(answer.entries), changed)
        return True

    # ----- server side (called by the NodeService handlers in replica.py)

    def on_compare(self, request: TreeRoot) -> TreeBuckets:
        """Compare the sender's root with ours. Equal (or a different ring): ``same``.
        Otherwise send all our bucket hashes so the sender can find the differing buckets."""
        if request.ring_fingerprint != self.ring_fingerprint:
            self._warn_once("ring", request.sender, "ring of %s differs from mine; not syncing",
                            request.sender, level=logging.ERROR)
            return TreeBuckets(same=True)
        tree = merkle.build(self._shared(request.sender))
        if tree.root == request.root:
            return TreeBuckets(same=True)
        return TreeBuckets(same=False, buckets=tree.buckets)

    async def on_exchange(self, request: BucketData) -> BucketData:
        """Apply the sender's keys of the asked buckets and answer with ours of the same buckets."""
        bucket_ids = list(request.buckets)[:MAX_BUCKETS_PER_ROUND]
        mine = self._entries(request.sender, bucket_ids)    # what we had, before applying theirs
        await self._apply_entries(request.sender, request.entries)
        return BucketData(sender=self.self_id, buckets=bucket_ids, entries=mine)

    # ----- helpers

    def _owns_both(self, key: str, peer: str) -> bool:
        """True if this node and ``peer`` are both replicas of ``key``."""
        owners = self.ring.preference_list(key, self.config.n)
        return self.self_id in owners and peer in owners

    def _shared(self, peer: str) -> dict[str, list[Version]]:
        """Our own data for the keys we share with ``peer``. One synchronous pass (no await),
        so no write can change the dict half way through."""
        return {key: self.storage.get(key) for key in self.storage.keys()
                if self._owns_both(key, peer)}

    def _entries(self, peer: str, bucket_ids: Iterable[int]) -> list[KeyVersions]:
        """Our keys and versions, shared with ``peer``, that fall in the given buckets."""
        wanted = set(bucket_ids)
        return [KeyVersions(key=key, versions=[version_to_proto(v) for v in versions])
                for key, versions in self._shared(peer).items() if merkle.bucket_of(key) in wanted]

    async def _apply_entries(self, peer: str, entries: Iterable[KeyVersions]) -> int:
        """Merge every received version into storage. Keys we do not share with ``peer`` are
        skipped. Returns how many versions changed our data."""
        changed = 0
        for entry in entries:
            if not self._owns_both(entry.key, peer):
                self._warn_once("unshared", peer, "%s sent key %s that we do not share; skipped",
                                peer, entry.key)
                continue
            for version in entry.versions:
                if await self.storage.apply(entry.key, version_from_proto(version)):
                    changed += 1
        return changed

    def _warn_once(self, kind: str, peer: str, message: str, *args: object,
                   level: int = logging.WARNING) -> None:
        """Log a problem about a peer only the first time, not every round."""
        if (kind, peer) in self._warned:
            return
        self._warned.add((kind, peer))
        logger.log(level, message, *args)

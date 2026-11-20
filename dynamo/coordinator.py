"""Coordinates client requests: routing, quorum reads and writes.

So far only the single-replica path is built: when this node is the key's only replica (N=1),
Put / Get / Delete are answered from local storage. Forwarding and the replica fan-out come in
D5, read repair in D7, sloppy quorum in D8; until then those cases answer UNIMPLEMENTED.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any

from dynamo.dynamo_pb2 import (
    INVALID,
    NOT_FOUND,
    OK,
    DeleteRequest,
    DeleteResponse,
    ForwardRequest,
    ForwardResponse,
    GetRequest,
    GetResponse,
    PutRequest,
    PutResponse,
)
from dynamo import vclock
from dynamo.vclock import VectorClock
from dynamo.version import Version, context_of, parse_context

if TYPE_CHECKING:
    from dynamo.config import ClusterConfig
    from dynamo.membership import Membership
    from dynamo.peers import Peers
    from dynamo.ring import Ring
    from dynamo.storage import Storage

logger = logging.getLogger(__name__)


def request_budget(context, config: ClusterConfig) -> float:
    """Seconds this node may spend on a request: the smaller of ``request_timeout_ms`` and the
    time left on the caller's deadline. No deadline from the caller means ``request_timeout_ms``."""
    budget = config.request_timeout_ms / 1000
    remaining = context.time_remaining()
    if remaining is None:
        return budget
    return min(budget, remaining)


class Coordinator:
    """Entry point for Put, Get and Delete, both from clients and from Forward."""

    def __init__(self, node_id: str, config: ClusterConfig, ring: Ring, storage: Storage,
                 peers: Peers, membership: Membership) -> None:
        self.node_id = node_id
        self.config = config
        self.ring = ring
        self.storage = storage
        self.peers = peers
        self.membership = membership
        self._locks: dict[str, asyncio.Lock] = {}
        self._tasks: set[asyncio.Task] = set()

    # ----- client entry: coordinate here or Forward -----

    async def put(self, req: PutRequest, budget: float) -> PutResponse:
        """Route a client Put. Only the single-replica case is built (D2); forwarding is D5."""
        self._require_only_replica(req.key)
        return await self._do_put(req, budget)

    async def get(self, req: GetRequest, budget: float) -> GetResponse:
        """Route a client Get. Only the single-replica case is built (D2); forwarding is D5."""
        self._require_only_replica(req.key)
        return await self._do_get(req, budget)

    async def delete(self, req: DeleteRequest, budget: float) -> DeleteResponse:
        """Route a client Delete. Only the single-replica case is built (D2); forwarding is D5."""
        self._require_only_replica(req.key)
        return await self._do_delete(req, budget)

    def _require_only_replica(self, key: str) -> None:
        """Until D5 this node can only answer keys whose one and only replica it is."""
        if self.ring.preference_list(key, self.config.n) != [self.node_id]:
            raise NotImplementedError("requests that need other replicas")

    # ----- Forward entry: always coordinates here, never forwards -----

    async def coordinate(self, req: ForwardRequest, budget: float) -> ForwardResponse:
        """Coordinate a forwarded request on this node (D5)."""
        raise NotImplementedError("coordinate")

    async def drain(self, timeout: float) -> None:
        """Wait up to ``timeout`` seconds for background tasks, then cancel the rest."""
        if not self._tasks:
            return
        _, pending = await asyncio.wait(set(self._tasks), timeout=timeout)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    # ----- shared core (D5, D7, D8) -----

    async def _do_put(self, req: PutRequest, budget: float) -> PutResponse:
        """New clock, local write (1 ack). D5 adds the ReplicaPut fan-out and waiting for W."""
        try:
            context = parse_context(req.context, self.config.nodes)
        except ValueError:
            return PutResponse(status=INVALID)
        version = await self._write_local(req.key, context, req.value, tombstone=False)
        return PutResponse(status=OK, context=context_of([version]),
                           written_to=[self.node_id])

    async def _do_get(self, req: GetRequest, budget: float) -> GetResponse:
        """Local read: live values plus the merged context. D5 adds ReplicaGet, D7 read repair.

        Storage keeps same-value siblings apart (D3), so equal values are sent only once.
        """
        versions = self.storage.get(req.key)
        live = [v for v in versions if not v.tombstone]
        context = context_of(versions)             # b"" for an unknown key
        if not live:  # unknown key, or only tombstones: still return the context
            return GetResponse(status=NOT_FOUND, context=context, read_from=[self.node_id])
        values = list(dict.fromkeys(v.value for v in live))   # drop repeats, keep order
        return GetResponse(status=OK, values=values, context=context,
                           read_from=[self.node_id])

    async def _do_delete(self, req: DeleteRequest, budget: float) -> DeleteResponse:
        """A put of a tombstone."""
        try:
            context = parse_context(req.context, self.config.nodes)
        except ValueError:
            return DeleteResponse(status=INVALID)
        await self._write_local(req.key, context, b"", tombstone=True)
        return DeleteResponse(status=OK)

    async def _write_local(self, key: str, context: VectorClock, value: bytes,
                           tombstone: bool) -> Version:
        """Build the new version under the key's lock and store it (WAL + fsync)."""
        async with self._key_lock(key):
            clock = self._new_clock(context, self.storage.get(key))
            version = Version(value=value, clock=clock, tombstone=tombstone)
            await self.storage.apply(key, version)
        return version

    def _new_clock(self, context: VectorClock, stored: list[Version]) -> VectorClock:
        """The context clock with this node's counter set to
        max(context[self], highest own counter among the stored versions) + 1  (DEC-02),
        then pruned to ``vclock_max_entries``. Stored clocks are not merged in: that would
        drop siblings the client never saw."""
        own_stored = max((v.clock.counters.get(self.node_id, 0) for v in stored), default=0)
        counter = max(context.counters.get(self.node_id, 0), own_stored) + 1
        clock = vclock.advance(context, self.node_id, counter)
        return vclock.prune(clock, self.node_id, self.config.vclock_max_entries)

    async def _forward(self, order: list[str], req: ForwardRequest,
                       budget: float) -> ForwardResponse | None:
        """Try each node in order; None when every attempt failed (D5)."""
        raise NotImplementedError("_forward")

    def _route_order(self, pref: list[str]) -> list[str]:
        """ALIVE, then SUSPECT, then DEAD, list order inside each (D7)."""
        raise NotImplementedError("_route_order")

    def _key_lock(self, key: str) -> asyncio.Lock:
        """The lock that serializes clock counters for one key."""
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    def _spawn(self, coro: Coroutine[Any, Any, Any]) -> None:
        """Run ``coro`` in the background and keep track of it until it finishes."""
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

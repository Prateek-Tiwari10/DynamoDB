"""Coordinates client requests: routing, quorum reads and writes.

Routing, the quorum logic and read repair are built in D5, D7 and D8. This file fixes the
methods the server calls and the background-task handling used at shutdown.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any

from dynamo.dynamo_pb2 import (
    DeleteRequest,
    DeleteResponse,
    ForwardRequest,
    ForwardResponse,
    GetRequest,
    GetResponse,
    PutRequest,
    PutResponse,
)

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
        """Route a client Put (D5)."""
        raise NotImplementedError("put")

    async def get(self, req: GetRequest, budget: float) -> GetResponse:
        """Route a client Get (D5)."""
        raise NotImplementedError("get")

    async def delete(self, req: DeleteRequest, budget: float) -> DeleteResponse:
        """Route a client Delete (D5)."""
        raise NotImplementedError("delete")

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
        """Per-key lock, new clock, local write, ReplicaPut fan-out, wait for W (D5)."""
        raise NotImplementedError("_do_put")

    async def _do_get(self, req: GetRequest, budget: float) -> GetResponse:
        """Local read, ReplicaGet fan-out, wait for R, reconcile, schedule repair (D5, D7)."""
        raise NotImplementedError("_do_get")

    async def _do_delete(self, req: DeleteRequest, budget: float) -> DeleteResponse:
        """A put of a tombstone (D5)."""
        raise NotImplementedError("_do_delete")

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

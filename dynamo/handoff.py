"""Hinted handoff: deliver writes held for a node that was down when they were made.

A stand-in keeps such writes as hints (``storage.put_hint``). This loop sends them back to their
owner with a plain ReplicaPut (empty ``hint_for``) once the owner is ALIVE, and deletes each hint
only after the owner acks it. A hint sent twice is harmless: the owner already has a version with
the same clock and ignores it.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING

import grpc

from dynamo.dynamo_pb2 import ALIVE, ReplicaPutRequest
from dynamo.wire import version_to_proto

if TYPE_CHECKING:
    from dynamo.config import ClusterConfig
    from dynamo.membership import Membership
    from dynamo.peers import Peers
    from dynamo.storage import Storage
    from dynamo.version import Version

logger = logging.getLogger(__name__)


class Handoff:
    """Retry loop that sends stored hints back to their owners once they are ALIVE."""

    def __init__(self, self_id: str, config: ClusterConfig, storage: Storage,
                 membership: Membership, peers: Peers) -> None:
        """Remember the parts it needs. Does no I/O."""
        self.self_id = self_id
        self.config = config
        self.storage = storage
        self.membership = membership
        self.peers = peers
        self._wake = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """Wake up when a node turns ALIVE, and start the retry loop."""
        self.membership.subscribe(self._on_state)
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Stop the retry loop and wait until it has stopped. Undelivered hints stay on disk."""
        if self._task is None:
            return
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def deliver_once(self) -> int:
        """One round: for every owner that is ALIVE, send its hints one by one and drop each one
        that is acked. The first failure ends that owner's round. Returns how many were delivered."""
        delivered = 0
        for owner, hints in self.storage.hints().items():
            if self.membership.state(owner) != ALIVE:
                continue
            sent = 0
            for key, version in hints:
                if not await self._send(owner, key, version):
                    break
                await self.storage.drop_hint(owner, key, version)
                sent += 1
            if sent:
                logger.info("handoff: delivered %d hint(s) to %s", sent, owner)
            delivered += sent
        return delivered

    async def _run(self) -> None:
        """Every ``handoff_retry_ms``, or at once when a node turns ALIVE: one delivery round."""
        while True:
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._wake.wait(), self.config.handoff_retry_ms / 1000)
            self._wake.clear()
            try:
                await self.deliver_once()
            except Exception:                       # never let one bad round stop the loop
                logger.exception("handoff round failed")

    def _on_state(self, node_id: str, old: int, new: int) -> None:
        """Membership callback (must be quick, no await): wake the loop when a node turns ALIVE."""
        if new == ALIVE:
            self._wake.set()

    async def _send(self, owner: str, key: str, version: Version) -> bool:
        """One ReplicaPut to the owner. True on an ack, False on any failure (logged)."""
        request = ReplicaPutRequest(sender=self.self_id, key=key, version=version_to_proto(version))
        try:
            reply = await self.peers.stub(owner).ReplicaPut(
                request, timeout=self.config.rpc_timeout_ms / 1000)
        except grpc.aio.AioRpcError as error:
            logger.warning("handoff: hint %s for %s not delivered: %s", key, owner, error.code().name)
            return False
        return reply.ok

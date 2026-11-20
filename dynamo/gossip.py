"""Gossip: spreads every node's heartbeat so each node can tell who is up.

Every ``gossip_interval_ms`` (fixed rate) a node raises its own heartbeat, swaps its whole
table with one random other node (push-pull), merges the reply, and re-checks every member's
state. The logic of the table is in ``membership.py``; this module only does timing, peer
choice and the RPC.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import TYPE_CHECKING

import grpc

if TYPE_CHECKING:
    from dynamo.dynamo_pb2 import GossipMessage
    from dynamo.membership import Membership

logger = logging.getLogger(__name__)


async def send_gossip(membership: Membership, peer_id: str, msg: GossipMessage) -> GossipMessage:
    """The one place a Gossip RPC is sent. Tests patch this to cut links between nodes."""
    return await membership.peers.stub(peer_id).Gossip(msg)


async def gossip_once(membership: Membership, timeout_s: float) -> None:
    """One gossip round: tick, exchange tables with one random peer, re-check states.

    The peer is picked from every other node in the config, DEAD ones included, so a healed
    network split finds itself again. A failed or slow exchange changes nothing: only
    heartbeat age decides a node's state.
    """
    membership.tick()
    others = [node_id for node_id in membership.config.nodes if node_id != membership.self_id]
    if others:
        peer = random.choice(others)
        try:
            reply = await asyncio.wait_for(send_gossip(membership, peer, membership.table()),
                                           timeout_s)
            membership.merge(reply, time.monotonic())
        except (grpc.aio.AioRpcError, asyncio.TimeoutError) as error:
            logger.debug("gossip to %s failed: %s", peer, error)
    membership.check_states(time.monotonic())


async def gossip_loop(membership: Membership) -> None:
    """Run gossip rounds at a fixed rate until cancelled. A failing round never stops the loop.

    The RPC timeout is ``min(rpc_timeout_ms, gossip_interval_ms)``, and the next tick is due one
    interval after the previous tick *started*, so a slow peer cannot slow our own heartbeat.
    The first round runs one interval after start, not at once: when the nodes boot together,
    a gossip sent at once mostly hits peers that are not listening yet, and gRPC then holds
    that channel in reconnect backoff, which also delays the first real requests to the peer.
    """
    config = membership.config
    interval_s = config.gossip_interval_ms / 1000
    timeout_s = min(config.rpc_timeout_ms, config.gossip_interval_ms) / 1000
    next_tick = time.monotonic() + interval_s
    await asyncio.sleep(interval_s)
    while True:
        try:
            await gossip_once(membership, timeout_s)
        except Exception:
            logger.warning("gossip round failed", exc_info=True)
        next_tick += interval_s
        now = time.monotonic()
        if next_tick < now:          # fell behind (a slow round): do not burst to catch up
            next_tick = now
        await asyncio.sleep(next_tick - now)

"""Gossip: spreads every node's heartbeat so each node can tell who is up.

Only the signature exists for now; the loop is built in D6.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dynamo.membership import Membership


async def gossip_loop(membership: Membership) -> None:
    """Every gossip interval: raise own heartbeat, swap tables with one random peer, re-check states (D6)."""
    raise NotImplementedError("gossip loop (D6)")

"""Hinted handoff: deliver writes held for a node that was down when they were made.

The hints are stored by storage.put_hint and delivered with ReplicaPut (empty hint_for).
The retry loop is built in D9; until then start() and stop() do nothing, so the server
can build and run a node with this in place.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dynamo.config import ClusterConfig
    from dynamo.membership import Membership
    from dynamo.peers import Peers
    from dynamo.storage import Storage


class Handoff:
    """Retry loop that sends stored hints back to their owners once they are ALIVE (D9)."""

    def __init__(self, self_id: str, config: ClusterConfig, storage: Storage,
                 membership: Membership, peers: Peers) -> None:
        """Remember the parts it needs. Does no I/O."""
        self.self_id = self_id
        self.config = config
        self.storage = storage
        self.membership = membership
        self.peers = peers

    async def start(self) -> None:
        """Start the retry loop (D9). Nothing runs yet."""

    async def stop(self) -> None:
        """Stop the retry loop (D9). Nothing runs yet."""

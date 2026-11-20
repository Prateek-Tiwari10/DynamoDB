"""Who is in the cluster and whether each node is up, as this node sees it.

The member list is fixed by cluster.yaml. Liveness will come only from gossip (D6). Until
then there is no gossip loop: every member stays ALIVE, so the coordinator can use this
interface unchanged and simply never sees a SUSPECT or DEAD node.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import TYPE_CHECKING

from dynamo.dynamo_pb2 import GossipMessage, Member, MemberState

if TYPE_CHECKING:
    from dynamo.config import ClusterConfig
    from dynamo.peers import Peers

logger = logging.getLogger(__name__)

StateCallback = Callable[[str, "MemberState", "MemberState"], None]


class Membership:
    """This node's table of every member: address, heartbeat, incarnation and state."""

    def __init__(self, self_id: str, config: ClusterConfig, peers: Peers,
                 ring_fingerprint: str) -> None:
        """Build the table from the config, every member ALIVE. Does no I/O."""
        if self_id not in config.nodes:
            raise KeyError(f"{self_id} is not in the config")
        self.self_id = self_id
        self.config = config
        self.peers = peers
        self.ring_fingerprint = ring_fingerprint
        self._members: dict[str, Member] = {
            node_id: Member(node_id=node_id, address=address, state=MemberState.ALIVE)
            for node_id, address in config.nodes.items()
        }
        self._members[self_id].ring_fingerprint = ring_fingerprint
        self._callbacks: list[StateCallback] = []

    async def start(self) -> None:
        """Set this node's incarnation to its boot time. The gossip loop is added in D6."""
        self._members[self.self_id].incarnation = int(time.time() * 1000)

    async def stop(self) -> None:
        """Stop the gossip loop (D6). Nothing runs yet."""

    def state(self, node_id: str) -> MemberState:
        """This node's opinion of ``node_id``. Itself is always ALIVE. Unknown id -> KeyError."""
        member = self._members[node_id]
        if node_id == self.self_id:
            return MemberState.ALIVE
        return member.state

    def is_alive(self, node_id: str) -> bool:
        return self.state(node_id) == MemberState.ALIVE

    def alive_nodes(self) -> list[str]:
        """Ids of every ALIVE member, in config order."""
        return [node_id for node_id in self._members if self.is_alive(node_id)]

    def members(self) -> list[Member]:
        """A copy of the full table, for ClusterInfo."""
        result = []
        for node_id, member in self._members.items():
            copy = Member()
            copy.CopyFrom(member)
            copy.state = self.state(node_id)
            result.append(copy)
        return result

    def address(self, node_id: str) -> str:
        """The host:port peers use to reach ``node_id``. Unknown id -> KeyError."""
        return self._members[node_id].address

    def on_gossip(self, msg: GossipMessage) -> GossipMessage:
        """Merge the sender's table and reply with ours (D6)."""
        raise NotImplementedError("gossip")

    def subscribe(self, callback: StateCallback) -> None:
        """Call ``callback(node_id, old_state, new_state)`` on every state change.

        Nothing changes state until D6, so callbacks are stored but not called yet.
        """
        self._callbacks.append(callback)

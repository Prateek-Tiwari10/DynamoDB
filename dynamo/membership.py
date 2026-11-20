"""Who is in the cluster and whether each node is up, as this node sees it.

The member list is fixed by cluster.yaml. Each row holds the member's heartbeat (raised by its
owner every gossip tick) and incarnation (the owner's boot time in ms). Gossip only spreads
those two numbers. ALIVE / SUSPECT / DEAD is this node's own opinion, from how long ago it
saw a member's (incarnation, heartbeat) go up, measured on this node's monotonic clock.

This module is pure logic: the time ``now`` is passed in, so it can be tested without a
network. Timing, peer choice and the RPC live in ``gossip.py``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Callable
from typing import TYPE_CHECKING

from dynamo.dynamo_pb2 import GossipMessage, Member, MemberState
from dynamo.gossip import gossip_loop

if TYPE_CHECKING:
    from dynamo.config import ClusterConfig
    from dynamo.peers import Peers

logger = logging.getLogger(__name__)

StateCallback = Callable[[str, "MemberState", "MemberState"], None]


def boot_ms() -> int:
    """This node's boot time in ms, used as its incarnation. A function so tests can patch it."""
    return int(time.time() * 1000)


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
        now = time.monotonic()
        self._last_seen: dict[str, float] = {node_id: now for node_id in self._members}
        self._warned: set[tuple[str, str]] = set()
        self._task: asyncio.Task | None = None

    # ----- lifecycle

    async def start(self) -> None:
        """Take a new incarnation, start every member's timer now, and start the gossip loop."""
        now = time.monotonic()
        self._members[self.self_id].incarnation = boot_ms()
        self._last_seen = {node_id: now for node_id in self._members}
        self._task = asyncio.create_task(gossip_loop(self))

    async def stop(self) -> None:
        """Stop the gossip loop. Safe to call twice, and before ``start()``."""
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    # ----- reading the table (frozen in D0)

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
        """A copy of the full table, for ClusterInfo and gossip."""
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

    def subscribe(self, callback: StateCallback) -> None:
        """Call ``callback(node_id, old_state, new_state)`` on every state change, flaps included.

        Called from the gossip loop after the table has changed. The callback must be quick and
        must not await (start a task for async work). One that raises is logged; the others
        still run.
        """
        self._callbacks.append(callback)

    # ----- gossip (D6a)

    def tick(self) -> None:
        """Raise this node's own heartbeat by one (once per gossip tick)."""
        self._members[self.self_id].heartbeat += 1

    def table(self) -> GossipMessage:
        """What this node sends in a gossip exchange: its whole table."""
        return GossipMessage(sender=self.self_id, members=self.members())

    def on_gossip(self, msg: GossipMessage) -> GossipMessage:
        """A peer gossiped to us: merge its table and reply with ours (push-pull)."""
        self.merge(msg, time.monotonic())
        return self.table()

    def merge(self, msg: GossipMessage, now: float) -> None:
        """Keep, per member, the row with the higher (incarnation, heartbeat).

        When a member's pair goes up, its timer restarts at ``now``. The received ``state`` is
        ignored: states are this node's own opinion. Our own row is never overwritten, but a
        newer row about us makes us refute it (``_maybe_refute``).
        """
        for row in msg.members:
            if row.node_id not in self._members:
                self._warn_once("unknown", row.node_id, "gossip names unknown node %s", row.node_id)
                continue
            if row.node_id == self.self_id:
                self._maybe_refute(row)
                continue
            self._check_fingerprint(row)
            mine = self._members[row.node_id]
            if (row.incarnation, row.heartbeat) > (mine.incarnation, mine.heartbeat):
                mine.incarnation = row.incarnation
                mine.heartbeat = row.heartbeat
                mine.ring_fingerprint = row.ring_fingerprint
                self._last_seen[row.node_id] = now
            # row.state is ignored; the address always comes from the config

    def check_states(self, now: float) -> None:
        """Set every other member to ALIVE, SUSPECT or DEAD from how long ago its pair went up.

        DEAD -> ALIVE happens directly when the heartbeat rises again. Every change is logged
        and passed to the subscribed callbacks.
        """
        for node_id, member in self._members.items():
            if node_id == self.self_id:
                continue
            age_ms = (now - self._last_seen[node_id]) * 1000
            if age_ms > self.config.dead_after_ms:
                new = MemberState.DEAD
            elif age_ms > self.config.suspect_after_ms:
                new = MemberState.SUSPECT
            else:
                new = MemberState.ALIVE
            if new != member.state:
                old = member.state
                member.state = new
                logger.info("%s %s -> %s", node_id, MemberState.Name(old), MemberState.Name(new))
                self._notify(node_id, old, new)

    # ----- helpers

    def _maybe_refute(self, row: Member) -> None:
        """A row about us that is newer than our own means an older life of ours is still
        believed (fast restart in the same ms, or a clock that went backwards): take a higher
        incarnation so our current row wins everywhere. An equal row is the normal echo."""
        me = self._members[self.self_id]
        if (row.incarnation, row.heartbeat) > (me.incarnation, me.heartbeat):
            old = me.incarnation
            me.incarnation = row.incarnation + 1
            logger.info("refuting stale row about me: incarnation %d -> %d", old, me.incarnation)

    def _check_fingerprint(self, row: Member) -> None:
        """A different ring fingerprint means a different cluster.yaml: log once per peer."""
        if row.ring_fingerprint and row.ring_fingerprint != self.ring_fingerprint:
            self._warn_once("fingerprint", row.node_id,
                            "ring fingerprint of %s is %s, mine is %s (different cluster.yaml?)",
                            row.node_id, row.ring_fingerprint, self.ring_fingerprint,
                            level=logging.ERROR)

    def _warn_once(self, kind: str, node_id: str, message: str, *args: object,
                   level: int = logging.WARNING) -> None:
        """Log a problem about a peer only the first time, not on every gossip tick."""
        if (kind, node_id) in self._warned:
            return
        self._warned.add((kind, node_id))
        logger.log(level, message, *args)

    def _notify(self, node_id: str, old: MemberState, new: MemberState) -> None:
        for callback in self._callbacks:
            try:
                callback(node_id, old, new)
            except Exception:
                logger.exception("membership callback failed for %s", node_id)

"""Coordinates client requests: routing, quorum reads and writes.

Any node can receive a request. Writes are coordinated by the key's first replica (others forward
to it), reads by any replica. The coordinator handles its own copy first (one answer), asks the
other replicas in parallel and replies as soon as W (or R) answers are in. Later answers finish in
the background. After a read, replicas that were missing versions are repaired in the
background (read repair). Replicas this node sees as DEAD are not asked.

Sloppy quorum (flag ``sloppy_quorum``): a write does not wait for owners that are not ALIVE. Each
one is replaced by a stand-in, the next ALIVE node clockwise past the N owners, which keeps the
write as a hint for that owner and hands it over later (handoff). Reads only ever use the owners.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any

import grpc

from dynamo import reqid, vclock
from dynamo.dynamo_pb2 import (
    ALIVE,
    DEAD,
    INVALID,
    NOT_FOUND,
    OK,
    QUORUM_FAILED,
    SUSPECT,
    DeleteRequest,
    DeleteResponse,
    ForwardRequest,
    ForwardResponse,
    GetRequest,
    GetResponse,
    PutRequest,
    PutResponse,
    ReplicaGetRequest,
    ReplicaPutRequest,
)
from dynamo.vclock import VectorClock
from dynamo.version import Version, clock_key, context_of, parse_context, reconcile
from dynamo.wire import version_from_proto, version_to_proto

if TYPE_CHECKING:
    from dynamo.config import ClusterConfig
    from dynamo.membership import Membership
    from dynamo.peers import Peers
    from dynamo.ring import Ring
    from dynamo.storage import Storage

logger = logging.getLogger(__name__)

# The only gRPC codes that mean "the target never answered" (D0-M 3.1).
RETRY_CODES = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}
# Route order: ALIVE first, then SUSPECT, then DEAD.
STATE_RANK = {ALIVE: 0, SUSPECT: 1, DEAD: 2}


def request_budget(context, config: ClusterConfig) -> float:
    """Seconds this node may spend on a request: the smaller of ``request_timeout_ms`` and the
    time left on the caller's deadline. No deadline from the caller means ``request_timeout_ms``."""
    budget = config.request_timeout_ms / 1000
    remaining = context.time_remaining()
    if remaining is None:
        return budget
    return min(budget, remaining)


def repair_plan(reconciled: list[Version],
                holdings: dict[str, list[Version]]) -> dict[str, list[Version]]:
    """For each node, the reconciled versions it is missing: those for which none of its own
    versions has an EQUAL clock. Nodes that miss nothing are left out."""
    plan: dict[str, list[Version]] = {}
    for node_id, held in holdings.items():
        have = {clock_key(v.clock) for v in held}
        missing = [v for v in reconciled if clock_key(v.clock) not in have]
        if missing:
            plan[node_id] = missing
    return plan


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
        self._repairing: set[tuple[str, str]] = set()     # (key, node) repairs in flight

    # ----- client entry: coordinate here or Forward -----

    async def put(self, req: PutRequest, budget: float) -> PutResponse:
        """A client Put: coordinated by the first replica in route order."""
        order = self._route_order(self._replicas(req.key))
        if order[0] == self.node_id:
            return await self._do_put(req, budget)
        reply = await self._forward(order, ForwardRequest(sender=self.node_id, put=req), budget)
        return reply.put if reply is not None else PutResponse(status=QUORUM_FAILED)

    async def get(self, req: GetRequest, budget: float) -> GetResponse:
        """A client Get: coordinated by any replica; a non-replica forwards it."""
        replicas = self._replicas(req.key)
        if self.node_id in replicas:
            return await self._do_get(req, budget)
        order = self._route_order(replicas)
        reply = await self._forward(order, ForwardRequest(sender=self.node_id, get=req), budget)
        return reply.get if reply is not None else GetResponse(status=QUORUM_FAILED)

    async def delete(self, req: DeleteRequest, budget: float) -> DeleteResponse:
        """A client Delete: coordinated like a Put."""
        order = self._route_order(self._replicas(req.key))
        if order[0] == self.node_id:
            return await self._do_delete(req, budget)
        reply = await self._forward(order, ForwardRequest(sender=self.node_id, delete=req), budget)
        return reply.delete if reply is not None else DeleteResponse(status=QUORUM_FAILED)

    # ----- Forward entry: always coordinates here, never forwards -----

    async def coordinate(self, req: ForwardRequest, budget: float) -> ForwardResponse:
        """Coordinate a forwarded request on this node, even if this node's ring disagrees."""
        op = req.WhichOneof("op")
        if op == "put":
            return ForwardResponse(put=await self._do_put(req.put, budget))
        if op == "get":
            return ForwardResponse(get=await self._do_get(req.get, budget))
        if op == "delete":
            return ForwardResponse(delete=await self._do_delete(req.delete, budget))
        raise ValueError("Forward without a request")

    async def drain(self, timeout: float) -> None:
        """Wait up to ``timeout`` seconds for background tasks, then cancel the rest."""
        if not self._tasks:
            return
        _, pending = await asyncio.wait(set(self._tasks), timeout=timeout)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    # ----- shared core -----

    async def _do_put(self, req: PutRequest, budget: float) -> PutResponse:
        """Write locally, then replicate until W acks."""
        status, version, written = await self._write(req.key, req.context, req.value, False,
                                                     req.w or self.config.w, budget)
        context = context_of([version]) if status == OK else b""
        return PutResponse(status=status, context=context, written_to=written)

    async def _do_delete(self, req: DeleteRequest, budget: float) -> DeleteResponse:
        """A put of a tombstone."""
        status, _, _ = await self._write(req.key, req.context, b"", True,
                                         req.w or self.config.w, budget)
        return DeleteResponse(status=status)

    async def _write(self, key: str, raw_context: bytes, value: bytes, tombstone: bool, w: int,
                     budget: float) -> tuple[int, Version | None, list[str]]:
        """The write path shared by Put and Delete: (status, version written, nodes that acked)."""
        try:
            context = parse_context(raw_context, self.config.nodes)
        except ValueError:
            return INVALID, None, []
        replicas = self._replicas(key)
        targets = self._targets(key, replicas)
        stand_ins = sum(1 for _, hint_for in targets if hint_for)
        if self._alive_count(replicas) + stand_ins < w:  # fast fail: W cannot be reached
            return QUORUM_FAILED, None, []
        deadline = self._deadline(budget)
        version = await self._write_local(key, context, value, tombstone)
        written = await self._replicate(key, version, targets, w, deadline)
        status = OK if len(written) >= w else QUORUM_FAILED
        return status, version, written

    async def _do_get(self, req: GetRequest, budget: float) -> GetResponse:
        """Read locally, then collect replica answers until R, reconcile."""
        r = req.r or self.config.r
        replicas = self._replicas(req.key)
        if self._alive_count(replicas) < r:  # fast fail: R cannot be reached
            return GetResponse(status=QUORUM_FAILED)
        deadline = self._deadline(budget)
        local = self.storage.get(req.key)    # this node's answer counts as one
        others = [node_id for node_id in self._reachable(replicas) if node_id != self.node_id]
        answers, late = await self._collect(req.key, others, r, deadline)
        self._spawn(self._repair(req.key, local, list(answers), late))   # after the reply
        read_from = [self.node_id] + [node_id for node_id, _ in answers]
        if len(read_from) < r:
            return GetResponse(status=QUORUM_FAILED, read_from=read_from)

        versions = reconcile(local + [v for _, found in answers for v in found])
        live = [v for v in versions if not v.tombstone]
        context = context_of(versions)       # b"" for an unknown key
        if not live:  # unknown key, or only tombstones: still return the context
            return GetResponse(status=NOT_FOUND, context=context, read_from=read_from)
        values = list(dict.fromkeys(v.value for v in live))   # equal values once (DEC-11)
        return GetResponse(status=OK, values=values, context=context, read_from=read_from)

    async def _write_local(self, key: str, context: VectorClock, value: bytes,
                           tombstone: bool) -> Version:
        """Build the new version under the key's lock and store it (WAL + fsync) = ack 1."""
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

    # ----- talking to the other replicas -----

    def _targets(self, key: str, replicas: list[str]) -> list[tuple[str, str]]:
        """Where a write's other copies go, as (node, hint_for); hint_for "" = a real owner.

        Sloppy quorum off: every other owner not seen as DEAD (D7). On: the ALIVE other owners,
        plus one stand-in for each other owner that is not ALIVE (SUSPECT or DEAD), paired in
        preference order; fewer stand-ins if not enough nodes are ALIVE."""
        others = [node_id for node_id in replicas if node_id != self.node_id]
        if not self.config.sloppy_quorum:
            return [(node_id, "") for node_id in self._reachable(others)]
        alive = [node_id for node_id in others if self.membership.state(node_id) == ALIVE]
        missing = [node_id for node_id in others if node_id not in alive]
        stand_ins = self._stand_ins(key, replicas, len(missing))
        return [(node_id, "") for node_id in alive] + list(zip(stand_ins, missing))

    def _stand_ins(self, key: str, replicas: list[str], count: int) -> list[str]:
        """The first ``count`` ALIVE nodes after the owners, clockwise from the key."""
        candidates = (node_id for node_id in self.ring.walk(key)
                      if node_id not in replicas and self.membership.state(node_id) == ALIVE)
        return list(itertools.islice(candidates, count))

    async def _replicate(self, key: str, version: Version, targets: list[tuple[str, str]], w: int,
                         deadline: float) -> list[str]:
        """Send the version to every (node, hint_for) target in parallel; the nodes that acked,
        this node first."""
        tasks = {asyncio.create_task(self._replica_put(node_id, key, version, deadline, hint_for)):
                 node_id for node_id, hint_for in targets}
        acks, late = await self._wait_for(tasks, have=1, needed=w, deadline=deadline)
        for task in late:
            self._track(task)                            # not cancelled (D5 Q4)
        return [self.node_id] + [node_id for node_id, _ in acks]

    async def _collect(self, key: str, others: list[str], r: int, deadline: float
                       ) -> tuple[list[tuple[str, list[Version]]], dict[asyncio.Task, str]]:
        """Ask ``others`` for their versions in parallel. Returns (node, versions) per answer
        and the calls still running (for read repair)."""
        tasks = {asyncio.create_task(self._replica_get(node_id, key, deadline)): node_id
                 for node_id in others}
        return await self._wait_for(tasks, have=1, needed=r, deadline=deadline)

    async def _wait_for(self, tasks: dict[asyncio.Task, str], have: int, needed: int,
                        deadline: float
                        ) -> tuple[list[tuple[str, Any]], dict[asyncio.Task, str]]:
        """Wait until ``have`` + successful answers reaches ``needed``, it no longer can, or the
        deadline passes. A task's result is None for "no answer". Returns (node, result) for each
        success in arrival order, and the tasks still running (task -> node). They are not
        cancelled; the caller decides who waits for them."""
        successes: list[tuple[str, Any]] = []
        pending = set(tasks)
        while have + len(successes) < needed:
            if have + len(successes) + len(pending) < needed:
                break                                    # fail early: cannot reach `needed`
            left = deadline - self._now()
            if left <= 0:
                break                                    # budget used up
            done, pending = await asyncio.wait(pending, timeout=left,
                                               return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task.result() is not None:
                    successes.append((tasks[task], task.result()))
        return successes, {task: tasks[task] for task in pending}

    async def _replica_put(self, node_id: str, key: str, version: Version, deadline: float,
                           hint_for: str = "") -> bool | None:
        """One ReplicaPut (a hint for ``hint_for`` when set). True on an ack, None on any
        failure (never raises)."""
        timeout = self._rpc_timeout(deadline)
        if timeout <= 0:
            return None
        request = ReplicaPutRequest(sender=self.node_id, key=key, version=version_to_proto(version),
                                    hint_for=hint_for)
        try:
            reply = await self.peers.stub(node_id).ReplicaPut(
                request, timeout=timeout, metadata=reqid.outgoing())
        except grpc.aio.AioRpcError as error:
            logger.warning("ReplicaPut %s to %s failed: %s", key, node_id, error.code().name)
            return None
        return True if reply.ok else None

    async def _replica_get(self, node_id: str, key: str, deadline: float) -> list[Version] | None:
        """One ReplicaGet. The node's versions ([] = not found), None on any failure."""
        timeout = self._rpc_timeout(deadline)
        if timeout <= 0:
            return None
        request = ReplicaGetRequest(sender=self.node_id, key=key)
        try:
            reply = await self.peers.stub(node_id).ReplicaGet(
                request, timeout=timeout, metadata=reqid.outgoing())
        except grpc.aio.AioRpcError as error:
            logger.warning("ReplicaGet %s from %s failed: %s", key, node_id, error.code().name)
            return None
        return [version_from_proto(v) for v in reply.versions]

    # ----- read repair (D7) -----

    async def _repair(self, key: str, local: list[Version],
                      answers: list[tuple[str, list[Version]]],
                      late: dict[asyncio.Task, str]) -> None:
        """Background, after a Get: wait for the late ReplicaGet answers (each ends within its
        RPC timeout), reconcile everything, and send every node that answered the versions it
        is missing. Best-effort: failures are logged by ``_replica_put`` and never retried."""
        if late:
            done, _ = await asyncio.wait(late)
            answers += [(late[task], task.result()) for task in done if task.result() is not None]
        holdings = {self.node_id: local, **dict(answers)}
        reconciled = reconcile([v for held in holdings.values() for v in held])
        plan = repair_plan(reconciled, holdings)
        await asyncio.gather(*(self._repair_node(key, node_id, missing)
                               for node_id, missing in plan.items()))

    async def _repair_node(self, key: str, node_id: str, missing: list[Version]) -> None:
        """Send one node its missing versions, one ReplicaPut each (or apply them locally when
        the node is this one). Skipped if a repair of the same (key, node) is already running."""
        if (key, node_id) in self._repairing:
            return
        self._repairing.add((key, node_id))
        try:
            logger.info("read repair: %d version(s) of %s to %s", len(missing), key, node_id)
            for version in missing:
                if node_id == self.node_id:
                    async with self._key_lock(key):
                        await self.storage.apply(key, version)
                else:
                    deadline = self._deadline(self.config.rpc_timeout_ms / 1000)
                    await self._replica_put(node_id, key, version, deadline)
        finally:
            self._repairing.discard((key, node_id))

    async def _forward(self, order: list[str], req: ForwardRequest,
                       budget: float) -> ForwardResponse | None:
        """Try each node in order until one coordinates. Reaching this node itself means
        coordinate here. Moves on only after UNAVAILABLE or DEADLINE_EXCEEDED; any other gRPC
        error is raised (the client gets INTERNAL). None when every attempt failed."""
        deadline = self._deadline(budget)
        for node_id in order:
            left = deadline - self._now()
            if left <= 0:
                break
            if node_id == self.node_id:
                return await self.coordinate(req, left)
            try:
                return await self.peers.stub(node_id).Forward(
                    req, timeout=left, metadata=reqid.outgoing())
            except grpc.aio.AioRpcError as error:
                if error.code() not in RETRY_CODES:
                    raise
                logger.warning("Forward to %s failed (%s), trying the next replica",
                               node_id, error.code().name)
        return None

    # ----- small helpers -----

    def _replicas(self, key: str) -> list[str]:
        """The key's N replicas (preference list)."""
        return self.ring.preference_list(key, self.config.n)

    def _route_order(self, replicas: list[str]) -> list[str]:
        """ALIVE, then SUSPECT, then DEAD; list order inside each (sorted() is stable)."""
        return sorted(replicas, key=lambda node_id: STATE_RANK.get(self.membership.state(node_id), 2))

    def _reachable(self, replicas: list[str]) -> list[str]:
        """The replicas worth a replica call: all but those this node sees as DEAD.
        SUSPECT ones are still asked (SUSPECT can flap on a healthy cluster)."""
        return [node_id for node_id in replicas if self.membership.state(node_id) != DEAD]

    def _alive_count(self, replicas: list[str]) -> int:
        """How many of the replicas this node believes are ALIVE."""
        return sum(1 for node_id in replicas if self.membership.state(node_id) == ALIVE)

    def _now(self) -> float:
        return asyncio.get_running_loop().time()

    def _deadline(self, budget: float) -> float:
        return self._now() + budget

    def _rpc_timeout(self, deadline: float) -> float:
        """Timeout for one replica call: ``rpc_timeout_ms``, but never past the deadline."""
        return min(self.config.rpc_timeout_ms / 1000, deadline - self._now())

    def _key_lock(self, key: str) -> asyncio.Lock:
        """The lock that serializes clock counters for one key."""
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    def _spawn(self, coro: Coroutine[Any, Any, Any]) -> None:
        """Run ``coro`` in the background and keep track of it until it finishes."""
        self._track(asyncio.create_task(coro))

    def _track(self, task: asyncio.Task) -> None:
        """Keep a background task in the task set until it finishes (``drain`` waits for it)."""
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

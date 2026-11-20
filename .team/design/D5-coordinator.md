# D5 — Coordinator: forwarding + quorum

Owner: Meet · Reviewer: Prateek · Status: DESIGNED · Date: 2026-11-21

> Confirmed by Meet on 2026-11-21 as written. No shared file changes. Many answers
> were already fixed in D0-M (Q1 routing, Q4 errors and time budget, Q10 counter rule, Q11
> NOT_FOUND context); they are repeated here so this doc stands on its own. Uses Prateek's D3
> (`vclock`, `version.reconcile`, `context_of`, `parse_context`, DEC-11).

## 1. Requirement
From TASKS.md: the Put / Get / Delete flows from the SPEC: forward to the first live node of the
preference list, write locally (1 ack), fan out `ReplicaPut` / `ReplicaGet`, return after W / R
replies, `QUORUM_FAILED` on timeout, merge reads with `reconcile`. Delete = put of a tombstone.
**Done when:** in-process tests: write via node1 / read via node4; 1 node down with N=3 R=2 W=2
still works; 2 replicas down → `QUORUM_FAILED`; Phase 2 demo in Docker.

## 2. Chosen approach (plain words)
Any node can receive a client request. It looks up the key's 3 replicas on its own ring. A write is
coordinated by the first replica (the receiving node forwards to it if it is not that node); a read
is coordinated by whichever replica received it, or forwarded to the first replica if the receiving
node is not a replica. The coordinator writes (or reads) its own copy first, which counts as one
answer, then asks the other replicas in parallel and returns as soon as W (or R) answers are in. It
gives up early if there are no longer enough replicas left that could still answer. Replies that
arrive later are not cancelled: they finish in the background (late writes still land; late reads
are kept for read repair in D7). Every call to another node has its own short timeout inside one
overall time budget per request.

### Decisions (confirmed)

**Q1. Who coordinates?** (D0-M Q1)
- Put / Delete: the **first replica** in the route order. Get: **any replica** that received it.
- Route order = the preference list sorted ALIVE, then SUSPECT, then DEAD (list order inside each).
  `membership.state()` already exists, and every node is ALIVE until gossip arrives in D6, so this
  is built now and simply starts to matter in D6/D7.
- Why first replica for writes: only coordinators add clock entries, so one coordinator per key
  keeps clocks small. Reads add no entries, so the extra hop is skipped.

**Q2. Forward loops** (D0-M Q1)
- The receiver of a `Forward` **always coordinates it and never forwards again**, even if its own
  ring says another node should. No hop count, no flag in the proto.

**Q3. Quorum wait loop**
```
acks = 1 (local write done) ; answered = [self] ; pending = one task per other replica
while acks < W:
    if acks + len(pending) < W:        break            # fail early: W can no longer be reached
    left = deadline - now
    if left <= 0:                      break            # budget used up
    done, pending = await asyncio.wait(pending, timeout=left, return_when=FIRST_COMPLETED)
    for task in done:
        if task.result() is an ok reply:  acks += 1 ; answered.append(node)
        # an RPC error or ok=False is "no ack" (logged)
status = OK if acks >= W else QUORUM_FAILED
hand every task still in `pending` to the background (Q4)
```
- Reads use the same loop with R, collecting each reply's versions; a replica that answers with
  no versions ("not found") **counts toward R**.
- Each replica call is wrapped in a helper that catches `grpc.aio.AioRpcError` and returns `None`,
  so a task never ends with an unhandled exception.
- **Fast fail** (SPEC edge case): if fewer than W (or R) of the N replicas are ALIVE in this node's
  view, answer `QUORUM_FAILED` at once, without waiting for timeouts. Until D6 everyone is ALIVE.

**Q4. Stragglers**
- Writes: tasks still pending after W acks are **not cancelled**; `_spawn` keeps them in the
  coordinator's task set, they finish on their own (bounded by their RPC timeout), a failure is
  logged. `drain()` at shutdown waits for them.
- Reads: same, and the late replies are kept with the request so D7 can repair stale replicas.
  In D5 they are only logged at DEBUG.

**Q5. Local write first, then fan out**
- Under the key's lock: build the clock (DEC-02 rule), `storage.apply` locally (WAL + fsync) = ack 1.
  Release the lock, then send `ReplicaPut` to the other replicas in parallel.
- Why: the lock must cover the local write so the next counter sees it; sending to replicas does
  not need the lock. Cost: one fsync (a few ms) before the fan-out. A local `apply` that returns
  False (already stored) still counts as an ack.

**Q6. Time limits** (D0-M Q4)
- Overall budget = `min(request_timeout_ms, time left on the caller's deadline)` (`request_budget`).
- Each replica call: timeout = `min(rpc_timeout_ms, time left in the budget)`.
- A `Forward` call: timeout = the time left in the budget (the coordinator needs it all).

**Q7. Read result**
- All versions from the local read and the R replies go through `version.reconcile`.
- No live version left (unknown key, or only tombstones) → `NOT_FOUND`, still with
  `context_of(all reconciled versions)`. Otherwise `OK`, the live values (equal values once,
  DEC-11), and the context. `read_from` = the nodes that answered before the reply.

**Q8. Forward fails**
- Try the next node in the route order, but only after `UNAVAILABLE` or `DEADLINE_EXCEEDED`
  (D0-M 3.1). If the next node is this node, coordinate locally. Every attempt failed →
  `QUORUM_FAILED`. A body status from the coordinator (`QUORUM_FAILED`, `INVALID`, `NOT_FOUND`) is
  passed back unchanged.

**Q9. Only the N preferred replicas in D5**
- The coordinator sends only to the key's N replicas, DEAD ones included (they just fail). Picking a
  stand-in node with `hint_for` is D8.

**Q10. Phase 2 demo**
- `tools/demo_phase2.sh`, on a running 5-node cluster (local script by default, `DOCKER=1` for
  Docker): put via node1, get via node4, show the replicas with `kv locate`, kill one replica →
  put and get still work, kill a second replica → `QUORUM FAILED` (exit code 4), restart both.

## 3. Alternatives we rejected
| Option | Why not |
|---|---|
| Any replica coordinates writes | Clock entries come only from coordinators; more coordinators = bigger clocks |
| Hop count / "forwarded" flag | Receiving a `Forward` is already the signal |
| Wait for all N replies | One slow or dead replica would slow every request; that is what W / R are for |
| Cancel pending replica calls after W acks | The third replica would often miss the write; late reads are needed for read repair |
| Local write and fan-out in parallel | The per-key lock would have to cover the network calls, or the next counter could miss the local write |
| One timeout for the whole request only | A single dead replica would use up the whole budget before we notice |
| Retry Forward on any error | Only UNAVAILABLE / DEADLINE_EXCEEDED mean "no answer"; anything else is a real answer |

## 4. Interfaces and data structures
```python
# dynamo/coordinator.py (Meet)
async def put(self, req: PutRequest, budget: float) -> PutResponse        # route: coordinate here or _forward
async def get(self, req: GetRequest, budget: float) -> GetResponse
async def delete(self, req: DeleteRequest, budget: float) -> DeleteResponse
async def coordinate(self, req: ForwardRequest, budget: float) -> ForwardResponse   # always here

async def _do_put(self, req: PutRequest, budget: float) -> PutResponse     # local write + _replicate
async def _do_get(self, req: GetRequest, budget: float) -> GetResponse     # local read + _collect + reconcile
async def _do_delete(self, req: DeleteRequest, budget: float) -> DeleteResponse

async def _replicate(self, key: str, version: Version, others: list[str], w: int,
                     deadline: float) -> list[str]                          # nodes that acked (self included)
async def _collect(self, key: str, others: list[str], r: int,
                   deadline: float) -> tuple[list[str], list[Version]]      # nodes that answered, all versions
async def _replica_put(self, node_id: str, key: str, version: Version, timeout: float) -> bool
async def _replica_get(self, node_id: str, key: str, timeout: float) -> list[Version] | None
async def _forward(self, order: list[str], req: ForwardRequest, budget: float) -> ForwardResponse | None
def _route_order(self, pref: list[str]) -> list[str]                        # ALIVE, SUSPECT, DEAD
def _alive_count(self, pref: list[str]) -> int                              # for the fast fail
```
- Every outgoing call carries `reqid.outgoing()` metadata.
- `ForwardRequest(sender=self, put|get|delete=...)`; the response's oneof is relayed unchanged.

## 5. Flow
**Put(key, value, context) to node X**
1. `pref = ring.preference_list(key, N)`; `order = _route_order(pref)`.
2. X is `order[0]` → coordinate here. Else `_forward(order, ...)`.
3. Coordinator: W = `req.w or config.w`; fast fail if `_alive_count(pref) < W`.
4. Parse the context (bad → `INVALID`); under the key's lock: new clock, `storage.apply` (ack 1).
5. `_replicate` to the other N−1 replicas (Q3); return `OK` + `context_of([version])` +
   `written_to`, or `QUORUM_FAILED`.

**Get(key) to node X**
1. X in `pref` → coordinate here. Else `_forward(order, ...)`.
2. R = `req.r or config.r`; fast fail; local `storage.get` (reply 1); `_collect` from the other
   replicas until R replies; `reconcile`; build the reply (Q7).

**Delete** = Put of `Version(tombstone=True)`; reply has only the status.

## 6. Edge cases
| Case | What happens | Test |
|---|---|---|
| Put arrives at replica 2 or 3, or at a non-replica | Forwarded to the first replica | `test_write_forwarded_to_first_replica` |
| Get arrives at replica 2 or 3 | Coordinated there, no extra hop | `test_get_no_extra_hop` |
| Get arrives at a non-replica | Forwarded to the first replica | `test_get_forwarded_from_non_replica` |
| First replica down when forwarding | Next replica coordinates | `test_forward_fallback` |
| Next node in the order is the receiving node | It coordinates locally | `test_forward_reaches_self` |
| Forward arrives at a node whose ring disagrees | It coordinates anyway | `test_forward_never_reforwarded` |
| One replica down, N=3 R=2 W=2 | Put and Get succeed | `test_one_replica_down_still_works` |
| Two replicas down | `QUORUM_FAILED` | `test_two_replicas_down_quorum_failed` |
| W = 1 | Answer right after the local write; others finish in the background | `test_w1_returns_after_local_write` |
| W = 3 with one replica down | `QUORUM_FAILED`; the 2 replicas that wrote keep it (no rollback) | `test_w3_one_down` |
| Fewer ALIVE replicas than W (gossip view) | `QUORUM_FAILED` at once | `test_fast_fail_when_not_enough_alive` |
| Third replica answers after the reply | Its write lands; no unhandled exception | `test_late_replica_write_lands` |
| Replica answers "not found" | Counts toward R | `test_not_found_counts_toward_r` |
| Two replicas hold concurrent versions | Get returns both values as siblings | `test_get_returns_siblings` |
| Get on a deleted key | `NOT_FOUND` with the tombstone's context | `test_get_after_delete_cluster` |
| Two puts with the same context through one coordinator | Different counters | `test_same_context_different_counters` |
| Bad context | `INVALID`, nothing sent to replicas | `test_put_bad_context_invalid` |
| Client deadline shorter than `request_timeout_ms` | Budget = time left; replica calls get only what remains | `test_budget_from_deadline` |
| Node stopped while background replica calls run | `drain()` waits up to 2 s, then cancels | `test_drain_on_stop` |

## 7. Tests
- Unit (`tests/test_coordinator.py`): the quorum loop with fake replica calls (acks, failures, fast
  fail, deadline), route order, read result building.
- In-process cluster (`tests/test_cluster_requests.py`): 5 real nodes on free ports (own small
  fixture until `conftest.py` has the shared one): every row of section 6 that needs real nodes;
  "down" = `node.stop()`.
- Phase 2 demo: `tools/demo_phase2.sh` on the local cluster and in Docker.

## 8. Interview questions on this part
1. Q: Why does the coordinator not wait for all 3 replicas?  A: W = 2 is enough for R + W > N;
   waiting for the third would make every request as slow as the slowest replica.
2. Q: What happens to the third replica's write after you answered?  A: It keeps running in the
   background with its own timeout; if it fails, read repair (D7) or hinted handoff (D9) fixes it.
3. Q: How do you fail fast?  A: If even all still-pending replicas answering could not reach W, stop
   waiting; and if gossip says fewer than W replicas are alive, answer at once.
4. Q: Why write locally before sending to replicas?  A: The per-key lock must cover the local
   write so the next counter sees it; holding it during network calls would block the key.
5. Q: Does a "not found" from a replica count toward R?  A: Yes. It is a real answer; that replica
   simply has no version.

## 9. As built
_Built 2026-11-21 as designed. Phase 2 demo passes on the local 5-process cluster; Docker run pending (`make up && DOCKER=1 tools/demo_phase2.sh`)._
- `coordinator.py`: `put/get/delete` route with `_route_order` / `_forward`; `coordinate` dispatches the Forward's oneof; the write path shared by Put and Delete is `_write` (parse context → fast fail → `_write_local` → `_replicate`); reads are `_do_get` (local read → `_collect` → `reconcile`). The quorum loop of Q3 is one helper, `_wait_for(tasks, have, needed, deadline)`, used by both `_replicate` and `_collect`. `_replica_put` / `_replica_get` catch `AioRpcError` and return None, so no task ends with an exception; still-running tasks go to `_track` (the set `drain` waits for).
- New `dynamo/wire.py` with `version_to_proto` / `version_from_proto`, moved out of `replica.py`: the coordinator needs them and `replica.py` already imports the coordinator (import cycle).
- `_forward` re-raises a gRPC error that is not UNAVAILABLE / DEADLINE_EXCEEDED, so the client gets INTERNAL instead of a hidden failure. A forward that timed out leaves no budget for the next node, so the fallback mainly helps when the first replica is down (UNAVAILABLE comes back at once).
- `read_from` can list more than R nodes when several answers arrive in the same wake-up; the reply uses all of them.
- A failed Put returns an empty context.
- After a peer restarts, calls to it fail fast for up to about 1 s (the 1 s reconnect backoff from D0-M); the demo retries its last put for that reason. Gossip (D6) will make this visible as SUSPECT.
- Found by the demo: the CLI crashed on an empty or broken context file. Now an empty file means no contexts and a broken one is reported on stderr and ignored (next save rewrites it).
- Tests: 27 unit tests in `test_coordinator.py` (quorum loop, fast fail, route order, deadline, single node) and 16 in `test_cluster_requests.py` on 5 real nodes (every routing / quorum / version row of section 6). Suite: 244 passed, 1 skipped.
- `tools/demo_phase2.sh` (local cluster by default, `DOCKER=1` for Docker): prints `PHASE 2 DEMO PASSED`.

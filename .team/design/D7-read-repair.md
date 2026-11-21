# D7 — Read repair + membership-aware routing

Owner: Meet · Reviewer: Prateek · Status: REVIEW · Date: 2026-11-21

> Confirmed by Meet on 2026-11-21 (Q5: only ALIVE counts for fast fail). Builds on D5
> (routing, quorum loop) and Prateek's D6a (real ALIVE / SUSPECT / DEAD states). D0-M Q2 already
> fixed the read-repair basics (background, best-effort, one ReplicaPut per version, late replies
> included); they are repeated here.

## 1. Requirement
From TASKS.md: after a Get, push the reconciled version(s) to replicas that were missing them or
behind, in the background. Coordinator skips DEAD nodes; fast `QUORUM_FAILED` when too few nodes
are alive.
**Done when:** a test makes one replica stale, does a Get, and sees the replica updated; DEAD-skip
test; Phase 3 demo.

## 2. Chosen approach (plain words)
The coordinator answers the client as soon as it has R replies, exactly as in D5. Then, in the
background, it waits a little longer for the replicas that had not answered yet (each one still
bounded by its 500 ms call timeout), so it knows what **every** answering replica holds. It
reconciles all of it into the newest set of versions and sends each replica only the versions it is
missing, one `ReplicaPut` per version. The coordinator repairs its own storage the same way. A
replica that never answered is not repaired (we do not know what it has; hinted handoff and
anti-entropy in D6b/6c cover it).

Routing now uses the real gossip states from D6a: DEAD replicas are no longer sent replica calls at
all, SUSPECT ones still are, and forwarding tries ALIVE nodes first and DEAD ones last.

### Decisions

**Q1. When to repair**
- After the reply, in the background (SPEC, D0-M Q2). The client never waits for repair.
- Replicas whose answer arrives **after** the reply are repaired too: the repair task waits for the
  still-running `ReplicaGet` calls (each already limited by `rpc_timeout_ms`), then repairs.
- Best-effort: a failed repair `ReplicaPut` is logged at WARNING and never retried.

**Q2. What "stale" means**
- Reconciled set = `reconcile(local + every answer)` (Prateek's D3).
- For each answering replica (and the coordinator itself): it is missing version `v` if none of
  its versions has a clock EQUAL to `v`'s (`version.clock_key`). It gets exactly the missing
  versions. Older versions it still holds disappear on its side when the newer one arrives
  (`merge_into` drops what is BEFORE).
- Tombstones are repaired like any other version.

**Q3. Hot key read many times while one replica is behind**
- Repair sends only what is missing, so once a replica has the versions, later reads send nothing.
- While a repair of `(key, node)` is in flight, a new read does not start a second one: the
  coordinator keeps a set `_repairing` of `(key, node)` pairs. A duplicate that still slips through
  is harmless: `storage.apply` returns False and writes no log line for it (DEC-11).

**Q4. SUSPECT nodes**
- Repair any node that **answered** the read, whatever its state: it just proved it is reachable.
  A node that did not answer is never repaired.

**Q5. DEAD nodes and the fast-fail count**
- Replica fan-out (`ReplicaPut` / `ReplicaGet`) skips replicas this node sees as DEAD. SUSPECT ones
  are still asked (SUSPECT can flap on a healthy cluster, DEC-13).
- Fast fail: answer `QUORUM_FAILED` at once if the number of **ALIVE** replicas among the N
  preferred is below W (or R), as in D5 (Meet's decision, 2026-11-21). Stricter: a request is
  rejected at once when SUSPECT replicas would be needed to reach the quorum, instead of trying them.
- Forwarding keeps the D5 order ALIVE → SUSPECT → DEAD; DEAD nodes are still tried last, so a wrong
  gossip view costs at most one timeout, never a failed request.
- D8 changes the count: with `sloppy_quorum` on, stand-in nodes found by `ring.walk` past the N
  preferred ones count too.

**Q6. Phase 3 demo** — `tools/demo_phase3.sh` (local cluster, `DOCKER=1` for Docker)
1. **Failure detection:** kill a node; `kv info` on another node shows it SUSPECT after ~5 s and
   DEAD after ~10 s; start it again → ALIVE.
2. **Read repair:** kill replica 3 of `cart:42`; put a new value (W=2 still met); start replica 3
   again; `kv local-get` on it shows the old value; `kv get` via any node; `kv local-get` on
   replica 3 now shows the new value.
3. **Siblings:** two clients read `cart:42` (same context). Client A writes `x`. The first replica is
   killed; client B writes `y` with the old context, coordinated by replica 2. First replica back;
   `kv get` shows `[1] x  [2] y` and "2 siblings"; the client puts `xy` with that context → one value.
   - Why the kill: two writes with the same context through the **same** coordinator get counters
     n and n+1 and look ordered (D0-M limitation 6.1; dotted version vectors would fix it). Through
     two different coordinators the clocks are really concurrent.

## 3. Alternatives we rejected
| Option | Why not |
|---|---|
| Repair before replying | Every read with one stale replica would wait for a write; SPEC says background |
| Repair only replicas that answered before the reply | The third replica is the one most often stale; waiting for it in the background costs the client nothing |
| Send the whole reconciled set to every stale replica | More writes for nothing; the replica already has some of the versions |
| Repair replicas that did not answer | We do not know what they hold; D6b hinted handoff and 6c anti-entropy cover them |
| Retry failed repairs | Best-effort is enough; anti-entropy (6c) repairs what read repair misses |
| Skip SUSPECT replicas in the fan-out | SUSPECT flaps on a healthy cluster; skipping them would make W/R fail for no reason |
| Rebuild the ring without DEAD nodes | D4 Q1: ownership never changes; DEAD only changes routing |
| Fast fail counts ALIVE + SUSPECT | Rejected by Meet: keep the D5 rule (only ALIVE counts); fail sooner rather than spend the budget on nodes that may be gone |

## 4. Interfaces and data structures
```python
# dynamo/coordinator.py (Meet)
def _repair_plan(reconciled: list[Version],
                 holdings: dict[str, list[Version]]) -> dict[str, list[Version]]
    # pure: node -> the reconciled versions it is missing (nodes missing nothing are left out)
async def _repair(self, key: str, local: list[Version], answers: list[tuple[str, list[Version]]],
                  late: dict[asyncio.Task, str]) -> None
    # background: wait for `late`, reconcile everything, apply the plan (local apply + ReplicaPut)
def _reachable(self, replicas: list[str]) -> list[str]     # replicas not DEAD (fan-out targets)
self._repairing: set[tuple[str, str]]                       # (key, node) repairs in flight
```
- `_collect` changes: it returns the still-running tasks too, so `_do_get` can hand them to
  `_repair` instead of `_track`.
- `_wait_for` gets a flag (or a sibling helper) to return the pending tasks instead of tracking them.

## 5. Flow
1. `_do_get`: fast fail; local read; `_collect` from the reachable other replicas until R.
2. Build and send the reply (D5).
3. `_spawn(_repair(key, local, answers, late))`:
   1. wait for the `late` tasks (each ends within its RPC timeout);
   2. `reconciled = reconcile(local + all answers)`;
   3. `plan = _repair_plan(reconciled, {self: local, node: versions, ...})`;
   4. for the coordinator: `storage.apply` each missing version; for each other node not already in
      `_repairing`: one `ReplicaPut` per missing version, then remove it from `_repairing`.

## 6. Edge cases
| Case | What happens | Test |
|---|---|---|
| One replica stale, Get | Replica gets the new version in the background | `test_stale_replica_repaired` |
| Replica answered "not found" | Gets every reconciled version | `test_missing_key_repaired` |
| Third replica answers after the reply | Still repaired | `test_late_reply_repaired` |
| Coordinator itself is stale | Its own storage is repaired | `test_coordinator_repairs_itself` |
| Deleted key, one replica still has the value | Tombstone is sent; replica shows nothing live | `test_tombstone_repaired` |
| Two concurrent versions on different replicas | Both get both siblings | `test_siblings_spread_by_repair` |
| All replicas up to date | No repair write at all | `test_no_repair_when_in_sync` (unit) |
| Same key read many times while repair runs | One repair per (key, node) in flight | `test_repair_not_duplicated` (unit) |
| Repair ReplicaPut fails | Logged, not retried | `test_repair_failure_logged` (unit) |
| Replica DEAD in this node's view | Not sent replica calls; request still succeeds with the others | `test_dead_replica_skipped` |
| Fewer usable replicas than W / R | `QUORUM_FAILED` at once | D5 `test_fast_fail_when_not_enough_alive` |
| First replica DEAD, write arrives elsewhere | Forward goes to an ALIVE replica first (no wasted timeout) | `test_forward_skips_dead_first_replica` |

## 7. Tests
- Unit (`tests/test_coordinator.py`): `_repair_plan` (missing, equal, siblings, tombstones,
  nothing to do), in-flight dedupe, failure logging, `_reachable`.
- In-process cluster (`tests/test_read_repair.py`, Prateek's shared `cluster` fixture from
  `conftest.py`): every cluster row of section 6. "Stale" is made by killing a replica, writing,
  restarting it; "DEAD" by killing a node and `cluster.wait_until` its state is DEAD.
- Phase 3 demo: `tools/demo_phase3.sh` on the local cluster and in Docker.

## 8. Interview questions on this part
1. Q: Why repair after the reply?  A: The client already has a correct answer from R replicas; a
   stale replica is a background problem, so it should not slow the read.
2. Q: How do you know a replica is stale?  A: Reconcile all answers; a replica is missing every
   reconciled version for which it has no version with an equal clock. It gets exactly those.
3. Q: What stops a hot key from causing thousands of repair writes?  A: Only missing versions are
   sent, so a repaired replica gets nothing more; and one repair per (key, node) runs at a time.
4. Q: Why can't you show siblings by writing twice through the same node?  A: The coordinator's
   counter rule gives the second write n+1, so it looks newer; plain vector clocks cannot tell. Dotted
   version vectors fix that; we show siblings with two different coordinators instead.
5. Q: What does read repair not fix?  A: Keys nobody reads, and replicas that did not answer.
   That is what hinted handoff (6b) and Merkle anti-entropy (6c) are for.

## 9. As built
Built 2026-11-21 in `dynamo/coordinator.py`, `tests/test_coordinator.py`,
`tests/test_read_repair.py`, `tools/demo_phase3.sh`.

**Small deviations from section 4/5**
- `_repair_plan` became the module-level function `repair_plan(reconciled, holdings)`: it is pure
  and needs nothing from the coordinator, so it is tested without building one.
- One extra helper `_repair_node(key, node, missing)`: the per-node part (the `_repairing` check,
  then one `ReplicaPut` per version, or a local `storage.apply` for this node). `_repair` runs it
  for all nodes of the plan in parallel with `asyncio.gather`.
- `_wait_for` now always returns `(answers, late)`, where `late` is {task: node} of the calls still
  running; it no longer tracks them itself. `_replicate` tracks its late tasks as before (D5 Q4);
  `_collect` hands them to `_repair`.
- The repair task is started right after `_collect`, also when the read ends in `QUORUM_FAILED`
  (what did answer can still be repaired, and the late calls need someone to wait for them). It
  still runs only after the reply is sent: it is a background task and the reply needs no await.
- A local repair applies under the key's lock, the same lock `_write_local` holds while it picks
  the next counter, so a repair cannot slip between "read stored versions" and "store new version".
- Every repair `ReplicaPut` gets its own `rpc_timeout_ms` (500 ms); it is not tied to the client's
  request deadline, which has already passed or almost passed by then.
- Fast fail counts only ALIVE (Meet's Q5 decision); the code is the D5 `_alive_count`, unchanged.
  Fan-out uses the new `_reachable` (not DEAD) for both `_write` and `_do_get`.

**Tests:** 12 new unit tests (4 `repair_plan`, 5 `_repair`, `_reachable`, DEAD skip on put and get,
fast fail counts only ALIVE) and the 4 `_wait_for` tests adapted; 8 cluster tests in
`tests/test_read_repair.py` (every cluster row of section 6). Full suite: **313 passed, 1 skipped**
(the skip is Prateek's `STORAGE_TIMING` benchmark). Checked that the tests really test the code:
with the repair call removed, all 6 repair cluster tests fail; with `_reachable` removed, both
DEAD-skip tests fail.

**Phase 3 demo (local cluster, 5 processes):** PHASE 3 DEMO PASSED. node1 saw the killed node3 as
SUSPECT ~5 s and DEAD ~11 s after the kill, ALIVE ~2 s after the restart; node3 held `book`, one
`get` later it held `pen` (log: `read repair: 1 version(s) of cart:42 to node3`); siblings `x`,
`y` shown and merged into `xy`. Docker run (`DOCKER=1 tools/demo_phase3.sh`) PASSED too (Meet, 2026-11-21, after the step-2 change from D8 §9).

**Known limits**
- Right after a restart the coordinator may still see the node as DEAD (until gossip says ALIVE,
  ~2 s) or its channel may still be in the 1 s reconnect backoff; reads in that window do not
  repair it. The next read does. Tests and the demo read again until the repair shows up.
- A replica that is never read stays stale until D6b / 6c.

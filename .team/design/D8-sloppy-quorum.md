# D8 — Sloppy quorum

Owner: Meet · Reviewer: Prateek · Status: REVIEW · Date: 2026-11-21

> Confirmed by Meet on 2026-11-21 (Q3 strict reads, Q5 SUSPECT owners get a stand-in). Builds on D5
> (coordinator), D7 (DEAD-aware fan-out, fast fail counts only ALIVE) and D6b (stand-ins keep
> hints and hand them back; hints are not visible to reads). Only `dynamo/coordinator.py` and tests
> change; no proto or config change (`sloppy_quorum` and `hint_for` already exist).

## 1. Requirement
From TASKS.md: when a preferred replica is DEAD, walk the ring past the top N and use the next
healthy node as a stand-in, sending `ReplicaPut` with `hint_for = <dead owner>`. A config flag
turns sloppy quorum on/off (cluster tests 3 and 4).
**Done when:** cluster test 3 (flag off, 2 of 3 replicas down → `QUORUM_FAILED`) and test 4 (flag
on → write succeeds; hint delivered after restart) pass in-process.

## 2. Chosen approach (plain words)
A write is still coordinated by an ALIVE owner, exactly as in D5. Before replicating, the
coordinator looks at the other owners: each one this node does not see as **ALIVE** (SUSPECT or
DEAD) is replaced by a stand-in, the next node clockwise past the N owners (`ring.walk`) that is ALIVE and not already
used. The stand-in gets the same version with `hint_for = <that owner>` and keeps it as a hint
(D6b); its ack counts toward W. When the owner comes back, the stand-in hands the write over.
Reads do not change: they only use the owners.

### Decisions

**Q1. Choosing stand-ins**
- `ring.walk(key)` yields every node once, clockwise; the first N are the owners. Stand-ins are
  taken from what follows, in that order, skipping nodes that are not ALIVE in this node's view.
  So a stand-in is never one of the N owners and never the coordinator (an owner).
- Chosen once, before the writes go out. If a stand-in then fails to ack, no second stand-in is
  tried for that owner (the write may still reach W with the others).

**Q2. Two owners down (DEAD or SUSPECT, Q5)**
- Two stand-ins, paired in order: the first non-ALIVE owner (preference order) gets the first stand-in,
  the second gets the next. Each `ReplicaPut` names its own owner in `hint_for`.
- Fewer ALIVE non-owners than non-ALIVE owners → only as many stand-ins as there are.

**Q3. Reads** *(must match D6b Q6: hints are not visible to reads)*
- **Reads stay strict** (recommended; the Dynamo way only makes sense if stand-ins also return
  their hinted data, which D6b Q6 ruled out). Gets ask only the owners; fast fail for reads stays "ALIVE owners
  < R". With 2 of 3 owners DEAD a write succeeds but a read with R=2 answers `QUORUM_FAILED`
  (a read with `--r 1` works).
- Why: a stand-in holds the key only as a hint, which it does not return, so its answer would be
  "not found". Counting that empty answer toward R would make R=2 mean "one real copy" and hide
  that from the client.
- **Alternative (Dynamo's way):** reads go to the first N healthy nodes, stand-ins included, and
  their (empty) answers count toward R. Reads stay available, but R no longer means R real copies.

**Q4. A hinted ack counts toward W**
- Yes (SPEC). `written_to` lists the stand-in's node id like any other.
- Durability cost (interview point): with 2 owners DEAD, the write is on the coordinator plus one
  stand-in that holds it as a hint. If that stand-in dies for good before handing it over, only the
  coordinator has the write; 6c anti-entropy can only copy it from there. We accept this: it is the
  price of "always writeable".

**Q5. Owner SUSPECT, not DEAD** — *Meet's decision (2026-11-21)*
- With the flag on, a **SUSPECT owner gets a stand-in too**, exactly like a DEAD one: the write goes
  to the stand-in (with `hint_for = <that owner>`) instead of to the owner. When the owner is ALIVE
  again, the stand-in hands the update over (D6b wakes on "turned ALIVE").
- Why: it matches the D7 rule that only ALIVE counts. A write never waits on, or fails fast
  because of, an owner that may be down; there is no ~5 s gap between SUSPECT and DEAD where a
  crashed owner neither counts nor has a stand-in.
- Fast fail with the flag on: `QUORUM_FAILED` at once if
  `ALIVE owners + stand-ins found (one per non-ALIVE owner) < W`.
- Cost, stated honestly: SUSPECT can flap on a healthy cluster; each flap during a write leaves a
  hint, which D6b delivers moments later (harmless, the owner ignores duplicates). Meanwhile that
  owner misses the update until the hint arrives.
- Flag off: unchanged D7 behaviour (SUSPECT owners are still sent the write, DEAD ones skipped).

**Q6. At least one ALIVE owner is needed**
- The coordinator is always an owner (D5: the first ALIVE replica coordinates; others forward).
  With all 3 owners DEAD the request fails (`QUORUM_FAILED`), even with the flag on. Dynamo would use
  3 stand-ins; we keep "coordinator = owner" so the write path stays the D5 one. With N=3, W=2 this
  still covers the spec case "2 of 3 replicas down".

**Q7. Flag off**
- `sloppy_quorum: false` → no stand-ins at all; behaviour is exactly D7.

**Q8. Demo** — `tools/demo_sloppy.sh` (local cluster, `DOCKER=1` for Docker)
- Kill 2 owners of `cart:42`; wait until the coordinator sees them DEAD; `put -v` succeeds and
  `written_to` shows a non-owner; `local-get` on the stand-in shows nothing (a hint, not data);
  start both owners; within ~5 s `local-get` on each owner shows the value.

## 3. Alternatives we rejected
| Option | Why not |
|---|---|
| Rebuild the preference list without DEAD nodes | D4 Q1: ownership never changes; the stand-in must know whose data it holds (`hint_for`) |
| Try a second stand-in when the first fails | More code for a rare case; the stand-in was ALIVE a moment ago |
| Non-owner coordinator when all owners are DEAD | Changes the D5 write path (who builds the clock); not needed for N=3, W=2 with one failure domain |
| Reads from stand-ins (Q3 alternative) | Empty answers would count toward R (see Q3) |
| Stand-ins only for DEAD owners (SUSPECT still asked) | Rejected by Meet: a crashed owner would block writes for ~5 s until DEAD; with ALIVE-only counting, every non-ALIVE owner gets a stand-in |

## 4. Interfaces and data structures
```python
# dynamo/coordinator.py (Meet)
def _targets(self, key: str, replicas: list[str]) -> list[tuple[str, str]]
    # (node, hint_for) for the other copies of a write. Flag on: ALIVE owners with hint_for "",
    # plus one stand-in per non-ALIVE owner. Flag off: reachable owners (D7), no stand-ins
def _stand_ins(self, key: str, replicas: list[str], count: int) -> list[str]
    # the first `count` ALIVE nodes after the N owners in ring.walk(key)
async def _replica_put(self, node_id, key, version, deadline, hint_for: str = "") -> bool | None
async def _replicate(self, key, version, targets: list[tuple[str, str]], w, deadline) -> list[str]
```
- `_write`: fast fail uses `ALIVE owners + len(stand-ins)` when the flag is on (Q5); stand-ins
  replace every non-ALIVE owner.
- `_do_get`, `_repair`, `_forward`: unchanged.

## 5. Flow (write, flag on)
1. `put` / `delete`: route order, forward if needed (D5).
2. `_write` on the coordinator: `missing = owners this node does not see as ALIVE`;
   `stand_ins = _stand_ins(key, replicas, len(missing))`;
   if `alive owners + len(stand_ins) < w` → `QUORUM_FAILED` (nothing written).
3. Write locally (ack 1).
4. `_replicate` to: ALIVE other owners (`hint_for=""`) and each stand-in
   (`hint_for=<its non-ALIVE owner>`), until W acks.
5. Stand-in: `storage.put_hint` (D6b); later its handoff loop delivers to the owner.

## 6. Edge cases
| Case | What happens | Test |
|---|---|---|
| 2 of 3 owners DEAD, flag on | Write OK via 1 owner + 1 stand-in | `test_sloppy_write_succeeds` (cluster) |
| Same, flag off | `QUORUM_FAILED` at once | `test_strict_write_fails` (cluster) |
| Owners come back | Each gets the write from its stand-in within ~5 s | `test_hints_reach_owners_after_restart` (cluster) |
| 1 owner DEAD | One stand-in (gets a hint) + other owner | `test_one_dead_owner_one_stand_in` (unit) |
| 2 owners DEAD | Two stand-ins, each with its own `hint_for` | `test_two_dead_owners_two_stand_ins` (unit) |
| Stand-in candidates not ALIVE | Skipped; next one in ring order | `test_stand_in_skips_non_alive` (unit) |
| Not enough stand-ins | Fast fail if ALIVE owners + stand-ins < W | `test_fast_fail_counts_stand_ins` (unit) |
| Owner SUSPECT, flag on | Not sent the write; its stand-in gets it with `hint_for` | `test_suspect_owner_gets_a_stand_in` (unit) |
| Owner SUSPECT, flag off | Still sent the write (D7) | `test_flag_off_keeps_d7_routing` (unit) |
| All owners DEAD | `QUORUM_FAILED` | `test_all_owners_dead_fails` (unit) |
| Read with 2 owners DEAD (R=2) | `QUORUM_FAILED`; with `--r 1` OK | `test_reads_stay_strict` (cluster) |
| Stand-in's own reads | `local-get` on the stand-in: not found | in `test_sloppy_write_succeeds` |

## 7. Tests
- Unit (`tests/test_coordinator.py`, fake membership/peers from D7): `_stand_ins`, `_targets`,
  fast-fail counts, `hint_for` on the right `ReplicaPut`s.
- Cluster (`tests/test_sloppy_quorum.py`, `make_cluster(5)` and
  `make_cluster(5, sloppy_quorum=False)`): kill 2 owners, `wait_until` they are DEAD in the
  coordinator's view, put; restart, `wait_until` owners hold the value and stand-in hints are gone.
  These are SPEC cluster tests 3 and 4, in-process; D10 runs them against Docker.
- Demo: `tools/demo_sloppy.sh`.

## 8. Interview questions on this part
1. Q: What is sloppy quorum?  A: W and R are counted over the first N *healthy* nodes, not the
   fixed N owners; a stand-in holds a dead owner's write as a hint and hands it back later.
2. Q: Why does the stand-in need `hint_for`?  A: It is not an owner of the key; it must know whom
   to hand the write to, and must not treat it as its own data.
3. Q: What do you give up?  A: R + W > N no longer guarantees overlap: a read from the owners can
   miss a write that sits on a stand-in. Vector clocks keep both versions when they meet again.
4. Q: Why do reads stay strict here when Dynamo's do not?  A: Our stand-ins do not serve hints, so
   their answers would be empty; counting them toward R would hide that only one real copy answered.
5. Q: What if the stand-in dies before handing over?  A: The write is still on the coordinator;
   Merkle anti-entropy (6c) copies it to the owners. If both die, it is lost — W=2 promised two
   copies, and one of them was a hint.

## 9. As built
Built 2026-11-21 in `dynamo/coordinator.py`, `tests/test_coordinator.py`,
`tests/test_sloppy_quorum.py`, `tools/demo_sloppy.sh`; `tests/test_read_repair.py` and
`tools/demo_phase3.sh` adjusted.

**As designed:** `_targets(key, replicas)` and `_stand_ins(key, replicas, count)` exactly as in
section 4; `_replicate` takes `(node, hint_for)` targets; `_replica_put` got `hint_for=""`.
Fast fail in `_write` is `ALIVE owners + stand-ins < W`, which with the flag off is the D7 rule
(no stand-ins). Reads, read repair and forwarding are unchanged.

**Small points**
- With 2 owners down there are 2 stand-ins; `written_to` lists the coordinator plus whichever
  stand-in acked first (W=2), the other finishes in the background. Live run:
  `written_to: node1, node5, node2` (the third ack arrived within the same wait).
- Tests about D5/D7 routing now say so: unit tests use a `FIVE_STRICT` config, and
  `tests/test_read_repair.py` overrides the `cluster` fixture with `sloppy_quorum=False`; with it on,
  a stand-in's hint could reach the stale replica before the read and hide what is tested.
- `tools/demo_phase3.sh` step 2 now makes the replica stale with a short outage (kill, write at
  once, start, before gossip suspects it). After a long outage the stand-in hands the write over
  itself, which is what `tools/demo_sloppy.sh` shows.
- `tools/demo_phase2.sh` is unchanged and still passes: its "2 replicas down → QUORUM FAILED"
  write comes right after the kills, while gossip still sees the owners as ALIVE, so no stand-in is
  used. After ~5 s (SUSPECT) the same write would succeed through stand-ins.
- While building, the full suite crashed with a grpc segfault on Python 3.14: a D5 unit test whose
  fake had `peers=None` now reached the stand-in path, and the failing background tasks it left
  behind crashed a later test. Fixed by making that test strict (it is about D5 routing); the
  suite then passed 3 runs in a row.

**Tests:** 9 new unit tests (stand-in choice, pairing, SUSPECT gets a stand-in, flag off = D7,
hints sent and counted, fast fail with stand-ins, all owners down, reads strict) and 3 cluster
tests (SPEC cluster test 3: flag off → `QUORUM_FAILED`; test 4: flag on → OK, stand-in has a hint,
not data; owners get the write back after restart; reads stay strict). Full suite: **343 passed,
1 skipped**. With the stand-in path switched off, 6 tests fail.

**Demos (local cluster):** `tools/demo_sloppy.sh` PASSED (2 owners killed, put OK via node1 +
stand-in node5, `local-get` on node5 NOT FOUND, R=2 QUORUM FAILED / R=1 book, owners restarted and
holding `book` within ~1 s of being ALIVE). `demo_phase3.sh` and `demo_phase2.sh` PASSED again.
Docker: `DOCKER=1 tools/demo_phase3.sh` and `DOCKER=1 tools/demo_sloppy.sh` PASSED (Meet, 2026-11-21). First try failed only because the scripts used the system `python` without grpc; the tools now default to `.venv/bin/python` (commit `a07100a`).

# D6b — Hinted handoff: hint store + delivery loop

Owner: Meet · Reviewer: Prateek · Status: REVIEW · Date: 2026-11-21

> Confirmed by Meet on 2026-11-21 (all recommended options). Part 6b of D6 (DEC-12), moved
> from Prateek to Meet (DEC-14). Uses the frozen
> signatures from INTERFACES (`put_hint`, `hints`, `drop_hint`, `Handoff`), no proto change.

## 1. Requirement
From TASKS.md (6b): stand-ins persist hints (survive restart), retry delivery every
`handoff_retry_ms` to owners that are ALIVE, delete only after the owner acks.
**Done when:** kill owner → hinted write → restart owner → within ~5 s the owner has the data and
the hint is gone, also after a stand-in restart. (The hinted write comes from D8 sloppy quorum;
until D8 the tests send the hinted `ReplicaPut` directly.)

## 2. Chosen approach (plain words)
When a key's owner is down, D8 sends the write to a stand-in with `hint_for = owner`. The stand-in
does **not** store it as its own data: it keeps it in a separate hint table, written to the same
log (fsync before the ack), so it survives a crash. A loop on the stand-in wakes every 5 s, and also
right away when gossip says an owner is ALIVE again; for each owner that is ALIVE it sends the
hints back one by one with a plain `ReplicaPut`, and deletes each hint after the owner acks.

### Decisions

**Q1. Delivered twice (ack lost)**
- Delete only after the ack; a lost ack means the hint is sent again on the next round.
- Harmless: the owner already has a version with an EQUAL clock, so `merge_into` changes nothing,
  `storage.apply` returns False and writes no log line (DEC-11). The owner still answers `ok=True`.

**Q2. One hint per RPC or a batch**
- One hint per `ReplicaPut` with an empty `hint_for` (DEC-04: no `DeliverHint` RPC, no proto
  change). Owners one after another; one owner's hints in the order `hints()` gives them.
- The first failed send stops that owner's round (it is probably down again); the next round
  tries again. Each call gets `rpc_timeout_ms` (500 ms).

**Q3. Owner never comes back**
- Out of scope: hints are kept until delivered. The node list is fixed in the config and ownership
  never changes (D4 Q1), so a node is never removed for good. A hint that is lost anyway (stand-in
  disk lost) is repaired by 6c Merkle anti-entropy.

**Q4. Storing and deleting hints durably** (the hint parts of `storage.py`, DEC-14)
- Hint table in memory: `owner -> key -> list[Version]`, kept with `merge_into` like normal data,
  so a newer hinted write for the same key replaces an older one (fewer deliveries).
- `put_hint(owner, key, version)`: log line `{"op": "put", ..., "hint_for": owner}` (the field
  already exists) + **fsync** before the ack, same rule as `apply`. Nothing new → no line.
- `drop_hint(owner, key, version)`: removes the version with an EQUAL clock and writes a line
  `{"op": "drop_hint", "owner", "key", "clock"}` **without fsync**. If the node crashes before the
  line reaches the disk, the hint comes back on replay and is sent again: harmless by Q1. Unknown
  hint → nothing, no line.
- Replay: a `put` line with `hint_for` goes to the hint table (today it would wrongly go into the
  data); a `drop_hint` line removes it.
- Snapshot: `{"format": 2, "data": {...}, "hints": {owner: {key: [versions]}}}`. `load` still reads
  the old format (a dict without an integer `"format"`), so existing data folders keep working.
- Hint lines count toward `snapshot_every` like any other line; same lock as `apply`.

**Q5. When delivery happens**
- Loop every `handoff_retry_ms` (5 s), plus a wake-up: `membership.subscribe` gets a callback that,
  when a node turns ALIVE, only sets an `asyncio.Event` (callbacks must be quick and not await).
  The loop waits for "event set or 5 s passed", whichever is first.

**Q6. Does the stand-in return hinted data on reads?** *(must match D8)*
- No. `storage.get`, `ReplicaGet`, `LocalGet` and `counts()` see only the node's own data.
- Cost: while an owner is down, a read can briefly return an older value. Vector clocks keep both
  sides, read repair (D7) and the handoff itself fix it when the owner returns.

**Q7. Who delivers, to whom**
- Only the stand-in that holds the hint delivers it, and only to owners that are ALIVE in its own
  gossip view; SUSPECT / DEAD owners are skipped until the next round.
- `hint_for` equal to the receiving node itself (should not happen) → stored as normal data.

## 3. Alternatives we rejected
| Option | Why not |
|---|---|
| Batch RPC with many hints per call | New proto message (shared file) for a speed-up we do not need |
| Expire hints after X hours | More code, and it silently loses acknowledged writes; nodes are never removed |
| Separate hints file per owner, rewritten atomically | Second file and second durable-write path; the log already does this |
| fsync every `drop_hint` | Costs a disk flush per delivery; a lost drop only causes a harmless re-send |
| Store hinted versions in the normal data | The stand-in is not a replica: reads, read repair and `counts` would see data that is not its own |
| Periodic loop only | Delivery could take ~2 s (gossip) + up to 5 s (loop); the wake-up costs 3 lines |
| Deliver straight from the `subscribe` callback | Callbacks must not await (6a contract) |
| Return hints on reads (`ReplicaGet`) | The stand-in starts acting as a replica; needs a by-key hint lookup on every read |

## 4. Interfaces and data structures
```python
# dynamo/storage.py — hint parts (Meet, DEC-14); signatures frozen in INTERFACES
async def put_hint(self, owner: str, key: str, version: Version) -> None   # log + fsync, then table
def hints(self) -> dict[str, list[tuple[str, Version]]]                     # owner -> [(key, version)], a copy
async def drop_hint(self, owner: str, key: str, version: Version) -> None  # log (no fsync), then table
self._hints: dict[str, dict[str, list[Version]]]                            # owner -> key -> versions
def encode_drop_line(owner: str, key: str, version: Version) -> bytes
# decode_line returns the record kind too: ("put", key, version, hint_for) / ("drop_hint", ...)

# dynamo/handoff.py (Meet, DEC-14); class signatures frozen in INTERFACES
class Handoff:
    async def start(self) -> None          # subscribe + start the loop task
    async def stop(self) -> None           # cancel the loop task, wait for it
    async def deliver_once(self) -> int    # one round; returns how many hints were delivered (tests)
    def _on_state(self, node_id, old, new) -> None   # new == ALIVE → self._wake.set()
    async def _send(self, owner, key, version) -> bool   # one ReplicaPut, False on any failure

# dynamo/replica.py (Meet): ReplicaPut with hint_for set (and not this node) → storage.put_hint
```

## 5. Flow
1. **Store:** `ReplicaPut(hint_for=owner)` arrives at the stand-in → `storage.put_hint` →
   log line + fsync → hint table → `ok=True`.
2. **Loop:** `while running: wait(wake or handoff_retry_ms); wake.clear(); await deliver_once()`.
3. **deliver_once:** for each owner in `storage.hints()` that is ALIVE: for each (key, version):
   `_send` → on ack `storage.drop_hint`; on failure log WARNING and go to the next owner.
   One INFO line per owner: `handoff: delivered 3 hint(s) to node3`.
4. **Restart of the stand-in:** `load` rebuilds the hint table from snapshot + log; the loop sends
   them on its first round.

## 6. Edge cases
| Case | What happens | Test |
|---|---|---|
| Owner down, hinted write, owner back | Delivered within ~5 s, hint gone | `test_hint_delivered_when_owner_returns` (cluster) |
| Stand-in restarts before delivering | Hint replayed from the log, delivered after | `test_hint_survives_stand_in_restart` (cluster) |
| Ack lost → sent twice | Owner ignores the duplicate | `test_duplicate_delivery_is_harmless` (unit) |
| Owner still DEAD / SUSPECT | Skipped, hint kept | `test_dead_owner_skipped` (unit) |
| Send fails mid-round | Rest of that owner's hints wait; other owners still served | `test_failure_stops_only_that_owner` (unit) |
| Two hinted writes, same key, second newer | Only the newer one kept and sent | `test_newer_hint_replaces_older` (storage unit) |
| Hinted write is not the stand-in's data | `get`, `LocalGet`, `counts` do not show it | `test_hints_invisible_to_reads` |
| Crash after delivery, before the drop line hit disk | Hint replayed, re-sent, harmless | `test_drop_line_replay` (storage unit) |
| Snapshot with pending hints | Hints in the snapshot, log cleared, reload keeps them | `test_snapshot_keeps_hints` (storage unit) |
| Old snapshot format on disk | Loaded as data, no hints | `test_old_snapshot_format_loads` (storage unit) |
| Owner turns ALIVE | Loop wakes at once instead of waiting 5 s | `test_alive_owner_wakes_the_loop` (unit) |
| `hint_for` = the receiving node | Stored as normal data | `test_hint_for_self_is_normal_write` |

## 7. Tests
- Storage unit (`tests/test_storage_hints.py`): put/drop/replay/snapshot/old format/newer hint.
- Handoff unit (`tests/test_handoff.py`): fake storage, membership and peers; skip, stop on
  failure, wake-up, duplicate.
- Cluster (`tests/test_handoff_cluster.py`, `cluster` fixture with `handoff_retry_ms` 300): kill
  owner, hinted `ReplicaPut` straight to a stand-in, restart owner, `wait_until` owner `LocalGet`
  shows it and the stand-in's `hints()` is empty; same with a stand-in restart in between.
- After D8 the same check runs end to end through a client Put (cluster test 4 in D10).

## 8. Interview questions on this part
1. Q: Why is delivering a hint twice safe?  A: The owner already holds a version with an equal
   clock; merging it changes nothing, so it is ignored. Delivery is idempotent.
2. Q: Why fsync `put_hint` but not `drop_hint`?  A: Losing a hint loses an acknowledged write;
   losing a drop only re-sends a hint the owner will ignore.
3. Q: Why are hints kept apart from the stand-in's own data?  A: The stand-in is not a replica of
   that key. Mixing them would make reads, read repair and key counts report data it does not own.
4. Q: What if the stand-in dies for good while holding hints?  A: Hinted handoff cannot help;
   the write may still be on other replicas (W counted them), and 6c anti-entropy repairs the rest.
5. Q: Hinted handoff vs read repair vs anti-entropy?  A: Handoff covers short outages of a known
   owner, read repair fixes keys that are read, Merkle anti-entropy fixes everything else.

## 9. As built
Built 2026-11-21 in `dynamo/storage.py` (hint parts only, DEC-14), `dynamo/handoff.py`,
`dynamo/replica.py`, tests `tests/test_storage_hints.py`, `tests/test_handoff.py`,
`tests/test_handoff_cluster.py`, and two tests in `tests/test_server.py`.

**Deviation from Q4: hints in their own `hints.json`, not in a "format 2" snapshot.**
`snapshot.json` keeps exactly its old format (key -> versions). The same snapshot step writes
`hints.json.tmp` next to `snapshot.json.tmp` (both inside `_write_snapshot_tmp`), renames
`snapshot.json` then `hints.json`, fsyncs the folder, and only then clears the log. Why: Prateek's
`test_snapshot_clears_log` reads `snapshot.json` as key -> versions, and his crash tests pause
right after `_write_snapshot_tmp`; this way neither his tests nor the data format change, and old
data folders need no format check (no `hints.json` = no hints). A crash between the two renames
leaves new data + old hints + the full log; replaying the log rebuilds the hints (merging a hint
twice or dropping a missing one does nothing) — tested.

**Other small points**
- `decode_record(line)` is new and used by replay; it returns `("put", key, version, hint_for)` or
  `("drop_hint", owner, key, clock)`. `decode_line` is kept with its old meaning (put lines only)
  and now calls `decode_record`.
- The log write and the "count a line, snapshot every N" steps of `apply` moved into two helpers
  (`_write_line(line, fsync)`, `_count_line()`), shared by `apply`, `put_hint` and `drop_hint`.
  `apply` behaves exactly as before (all of Prateek's storage and crash tests pass unchanged).
- `Handoff.deliver_once()` is public (tests call it); the loop catches and logs any error in a
  round so one bad round never stops delivery.
- `ReplicaPut` with `hint_for` equal to the receiving node is stored as normal data.
- Not handled (out of scope, Q3): a `hint_for` that is not a node in the config would be kept
  forever. Only D8's coordinator sends hints, and it only names nodes from the ring.

**Tests:** 8 storage hint tests, 6 handoff unit tests, 3 cluster tests, 2 server tests (the old
"hinted ReplicaPut is UNIMPLEMENTED" test replaced). Full suite: **331 passed, 1 skipped**.
Checked that the tests catch real breakage: with the loop's `deliver_once` call removed, 3 tests
fail; with replay ignoring `hint_for` (the old bug: hints became data after a restart), 4 fail.

**Done-when check:** in-process cluster: owner killed, hint sent to a stand-in, owner restarted →
owner has the value and the stand-in has no hints within the 8 s wait (in practice < 1 s with the
300 ms test timer); same after a stand-in crash + restart. End to end through a client Put comes
with D8.

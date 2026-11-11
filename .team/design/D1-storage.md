# D1 — Storage engine (write-ahead log + snapshot)

Owner: Prateek · Reviewer: Meet · Status: DESIGNED · Date: 2026-11-11

> Function names follow Meet's D0-M draft (`recover`, `get`, `apply`, `close`), because
> `.team/INTERFACES.md` does not exist yet. The first version of this design called them
> `load`, `read`, `save`, `close`; the behaviour is unchanged.

## 1. Requirement

Phase 1: one node, no copies. Data must survive a crash.
Demo: put a key, kill the node (`kill -9`), restart it, the key is still there.

## 2. Chosen approach (plain words)

- All keys live in one Python dict in memory. Reads only look at the dict and never touch the disk.
- Every change (put or delete) is first appended as one line to a log file on disk (`wal.log`) and
  forced onto the physical disk with `fsync`. Only after that is the dict updated, and only after
  that does the client get OK.
- A delete does not remove the key. It stores the key with a "deleted" marker (a tombstone).
- The log would grow forever, so after every 1000 log lines we copy the whole dict into
  `snapshot.json` and then clear the log. The snapshot is a copy of the dict, never of the log.
- On restart the node rebuilds the dict: load `snapshot.json`, then replay the log on top of it,
  and only then start accepting requests.
- One lock protects the whole storage. Writes go one at a time, and a snapshot holds the same lock,
  so writes that arrive during a snapshot wait in the lock's queue and run afterwards in order.
- With many nodes (Phase 2+), every node snapshots on its own. No global snapshot algorithm.

## 3. Alternatives we rejected

| Option | Why not |
|---|---|
| Dict as a cache of recent keys, with a lookup store on disk for misses | Finding one key inside a file fast needs an index or B-tree, which is a database engine of its own. Keeping all keys in memory is enough for our data size. |
| Update the dict first, then write the log | A crash between the two loses a write the node already holds in memory, and the client could have been told OK for data that is not on disk. |
| Build the snapshot from the log | The log holds history (put, delete, put again). The dict already holds the final state, so copying it needs no merging. |
| Clear the log first, then write the snapshot | A crash in between loses everything since the previous snapshot. |
| Write the snapshot directly into `snapshot.json` | A crash halfway leaves a half-written snapshot and destroys the old good one. Temp file + rename is atomic. |
| A separate buffer for requests that arrive during a snapshot | The lock's waiting queue already does this for free, in arrival order. |
| Per-key locks, or batching several writes into one `fsync` (group commit) | Faster, but more code and harder to explain. One lock is enough for the demo load. |
| Checksum on every log line | Detects silent disk corruption, but for crash recovery the newline + JSON check is enough. |
| Chandy-Lamport style global snapshot across the 5 nodes | That captures one consistent state of the whole system. Our snapshot is only a local recovery checkpoint. Differences between nodes are expected and fixed later by hinted handoff, read repair and vector clocks. |
| Calling `fsync` directly on the event loop | Freezes the whole node (including gossip heartbeats later) for every write. We run it in a thread while holding the lock. |

## 4. Interfaces and data structures

Function names must match `.team/INTERFACES.md` once D0 freezes it. Until then they follow
Meet's D0-M draft.

```python
# What is stored per key
Version:
    value: bytes        # empty for a tombstone
    clock: dict         # vector clock, empty {} until Phase 3 (D3)
    tombstone: bool     # True = deleted marker

class Storage:
    def __init__(self, data_dir: str, snapshot_every: int = 1000): ...
    def recover(self) -> None                           # startup: snapshot + log replay (section 5.4)
    def get(self, key: str) -> list[Version]            # from the dict only; [] if key unknown
    async def apply(self, key: str, version: Version) -> bool  # write flow (5.1); returns after data is on disk
    async def close(self) -> None                       # clean shutdown: wait for the lock, close the log file
```

- In memory: `dict[str, list[Version]]`. In Phase 1 the list always has exactly one item:
  the newest write replaces the old one. In D3 this "replace" rule is swapped for vector-clock
  reconcile behind the same `apply` signature, and the list can then hold siblings.
- Files, both inside `data_dir` (`/data/<node_id>`):
  - `wal.log` — one JSON object per line, each line ends with `\n`:
    `{"op":"put","key":"cart:42","version":{"value":"<base64>","clock":{},"tombstone":false},"hint_for":""}`
  - `snapshot.json` — the whole dict as one JSON object (values base64).
  - `snapshot.json.tmp` — temp file used while writing a snapshot.
- `hint_for` is always empty in Phase 1. It is reserved for hinted handoff (D9).

## 5. Flow

### 5.1 Put (and delete)
1. Server calls `apply(key, version)`. For a delete, `version` is a tombstone.
2. Take the lock (wait in its queue if a write or snapshot is running).
3. Append one line to `wal.log`.
4. `fsync` the log (run in a thread so the event loop stays free).
5. Update the dict.
6. Add 1 to the line counter. If it reached 1000, run the snapshot (5.3) while still holding the lock.
7. Release the lock and return True. The server replies OK to the client.

If step 3 or 4 fails, the dict is not touched and `apply` returns False / raises; the client gets an error.

### 5.2 Get
1. Server calls `get(key)`.
2. Return what the dict holds. No lock, no disk.
3. The server turns "unknown key" or "only a tombstone" into NOT_FOUND.

### 5.3 Snapshot (every 1000 log lines, lock already held)
1. Write the whole dict to `snapshot.json.tmp` and `fsync` it.
2. Rename it to `snapshot.json` (atomic: either the old snapshot or the new one, never half).
3. Clear `wal.log` and `fsync`. Reset the line counter to 0.

### 5.4 Restart (`recover`)
1. If `snapshot.json` exists, load it into the dict. If not (first start, or fewer than 1000
   writes so far), start with an empty dict.
2. Replay `wal.log` from the first line to the last, applying each line to the dict.
3. If the last line is broken, skip it and cut the file back to the end of the last good line.
4. Set the line counter to the number of lines replayed.
5. Only now may the server start accepting requests.

## 6. Edge cases

| Case | What happens | Test |
|---|---|---|
| Crash in the middle of writing a log line | The line has no `\n` or is not valid JSON. On restart it is skipped and the file is cut back. Safe, because the client never got OK for it. | `test_torn_last_line` |
| Broken line left in the file, then a new write | Cannot happen: the file is cut back during restart, so the next line never glues onto a half-line. | `test_append_after_torn_line` |
| Broken line in the middle of the log | Not a crash but disk corruption. Stop with a clear error instead of guessing. | `test_corrupt_middle_line` |
| Crash after the log `fsync` but before the dict update | The write is in the log, so replay restores it. The client got no OK and may retry; the retry just writes the same value again. | `test_kill_after_fsync` |
| Crash while writing the temp snapshot | Old `snapshot.json` and full log are untouched. Leftover `.tmp` is deleted on restart. | `test_crash_during_snapshot_tmp` |
| Crash after the rename but before the log is cleared | New snapshot + old log both exist. Replaying the whole log in order over the snapshot ends in the same final state (put a=1, delete a, put a=3 → a=3). | `test_crash_between_rename_and_clear` |
| Put arrives while a snapshot is running | It waits in the lock's queue, runs after the snapshot, and only then gets OK. It can never be in the cleared log but missing from the snapshot. | `test_put_during_snapshot` |
| Crash while puts are waiting in the queue | Those clients never got OK, so nothing promised is lost. | covered by the above |
| Two puts for the same key at the same moment | The lock makes them run one after the other; log order = dict order. | `test_concurrent_puts_same_key` |
| Get during a snapshot or during a put | Reads the dict directly, does not wait. | `test_get_during_snapshot` |
| Get arrives during restart | Cannot happen: requests are accepted only after replay finishes. | `test_no_requests_before_load` |
| No `snapshot.json` yet | Dict is built from the log alone. | `test_restart_without_snapshot` |
| Restart with 600 lines in the log | Counter continues from 600, so the next snapshot comes after 400 more writes, not 1000. | `test_counter_after_restart` |
| Replaying the same line twice | Harmless: applying a line again gives the same result (newest line for a key wins). | `test_replay_twice` |
| Delete of a key that does not exist | A tombstone is stored anyway (needed in Phase 2 so a stale copy cannot bring the key back). | `test_delete_unknown_key` |

Known limitations we accept:
- All data must fit in RAM (fine here: 10,000 keys × 1 KB ≈ 10 MB).
- Writes go one at a time, so write speed is limited by `fsync` speed.
- Puts pause briefly while a snapshot is written.
- No per-line checksum, so silent corruption inside a valid-looking line is not detected.
- Tombstones are never purged in Phase 1 (the 24 h purge from the spec is left for later).

## 7. Tests

- Unit (`tests/test_storage.py`): every test named in section 6, using a temp folder as `data_dir`.
- Real crash test: start a node as a subprocess, write keys, `kill -9` it, start it again,
  check every acknowledged key is present. Same again with a half-line appended to `wal.log` by hand.
- Timing: 10,000 puts on one node; record total time and writes per second here: _to be measured_.

## 8. Interview questions on this part

1. Q: Why write to the log before updating memory?
   A: The log on disk is the only thing that survives a crash. If memory were updated first, we
   could tell a client OK for data that is not on disk yet, and lose it.
2. Q: What happens if the node dies halfway through writing a log line?
   A: On restart the last line fails the newline/JSON check, so we drop it and cut the file back.
   The client never got OK for that write, so no promise is broken.
3. Q: How do you take a snapshot without losing writes that arrive at the same time?
   A: The snapshot holds the same lock as writes. Writes wait in the lock's queue and run afterwards.
4. Q: What if the node crashes during a snapshot?
   A: We write to a temp file and rename it, which is atomic. Before the rename, the old snapshot
   and full log are still there. After the rename but before the log is cleared, replaying the old
   log over the new snapshot gives the same final state.
5. Q: With 5 nodes, why don't you need a distributed snapshot algorithm like Chandy-Lamport?
   A: Our snapshot is only a local recovery checkpoint for one node, not a global state. Nodes are
   allowed to differ for a while; hinted handoff, read repair and vector clocks fix the differences.
6. Q: Why keep everything in memory instead of on disk?
   A: It keeps reads simple and fast and avoids building an on-disk index. Real Dynamo plugs in a
   disk storage engine; we kept memory + log to focus on the distributed part. The cost is that
   data must fit in RAM.
7. Q: Why is a delete stored as a tombstone instead of removing the key?
   A: With copies on several nodes, a node that missed the delete would otherwise bring the key
   back. The tombstone is a newer version that wins over the old value.
8. Q: What limits your write throughput?
   A: One `fsync` per write under one lock. Group commit (one `fsync` for several writes) would
   raise it; we rejected it to keep the design simple.

## 9. As built

_Filled after implementation: what changed from this design and why._

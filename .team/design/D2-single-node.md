# D2 — Single-node server + CLI

Owner: Meet · Reviewer: Prateek · Status: REVIEW · Date: 2026-11-20

> Confirmed by Meet on 2026-11-20 as written. The shared-file changes (Q2 helpers, Q5) are DEC-10:
> Meet's call, Prateek informed, no confirmation needed.

## 1. Requirement
From TASKS.md: a `grpc.aio` server exposing `KVStore` and `NodeService` on one port. With N=1 it
serves Put / Get / Delete / ClusterInfo against local storage. CLI as in the SPEC, with a context
file. Clean shutdown on SIGTERM / SIGINT.
**Done when:** the Phase 1 demo works (put → `kill -9` → restart → get) and the CLI tests cover
put / get / delete / siblings output.

Already built in D0-M (not redone here): server wiring, flags, start/stop order and signals,
validation, CLI with context file, exit codes and sibling printing, `ReplicaPut` / `ReplicaGet`,
peers with 1 s reconnect. Prateek's D1 storage gives `get` / `apply` with WAL + fsync.

## 2. Chosen approach (plain words)
Put, Get and Delete go through the real `Coordinator`, not straight to storage. In D2 the
coordinator only handles the case where **this node is the only replica** (N=1): it builds the new
clock, writes to local storage (that is the one and only ack), and answers. Anything that would
need another node (this node not in the preference list, or N > 1) still raises
`NotImplementedError`, so it answers UNIMPLEMENTED until D5. D5 then adds forwarding and the
replica fan-out around the same code instead of rewriting it.

The context already uses its final format, a serialized `VectorClock` proto, and the clock already
follows the DEC-02 counter rule. Until Prateek's D3 lands, storage keeps only the newest version,
so there are no siblings yet; the code paths for siblings exist and are tested with storage that
returns two versions.

The demo runs one node with a one-node config file.

### Decisions (confirmed)

**Q1. Local coordinator or direct storage calls?**
- Decision: through the `Coordinator`. `put/get/delete` check the preference list; if it is
  `[self]` they call `_do_put/_do_get/_do_delete`, which in D2 do only the local step. Otherwise
  `NotImplementedError` (forwarding is D5).
- Why: D5 extends `_do_*` with the fan-out and `_forward`; the server and handlers never change.

**Q2. Clock and context before D3**
- New clock = the client's context clock, with this node's counter set to
  `max(context[self], highest own counter among the stored versions of the key) + 1`, under
  `_key_lock(key)` (DEC-02). Stored as the `clock` dict of storage's `Version`.
- Context on the wire = `VectorClock(counters=...)` serialized with `SerializeToString()`, which is
  the format D0-P fixed. Two small helpers in `coordinator.py`, `_encode_context` and
  `_parse_context`, are replaced by Prateek's `version.context_of` / `version.parse_context` once
  D3 exists.
- Bad context bytes → `INVALID`.
- Prateek informed (DEC-10): he swaps them for `version.py` when D3 lands.

**Q3. What Get returns**
- `storage.get(key)` → all versions. Live = versions that are not tombstones.
- No versions at all → `NOT_FOUND`, empty context.
- Only tombstones → `NOT_FOUND`, with the merged clock of the tombstones as context (Q11 in D0-M).
- Otherwise `OK`, `values` = the live values (more than one = siblings), context = merged clock of
  **all** versions, tombstones included. `read_from = [self]`.
- Put answers `OK`, the new context, `written_to = [self]`. Delete answers `OK`.

**Q4. R and W with one node**
- `r` / `w` = 0 means the cluster default. A value above N is already `INVALID` (D0-M `validate`).
- With N=1 the local write is the only ack, so W=1 is met as soon as `storage.apply` returns.

**Q5. Demo config**
- New file `cluster.single.yaml`: one node `node1: localhost:50051`, `n: 1, r: 1, w: 1`.
- `Makefile`: `CONFIG ?= cluster.local.yaml` instead of the fixed value, so
  `make run NODE=node1 CONFIG=cluster.single.yaml` works.
- Shared files; Prateek informed (DEC-10).

**Q6. Demo script**
- `tools/demo_phase1.sh`: start the node in the background, `kv put demo:1 hello`,
  `kill -9` the node, start it again, `kv get demo:1` must print `hello`, then stop it. It prints
  `PHASE 1 DEMO PASSED` or fails with a non-zero exit code.
- The same steps run as an automated test with a real subprocess.

## 3. Alternatives we rejected
| Option | Why not |
|---|---|
| KVStore handlers call storage directly in D2 | D5 would have to move all of it into the coordinator again |
| Empty context until D3 | The CLI context file and the counter rule could not be tested before D3; the final format is already fixed |
| A temporary JSON context | It would change format at D3 and break saved context files |
| Put/Get on a multi-node config answered locally anyway | It would store keys on the wrong node; UNIMPLEMENTED is honest until D5 |
| Change `cluster.local.yaml` to N=1 | The 5-node local config is needed for D4 and D5 |

## 4. Interfaces and data structures
```python
# dynamo/coordinator.py (Meet) — D2 fills these in
async def put(self, req: PutRequest, budget: float) -> PutResponse
async def get(self, req: GetRequest, budget: float) -> GetResponse
async def delete(self, req: DeleteRequest, budget: float) -> DeleteResponse
    # preference list == [self] → _do_*; otherwise NotImplementedError (D5)

async def _do_put(self, req: PutRequest, budget: float) -> PutResponse
    # parse context (bad → INVALID) → under _key_lock: new clock (Q2) → storage.apply → OK
async def _do_get(self, req: GetRequest, budget: float) -> GetResponse
    # storage.get → live values + merged context (Q3)
async def _do_delete(self, req: DeleteRequest, budget: float) -> DeleteResponse
    # same as _do_put with Version(tombstone=True)

def _new_clock(self, context: dict[str, int], stored: list[Version]) -> dict[str, int]
def _encode_context(versions: list[Version]) -> bytes        # until D3: merged clock → VectorClock bytes
def _parse_context(data: bytes) -> dict[str, int]            # until D3: b"" → {}; bad bytes → ValueError
```

```python
# uses, from Prateek (unchanged)
Storage.get(key) -> list[Version]
await Storage.apply(key, version) -> bool
Version(value: bytes, clock: dict[str, int], tombstone: bool)
```

## 5. Flow
1. `kv put demo:1 hello` → KVStore `Put` → `validate` → `coordinator.put`.
2. `ring.preference_list(key, n)` is `[node1]` = this node → `_do_put`.
3. Parse the context; under the key's lock compute the new clock from the context and the stored
   versions; `storage.apply(key, Version(value, clock))` (WAL + fsync).
4. Reply `OK`, context = new clock, `written_to = [node1]`. The CLI saves the context.
5. `kv get demo:1` → `_do_get` → `storage.get` → `OK`, `values = [b"hello"]`, context.
6. `kill -9`, restart: `storage.load()` replays the log; the same Get returns `hello`.

## 6. Edge cases
| Case | What happens | Test |
|---|---|---|
| Put without context on a new key | Clock `{node1: 1}` | `test_put_first_clock` |
| Put with the context from the last Get | Counter goes up by one | `test_put_with_context_counter` |
| Two puts with the same context | Different counters (DEC-02) | `test_same_context_different_counters` |
| Bad context bytes | `INVALID` | `test_put_bad_context_invalid` |
| Get on an unknown key | `NOT_FOUND`, empty context | `test_get_unknown` |
| Get after delete | `NOT_FOUND` with the tombstone's clock; a put with that context replaces it | `test_get_after_delete` |
| Storage holds two live versions (as after D3) | Both values returned, context = merged clock | `test_get_siblings` |
| `w` or `r` larger than N | `INVALID` (D0-M validate) | `test_quorum_above_n_invalid` |
| This node not in the preference list (multi-node config) | UNIMPLEMENTED until D5 | `test_not_replica_unimplemented` |
| `kill -9` right after an acknowledged put | Value is there after restart | `test_phase1_kill9` |
| SIGTERM | Clean stop, exit code 0 (D0-M) | `test_sigterm_stops_node` |

## 7. Tests
- Unit (`tests/test_coordinator.py`): clock rule, context encode/parse, Get result building, with a
  fake storage.
- Single node (`tests/test_single_node.py`): one real node with N=1 in the test process: put, get,
  delete, get-after-delete, bad context, quorum above N.
- Real process: `test_phase1_kill9` starts `python -m dynamo.server` with `cluster.single.yaml`,
  puts with the CLI, sends SIGKILL, restarts, gets.
- CLI tests from D0-M already cover put / get / delete / siblings output.
- Manual: `tools/demo_phase1.sh`.

## 8. Interview questions on this part
1. Q: Why route a single-node Put through the coordinator at all?  A: The coordinator is where the
   quorum logic will live; building the local step there now means D5 only adds the fan-out.
2. Q: What does the context contain with one node?  A: A vector clock with one entry, e.g.
   `{node1: 3}`, serialized as a `VectorClock` proto: the same format the cluster will use.
3. Q: How do you know an acknowledged write survives `kill -9`?  A: The reply is sent only after
   `storage.apply` returns, which is after the log line is fsynced; the demo and a test kill the
   real process and read it back.
4. Q: Why is Get on a deleted key NOT_FOUND but still returns a context?  A: The tombstone has a
   clock; the next put must be newer than it, or it would look concurrent with the delete.

## 9. As built
_Done 2026-11-20, built as designed._
- `coordinator.py`: `put/get/delete` check `_require_only_replica` (preference list == `[self]`), then `_do_*`. Shared write step `_write_local` builds the clock under the key's lock (`_new_clock`) and calls `storage.apply`. Temporary helpers `_merge_clocks`, `_encode_context` (serialized `VectorClock`, `deterministic=True`) and `_parse_context` (bad bytes → `ValueError` → `INVALID`), to be replaced in D3.
- `cluster.single.yaml` (node1 on localhost:50051, N=R=W=1); `Makefile` has `CONFIG ?= cluster.local.yaml`.
- `tools/demo_phase1.sh` runs the demo with a fresh temp data folder and prints `PHASE 1 DEMO PASSED` (checked on Meet's laptop).
- Tests: `test_coordinator.py` (clock rule, context, get result, siblings with a fake storage, multi-node → UNIMPLEMENTED) and `test_single_node.py` (real node over gRPC, plus `test_phase1_kill9` with a real process and SIGKILL). Whole suite: 139 passed, 1 skipped.
- Small additions to the design: a test that clock entries of other nodes in the context are kept, and `ClusterInfo` key/tombstone counts checked on a running node.
- Run the demo: `PYTHON=.venv/bin/python tools/demo_phase1.sh`, or by hand: `make run NODE=node1 CONFIG=cluster.single.yaml`, then `kv put demo:1 hello`, `kill -9`, start again, `kv get demo:1`.

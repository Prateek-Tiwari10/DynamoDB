# D3 — Vector clocks and reconciliation

Owner: Prateek · Reviewer: Meet (later, see REVIEW-LATER.md) · Status: IMPLEMENTING · Date: 2026-11-21

Saved word for word from Prateek's design chat ("D3 — Vector Clocks: Implementation Instructions").
The design is in the numbered sections below; it is updated as each topic is decided. Shared-file
changes it causes are recorded as DEC-11 (Meet to confirm). Section "As built" is at the end.

---

Scope of D3: `dynamo/vclock.py`, `dynamo/version.py`, `tests/test_vclock.py`, `tests/test_version.py`, and switching `storage.apply` to `merge_into`.

This file is updated as each topic is decided. Implement only the sections below; the rest comes later.

---

## 1. VectorClock shape

`VectorClock` holds two dicts:

- `counters: dict[str, int]` — node id → counter. One entry per node that coordinated a write to this key. **Only counters decide ordering.**
- `updated_ms: dict[str, int]` — node id → wall-clock time (ms) when that entry was last bumped. **Used only by prune** to pick which entry to drop. Never used for ordering (node clocks can be skewed).

Each key's versions carry their own clock (clock is per key; entries are per node).

Basics to include: empty constructor, `copy()`, and `__eq__` (compares both dicts; used by round-trip and replay tests).

---

## 2. `compare(a, b)`

Returns how version `a` stands relative to version `b`: `BEFORE`, `AFTER`, `EQUAL`, or `CONCURRENT`.

### Rule

1. Loop over the union of node ids in `a.counters` and `b.counters`.
2. For each node, read both counters; **a missing node counts as 0**.
3. Track two flags:
   - `a_ahead` = some node has `a[n] > b[n]`
   - `b_ahead` = some node has `b[n] > a[n]`
4. Decide:

| a_ahead | b_ahead | Result |
|---|---|---|
| no | no | EQUAL |
| no | yes | BEFORE (a is older; b includes it) |
| yes | no | AFTER (a is newer; a includes b) |
| yes | yes | CONCURRENT (siblings) |

`compare` must **never read `updated_ms`**.

### Tests (table-driven, in `tests/test_vclock.py`)

| a | b | expected | checks |
|---|---|---|---|
| `{}` | `{}` | EQUAL | both empty |
| `{}` | `{A:1}` | BEFORE | empty is before everything |
| `{A:1}` | `{}` | AFTER | reverse |
| `{A:2}` | `{A:2}` | EQUAL | same counters |
| `{A:1}` | `{A:2}` | BEFORE | same key, smaller |
| `{A:2, B:1}` | `{A:2}` | AFTER | b missing a key |
| `{A:2}` | `{A:2, B:1}` | BEFORE | a missing a key |
| `{A:2}` | `{B:1}` | CONCURRENT | disjoint keys |
| `{A:2, B:1}` | `{A:1, B:2}` | CONCURRENT | each ahead somewhere |
| `{A:3}` | `{A:2, B:1}` | CONCURRENT | ahead on A, behind on B |
| `{A:3, B:1}` | `{A:2, B:1}` | AFTER | ahead on A, tie on B |
| `{A:0}` | `{}` | EQUAL | explicit 0 same as missing |
| `{A:2} @t100` | `{A:2} @t999` | EQUAL | updated_ms ignored |

Plus a **symmetry test** over random clocks:
- `compare(a, b) == BEFORE` ⇔ `compare(b, a) == AFTER`
- `EQUAL` and `CONCURRENT` give the same result in both directions.

---

## 3. `prune(clock, self_id, max_entries=10)`

Keeps a clock from growing without limit. `max_entries` comes from config `vclock_max_entries` (= 10).

### Rule

- Runs **only in the coordinator, right after `advance`**. Replicas never prune.
- Returns a **copy**; never modifies the input.
- While the clock has more than `max_entries` entries: drop the entry with the **oldest `updated_ms`**.
  - Tie on `updated_ms` → drop the **smaller node id** (deterministic).
  - **Never drop `self_id`** (the coordinator just bumped it).
- `context_of` (merged clock) is allowed to exceed 10 entries — merging two pruned clocks can do that. Do not prune there.

Why it is safe: pruning can only make an ordered pair look CONCURRENT (extra sibling), never lose a write.

### Tests

- 10 entries → unchanged.
- 11 entries → the one with the oldest `updated_ms` is removed.
- Two entries tie on oldest `updated_ms` → the smaller node id is removed.
- `self_id` has the oldest `updated_ms` → it is kept; the next oldest is removed.
- Input clock is unchanged after the call.

---

## 4. `merge(a, b)`

Returns a new clock: for every node in either clock, `max` of the counters and `max` of the `updated_ms`. Inputs unchanged.

Tests: disjoint keys, overlapping keys, `updated_ms` takes the max, inputs unchanged, `merge(a, b) == merge(b, a)`.

---

## 5. `advance(clock, node, counter)`

Returns a **copy** with `counters[node] = counter` and `updated_ms[node] = now_ms`. It does not compute the counter.

Signature detail: `advance(clock, node, counter, now_ms=None)` — if `now_ms` is None use the current time in ms; tests pass a fixed value so they can assert it.

The coordinator (D5) computes it, under a per-key lock:
`counter = max(context[self], highest counter of self stored for this key) + 1`

The new version's clock = **client's context** → advance → prune. Never merge in locally stored clocks (that would delete siblings the client never saw).

Known limitation (DEC-02): two writes with the same context through one coordinator get counters N and N+1, so they look ordered. Dotted version vectors would fix it; out of scope.

Tests: returns a copy (input unchanged), sets counter and `updated_ms` for that node, other entries untouched.

---

## 6. `context_of(versions)` and `parse_context(data)`

### Encoding (shared with WAL, see §8)

Clock as JSON with sorted keys: `{"A":[3,1732090000000],"B":[1,1732090005000]}` → `node: [counter, updated_ms]`. Encoded as UTF-8 bytes.

JSON is used (not protobuf) because protobuf parsing is lenient — some random bytes decode without error, which would break "bad bytes → INVALID".

### `context_of(versions) -> bytes`

Merge the clocks of **all** versions (tombstones included) and encode. Empty list → `b""`. Do not prune. Returned on NOT_FOUND too.

### `parse_context(data, known_nodes) -> VectorClock`

- `b""` → empty clock.
- Raise `ValueError` (server answers INVALID) if: not valid UTF-8/JSON; top level not an object; a node id not in `known_nodes`; a value that is not a list of exactly 2 ints; any number negative; any bool (Python treats bool as int — reject explicitly).
- If the signature in INTERFACES has no `known_nodes`, do the node-id check in the server right after parsing.

Tests: empty bytes, valid bytes, each invalid case above, random garbage bytes, round trip `parse_context(context_of(vs)) == merge of all clocks`.

---

## 7. `Version`, `reconcile`, `merge_into`

### Version

`Version(value: bytes, tombstone: bool, clock: VectorClock)` — `clock` was `dict[str, int]`, now `VectorClock`.

### Decision: NO collapsing of same-value siblings

The stored set for a key = **the versions that no other version is newer than** (the maximal versions of everything received). Same-value siblings are kept as separate versions; the coordinator's Get removes duplicate values from the response only.

Reason: collapsing makes the result depend on arrival order. Counterexample:
- order `x{a:2}`, `x{b:2}`, `z{a:3}` → collapse gives `x{a:2,b:2}`, `z{a:3}`
- order `x{a:2}`, `z{a:3}`, `x{b:2}` → `x{a:2}` dropped, gives `x{b:2}`, `z{a:3}`

Without collapse the result depends only on the set of versions received, so one pass is enough (no "repeat until stable").

### `reconcile(versions) -> list[Version]`

1. Group by counters (ignore zero entries; missing = 0). Within a group of EQUAL clocks:
   - same `(tombstone, value)` → keep one, clock = `merge` of their clocks;
   - different `(tombstone, value)` (only possible via a bug or a bad retry) → keep the one with the smaller `(tombstone, value)` and log a **WARNING**. Deterministic, so every replica picks the same one whatever the arrival order.
2. Drop every version that is `BEFORE` some other version.
3. Return the rest sorted by a fixed key (e.g. sorted counter items) so output is deterministic.

### `merge_into(stored, new) -> (list[Version], bool)`

`result = reconcile(stored + [new])`; changed = `snapshot(result) != snapshot(stored)`.

`snapshot(vs)` = set of `(clock_key, sorted updated_ms items, value, tombstone)`.

`changed` is True **only if the stored set differs** — so a stale or duplicate write causes no WAL line / fsync. A merge that only changes a stored clock counts as changed.

This matches the SPEC rules: versions BEFORE the new one are dropped; the new one is ignored if something is AFTER or EQUAL to it; otherwise it becomes a sibling.

Tombstones are never purged (DEC-06); a tombstone is just a version with `tombstone=True`.

### Tests (`tests/test_version.py`)

- Random: result is always pairwise CONCURRENT.
- Random: any shuffle of the inputs, with random duplicates, applied one by one through `merge_into`, gives the same `snapshot`.
- The collapse counterexample above: both orders give the same result.
- Tombstone sibling next to a value sibling: both kept.
- EQUAL clock, different value: same winner in both orders, WARNING logged (use `caplog`).
- Duplicate write → `changed == False`; stale (BEFORE) write → `changed == False`; new sibling → `changed == True`; dominating write → `changed == True`.

---

## 8. Storage switch (`storage.apply`) and WAL/snapshot format

- `storage.apply(key, version)`: `new_list, changed = merge_into(current_list, version)`. If `changed`: append WAL line (fsync), then update memory. Return `changed`.
- `version_to_json` / `version_from_json`: clock uses the same encoding as §6: `"clock": {"A": [3, 1732090000000]}`. Value and tombstone encoding unchanged.
- Old D1 data folders are **not supported** — delete `data/` before running. No compatibility code (nothing was deployed).
- Tests: existing storage tests pass; replaying the WAL rebuilds the same sibling sets (compare `snapshot` per key).

---

## 9. For Meet (send before pushing)

1. `replica.py`: `version_to_proto` / `version_from_proto` must use `version.clock.counters` and `version.clock.updated_ms`.
2. Proto `VectorClock` message: add `map<string, int64> updated_ms` next to `counters`, otherwise `updated_ms` is lost on every replica hop.
3. Coordinator Get: remove duplicate values from the response (storage keeps same-value siblings separately). Context = `context_of(all siblings)`, returned on NOT_FOUND too.
4. The SPEC "collapse same-value siblings" rule is dropped (reason in §7).

---

## As built

_Filled after implementation._

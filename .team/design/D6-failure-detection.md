# D6 — Failure Detection + Replica Sync: Implementation Instructions

D6 has three parts, built and pushed in order: **6a gossip + failure detector** → 6b hinted handoff → 6c Merkle anti-entropy (DEC-12).

> **6b moved to Meet on 2026-11-21 (DEC-14).** Design and as-built: `.team/design/D6b-hinted-handoff.md`, row D6b in PROGRESS.
> This doc keeps 6a (built, §1–§10) and 6c (decided 2026-11-21, §11–§21).

Status: **6a decided and built** (§1–§10). **6b moved to Meet** (DEC-14). **6c decided** (§11–§21) — implement it now.

Note: this file is a design/instructions note — do NOT commit it to the code repo. Its content goes into `.team/design/D6-failure-detection.md` (notes commit) before coding.

---

# 6a — Gossip + failure detector

Scope of 6a: `dynamo/membership.py`, `dynamo/gossip.py`, `tests/test_membership.py`, new `tests/test_gossip.py`, `tests/conftest.py` (test timer), and the shared timer change in §8 (needs DEC-13).

## 1. What 6a does (one paragraph)

Every node keeps a table with one row per member. Every gossip tick a node raises its own heartbeat counter, swaps its whole table with one random peer (push-pull), keeps the newer row for each member, and then re-checks every member: if this node has not seen that member's heartbeat go up for 5 s it is SUSPECT, for 10 s it is DEAD, and as soon as it goes up again it is ALIVE. Each node decides states **on its own**; states are never gossiped.

## 2. Decisions (final)

| # | Decision |
|---|---|
| Q1 | "Time since heartbeat" = **this node's `time.monotonic()`** at the moment it saw the member's `(incarnation, heartbeat)` go up. Never another node's clock. |
| Q2 | SUSPECT / DEAD are **this node's own opinion** from heartbeat age. Only `heartbeat` and `incarnation` matter in gossip; the `state` field of a received row is **ignored**. |
| Q3 + B1 + B2 | **Refutation:** if a node receives *its own* row with `(incarnation, heartbeat)` **strictly greater** than its own pair, it sets `incarnation = received_incarnation + 1`. Covers fast restart in the same ms (B2) and a clock that went backwards. Compare the **pair**, never incarnation alone. |
| Q4 | Gossip peer = **random choice from all other nodes in the config**, DEAD ones included (a healed partition recovers by itself). |
| Q5 | A failed Gossip RPC **changes nothing**. Only heartbeat age decides (DEC-03: one liveness source). |
| Q6 | Merge: per member keep the row with the higher `(incarnation, heartbeat)`; when it goes up, set `last_seen = now`. Ring fingerprint mismatch: **log ERROR once per peer**, keep gossiping. |
| B3 | Gossip RPC timeout = `min(rpc_timeout_ms, gossip_interval_ms)`. Ticks run at a **fixed rate**: next tick = previous tick start + interval, not "after the RPC". |
| B4 | **SUSPECT after 5 s** (5 ticks), DEAD after 10 s (unchanged). Shared change → DEC-13 (§8). |
| B5 | `subscribe` callbacks: plain functions, called from the gossip loop after the table is updated, must be quick and must not await, fire on **every** change (flaps included). A callback that raises is logged; the others still run and the loop goes on. |
| B6 | `gossip.py` = timing, peer choice, RPC. `membership.py` = pure logic with `now` passed in, testable without a network. |
| B7 | `gossip.py` sends through one module-level function `send_gossip(...)` so tests can patch it to cut node pairs (partition test). |
| — | DEAD → ALIVE happens **directly** when the heartbeat goes up (no stop at SUSPECT). Own row is always ALIVE and is never overwritten by gossip. Rows for node ids not in the config are ignored with a WARNING (once per id). At boot, `last_seen = now` for every member (DEC-03: timers start at boot). |

### Why refutation is `>` and not `>=` (correction)

Peers send my **own current row** back to me all the time, so "equal to mine" is the normal case. With `>=` a node would bump its incarnation on almost every gossip round. Only a row that is **newer than anything I have** (an older life of mine with a higher heartbeat, or a higher incarnation from before a clock jump) means "the cluster believes something about me that is not my current life" → refute.

Fast restart check: old life `(T, 57)`, new life `(T, 3)`. Peer sends `(T, 57)` → `57 > 3` → new incarnation `T+1` → my new row beats the old one everywhere. If the old heartbeat happened to be *equal* to mine, nothing is needed: my next tick makes mine bigger and it wins normally.

## 3. Data kept per member

| Field | Where | Gossiped? | Meaning |
|---|---|---|---|
| `heartbeat` | `Member` proto | yes | owner raises by 1 every tick; starts at 0 on boot |
| `incarnation` | `Member` proto | yes | owner's boot time in ms; raised only by refutation |
| `ring_fingerprint` | `Member` proto | yes | from the owner; only used for the mismatch check |
| `state` | `Member` proto | sent, **ignored** on receive | this node's opinion; shown by ClusterInfo / `kv info` |
| `last_seen` | `self._last_seen: dict[str, float]` | no | `time.monotonic()` when the pair last went up |

## 4. `membership.py` — new and changed methods

Existing frozen methods stay (`state`, `is_alive`, `alive_nodes`, `members`, `address`, `subscribe`). New public methods (add to my section of `INTERFACES.md`, tell Meet — not a shared-file change):

```python
def tick(self) -> None                                  # own heartbeat += 1
def table(self) -> GossipMessage                         # sender = self_id, members = members()
def merge(self, msg: GossipMessage, now: float) -> None  # §4.2
def check_states(self, now: float) -> None               # §4.3, fires callbacks
def on_gossip(self, msg: GossipMessage) -> GossipMessage # merge(msg, time.monotonic()); return table()
```

### 4.1 `start` / `stop`

```python
def boot_ms() -> int:                     # module-level, so a test can patch it (fast-restart test)
    return int(time.time() * 1000)

async def start(self) -> None:
    now = time.monotonic()
    self._members[self.self_id].incarnation = boot_ms()
    self._last_seen = {node_id: now for node_id in self._members}
    self._task = asyncio.create_task(gossip_loop(self))

async def stop(self) -> None:
    if self._task is not None:
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None
```

`stop()` must be safe to call twice and before `start()` (the test fixture's `kill` and `stop` both call it).

### 4.2 `merge(msg, now)`

```python
def merge(self, msg, now):
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
        # row.state is ignored (Q2); address always comes from the config

def _maybe_refute(self, row):
    me = self._members[self.self_id]
    if (row.incarnation, row.heartbeat) > (me.incarnation, me.heartbeat):
        old = me.incarnation
        me.incarnation = row.incarnation + 1
        logger.info("refuting stale row about me: incarnation %d -> %d", old, me.incarnation)

def _check_fingerprint(self, row):
    if row.ring_fingerprint and row.ring_fingerprint != self.ring_fingerprint:
        self._warn_once("fingerprint", row.node_id,
                        "ring fingerprint of %s is %s, mine is %s (different cluster.yaml?)",
                        row.node_id, row.ring_fingerprint, self.ring_fingerprint, level=logging.ERROR)
```

`_warn_once(kind, node_id, ...)` keeps a `set[(kind, node_id)]` so each problem is logged once per peer, not every tick. An empty fingerprint (peer not heard from yet) is not a mismatch.

### 4.3 `check_states(now)`

```python
def check_states(self, now):
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

def _notify(self, node_id, old, new):
    for callback in self._callbacks:
        try:
            callback(node_id, old, new)
        except Exception:
            logger.exception("membership callback failed for %s", node_id)
```

The log line matches the D0-P format example: `gossip: node2 ALIVE -> SUSPECT`.

## 5. `gossip.py`

```python
async def send_gossip(membership, peer_id: str, msg: GossipMessage) -> GossipMessage:
    """The one place a Gossip RPC is sent. Tests patch this to cut links."""
    return await membership.peers.stub(peer_id).Gossip(msg)

async def gossip_once(membership, timeout_s: float) -> None:
    membership.tick()
    others = [n for n in membership.config.nodes if n != membership.self_id]
    if others:
        peer = random.choice(others)
        try:
            reply = await asyncio.wait_for(send_gossip(membership, peer, membership.table()), timeout_s)
            membership.merge(reply, time.monotonic())
        except (grpc.aio.AioRpcError, asyncio.TimeoutError) as error:
            logger.debug("gossip to %s failed: %s", peer, error)   # Q5: changes nothing
    membership.check_states(time.monotonic())

async def gossip_loop(membership) -> None:
    cfg = membership.config
    interval_s = cfg.gossip_interval_ms / 1000
    timeout_s = min(cfg.rpc_timeout_ms, cfg.gossip_interval_ms) / 1000          # B3
    next_tick = time.monotonic()
    while True:
        try:
            await gossip_once(membership, timeout_s)
        except Exception:
            logger.warning("gossip round failed", exc_info=True)                  # loop never dies
        next_tick += interval_s                                                   # B3: fixed rate
        now = time.monotonic()
        if next_tick < now:          # fell behind (e.g. a slow round): do not burst to catch up
            next_tick = now
        await asyncio.sleep(next_tick - now)
```

- `asyncio.wait_for` (not only the gRPC `timeout=`) so a patched/hanging `send_gossip` is also cut off.
- A failed RPC is logged at DEBUG: dead peers get picked every few ticks and WARNING would flood the log.
- `CancelledError` is not caught by `except Exception` in 3.11, so `stop()` still works.
- Meet's `replica.py` Gossip handler already calls `membership.on_gossip(request)`: no change needed there.

## 6. Edge cases

| Case | What happens | Test |
|---|---|---|
| Peer sends my own current row back (equal pair) | Nothing (refutation needs `>`) | `test_equal_own_row_no_refute` |
| Restart in the same ms (same incarnation, heartbeat back to 0) | Peer sends old `(T, 57)` → I go to `T+1` → my row wins everywhere | unit + `test_fast_restart_same_incarnation` |
| Clock went backwards (new incarnation smaller) | Old row has higher incarnation → refute → `old + 1` | `test_refute_higher_incarnation` |
| Row with higher incarnation but lower heartbeat | Wins (new life of that node) | `test_merge_higher_incarnation_wins` |
| Stale or equal row | Ignored, `last_seen` unchanged | `test_merge_stale_ignored` |
| Received row has `state = DEAD` for a live node | Ignored (Q2) | `test_merge_ignores_state` |
| Unknown node id in gossip | Ignored, one WARNING | `test_merge_unknown_node` |
| Ring fingerprint differs | One ERROR per peer, rows still merged | `test_fingerprint_mismatch_logged_once` |
| Right after boot, no gossip yet | Everyone ALIVE (`last_seen = boot`) | `test_all_alive_after_start` |
| DEAD node's heartbeat rises | DEAD → ALIVE directly, one callback | `test_dead_to_alive_direct` |
| Callback raises | Logged; other callbacks still called; state still changed | `test_callback_error_isolated` |
| Gossip RPC fails / times out | DEBUG log, no state change, states still re-checked | `test_rpc_failure_changes_nothing` |
| Peer hangs | `wait_for` cuts it at `min(rpc, interval)`; own heartbeat keeps its rate | `test_hanging_peer_does_not_stall_ticks` |
| Network split heals | Peers are picked from all nodes, so the halves find each other again | `test_partition_heals` |
| `stop()` twice / before `start()` | No error | `test_stop_idempotent` |

## 7. Tests

### `tests/test_membership.py` (unit, fake `now`, no network)

Remove `test_gossip_not_built_yet` (it expects `NotImplementedError`). Build a `Membership` from a 3-node config with a dummy peers object, call `start` logic by hand or set `_last_seen` directly, then feed `GossipMessage`s to `merge` with chosen `now` values.

- Merge rule, table-driven over `(mine, received) → kept, last_seen updated?`:
  `(5,10) vs (6,0)` → `(6,0)` yes · `(5,10) vs (5,11)` → `(5,11)` yes · `(5,10) vs (5,10)` → no · `(5,10) vs (5,9)` → no · `(5,10) vs (4,99)` → no.
- Refutation: own `(T,3)`, received `(T,57)` → `T+1`; received `(T+5,0)` → `T+6`; received `(T,3)` → unchanged; received `(T,2)` → unchanged. Own row never overwritten.
- `check_states` with `suspect=5000, dead=10000`: age 4.9 s ALIVE, 5.1 s SUSPECT, 10.1 s DEAD; then a newer row → ALIVE directly.
- Callbacks: exactly one call per change with `(node_id, old, new)`; a raising callback doesn't stop the next one (`caplog`).
- Received `state` ignored; unknown id → WARNING once; fingerprint mismatch → ERROR once over many merges (`caplog`).
- `table()` has `sender = self_id` and all members; `on_gossip` merges then returns our table.

### `tests/test_gossip.py` (in-process cluster, test timers)

Always use `cluster.wait_until(...)`, never "all ALIVE at this instant" (flaps are possible by design).

- All 5 nodes see all 5 ALIVE (`ClusterInfo` states) — `wait_until`, 3 s.
- `kill("node3")` → every other node shows node3 SUSPECT, then DEAD (`wait_until`, 3 s each); `kv info`/ClusterInfo shows the states.
- `restart("node3")` → every node shows node3 ALIVE again (`wait_until`, 3 s).
- Fast restart: read node3's incarnation, `kill`, `monkeypatch.setattr(membership_module, "boot_ms", lambda: old_incarnation)`, `restart` → node3 ends with incarnation `> old` and is ALIVE everywhere.
- Partition: patch `dynamo.gossip.send_gossip` to raise for pairs across `{node1,node2} | {node3,node4,node5}` → node1 sees node3 DEAD; remove patch → all ALIVE again.
- Hanging peer: patch `send_gossip` to `await asyncio.sleep(10)` for one peer → the sender's own heartbeat still rises about once per interval (check over ~1 s).
- Subscribe in a cluster: callback on node1 records `node3: ALIVE→SUSPECT→DEAD` after a kill.

## 8. Shared changes (DEC-13 — Meet must agree)

**DEC-13 — Gossip refutation rule and SUSPECT timeout** (from D6a)
- Decision: (1) a node that receives its own row with a strictly greater `(incarnation, heartbeat)` sets `incarnation = received + 1`; it replaces the SPEC sentence "a node seeing itself marked SUSPECT bumps its own incarnation". (2) `suspect_after_ms` default 3000 → **5000**.
- Why: (1) states are each node's own opinion (never gossiped), so "seeing itself SUSPECT" can never happen; the new rule fixes fast restarts in the same ms and clock jumps. (2) simulation of a healthy 5-node cluster: ~6–7 false SUSPECTs per 1000 ticks at 3 ticks, 0 at 5 ticks; DEAD never fired falsely.
- Rejected: gossiping states (SWIM) — needs rules on who may overrule whom. Persisting the last incarnation to disk — more code; refutation covers the same cases.
- Affects: SPEC (gossip merge rule, settings table, phase-3 text "ALIVE → SUSPECT (3 s)"), `dynamo/config.py`, `cluster.yaml`, `cluster.local.yaml`, D5 unaffected (SUSPECT only reorders routes).

Files to change:
- `dynamo/config.py`: `suspect_after_ms: int = 5000`.
- `cluster.yaml`, `cluster.local.yaml`: `suspect_after_ms: 5000`.
- `.team/SPEC.md`: settings table + merge-rule sentence + phase text.
- `tests/conftest.py` (mine): `TEST_TIMERS["suspect_after_ms"] = 500` (5 ticks of 100 ms). Also update D0-P §4.9 test timers line.
- Validation `suspect < dead` still holds (5000 < 10000, 500 < 1000).

## 9. For Meet (send with DEC-13)

1. DEC-13 above — please confirm (SPEC + config + yamls are shared).
2. `subscribe` contract for D7 (B5): plain function `cb(node_id, old, new)`, called from the gossip loop after the table changes, must be quick and must not await (start a task if you need async work), fires on every change including short SUSPECT flaps.
3. States: `is_alive` = ALIVE only. SUSPECT can flap briefly on a healthy cluster (rarer now with 5 s); your D5 route order (ALIVE → SUSPECT → DEAD) already handles that.
4. No change needed in `replica.py` for 6a: the Gossip handler already calls `on_gossip`.
5. New public methods on `Membership`: `tick`, `table`, `merge(msg, now)`, `check_states(now)` (my INTERFACES section).

## 10. Done when (6a)

- Kill a node → all others SUSPECT by ~5 s, DEAD by ~10 s (tests: ~0.5 s / ~1 s); restart → ALIVE.
- Merge-rule and refutation unit tests pass, including the equal-row case.
- `kv info` shows SUSPECT / DEAD for a killed node.
- `make test` green; 6a section "As built" filled; status stays IMPLEMENTING (D6 is one task — REVIEW only after 6c).

---

## 6a — As built

Built 2026-11-21. Commits `056d14d` (config, shared), `ca1cbd5` (membership, gossip, tests), `685c1bf`
(Meet's `tests/test_server.py`). Files: `dynamo/membership.py`, `dynamo/gossip.py`,
`tests/test_membership.py` (36), new `tests/test_gossip.py` (10), `tests/conftest.py` (suspect 500 ms),
`dynamo/config.py` + `cluster.yaml` + `cluster.local.yaml` (suspect 5000, DEC-13). Full suite 292
passed, 1 skipped, 5 runs in a row.

Built as designed: §2–§5 (merge rule, `>` refutation, local states from monotonic heartbeat age,
DEAD → ALIVE direct, callbacks isolated, peer from all other nodes, failed RPC changes nothing,
`wait_for` timeout `min(rpc, interval)`, fixed-rate ticks, `send_gossip` patch point, `boot_ms`).

What differs, and why:
- **The first gossip round runs one interval after `start()`, not at once.** Found when the full
  suite ran: nodes start one after another, so a round at t=0 mostly hit peers that were not
  listening yet; gRPC then held that channel in reconnect backoff (Meet's max 1 s), and Meet's D5
  cluster tests, which write through the same channels right after startup, failed with
  UNAVAILABLE / QUORUM_FAILED (4 tests, flaky). Starting 5 test nodes takes ~4 ms, far below one
  interval (100 ms in tests, 1 s in production). Timers still start at boot (DEC-03); only the first
  heartbeat goes out one interval later.
- **`_last_seen` is also filled in `__init__`** (monotonic now), not only in `start()`, so
  `check_states` works on a Membership that was never started (unit tests); `start()` resets it.
- **The state-change log line reads `membership: node3 ALIVE -> SUSPECT`**, not `gossip: …`: the log
  format prints the source module, and `check_states` is in `membership.py` (§4.3).
- **Kill / SUSPECT cluster tests record the changes with `subscribe`** on every other node instead of
  polling ClusterInfo for SUSPECT on all of them at once (the SUSPECT window is only 500 ms per node
  and the nodes' windows are offset, so polling could miss it). One extra test polls ClusterInfo on
  node1 for SUSPECT; others poll ClusterInfo for DEAD and ALIVE.
- **Extra tests:** `test_empty_fingerprint_is_not_a_mismatch`, `test_own_row_never_overwritten_or_suspected`,
  `test_tick_raises_own_heartbeat`, `test_slow_rpc_is_cut_by_timeout` (unit, no cluster).
- **Removed** `test_gossip_not_built_yet` (as §7 says) and the harness test `test_gossip_loop_is_a_stub`.
- **Meet's `tests/test_server.py`:** `test_stub_rpcs_unimplemented` used Gossip as its example of an
  unbuilt RPC; it now checks that Gossip answers with the member table
  (`test_gossip_answers_with_its_table`). The "unbuilt RPC → UNIMPLEMENTED" path is still covered by
  his `test_hinted_replica_put_unimplemented`.

Live check (5 local processes, `tools/cluster_local.sh`, production timers): `kv info` on node1 showed
node3 SUSPECT 6.5 s after `kill -9` and DEAD at 11.5 s (log: `ALIVE -> SUSPECT` 5.0 s and
`SUSPECT -> DEAD` 10.0 s after the last heartbeat); after a restart, ALIVE within 3 s with a new
incarnation.

---

# 6b — Hinted handoff

Moved to Meet (DEC-14). Design and as-built notes: `.team/design/D6b-hinted-handoff.md`. What 6c relies on from it: hints are stored **apart** from own data (`storage._hints`, `hints.json`), so `storage.keys()` never lists them and they never enter a Merkle tree.

---

# 6c — Merkle anti-entropy

Scope of 6c: new `dynamo/merkle.py` (pure tree functions), new `dynamo/antientropy.py` (the loop + the two server-side functions), `dynamo/storage.py` (`keys()`), `tests/conftest.py` (test timer + `kill` stops the loop), new `tests/test_merkle.py`, new `tests/test_antientropy.py`. Shared changes (proto, config, `server.py` wiring, `replica.py` handlers) need **DEC-15** (§17).

## 11. What 6c does (one paragraph)

Hinted handoff and read repair fix most differences, but not all: a hint can die with its stand-in, a key that is never read is never read-repaired, and a write can reach fewer than N replicas. So in the background, every node, once per interval, picks one partner node and checks whether **the keys they both own** are identical. Each side builds a small hash tree over those keys. If the two root hashes are equal (the normal case), the round ends after one RPC. If not, the sides find the few buckets whose hashes differ and swap only those buckets' keys and versions, merging them with `storage.apply` (safe in any order, any number of times — DEC-11). Repeat rounds until the roots match.

## 12. Which pairs, and what one tree covers

- A tree is built **per pair of nodes, over the keys both of them own**: key `k` is in the tree of pair (A, B) when both A and B are in `ring.preference_list(k, n)`.
- Both sides apply the **same filter** to their own data, so both trees cover the same "should-be-identical" set. A key one side is missing still changes that side's bucket hash → it gets found.
- **Which pairs share keys:** every pair. With 5 nodes and N = 3, each key has 3 owners = 3 pairs out of 10, so each pair shares about **30 %** of all keys (measured on our ring, 32 vnodes: 22–33 % per pair). Each node is in 4 pairs.
- The tree is **not stored**. It is rebuilt from the storage dict at the start of each round for the chosen partner (measured: ~60 ms for 10k keys, mostly `preference_list`).
- Rejected: one tree per ring range (Dynamo paper / Cassandra). Needs a key-range API in Meet's `ring.py` and range bookkeeping per vnode; the pair filter is one line on top of `preference_list` and gives the same result for our cluster size.

## 13. Decisions (final)

| # | Decision |
|---|---|
| Q1 | One tree **per pair**, over keys whose preference list contains both nodes (§12). Rebuilt each round. |
| Q2 | Key digest = `sha256` of a fixed JSON encoding of `[key, sorted(version encodings)]`, version encoding = `json.dumps(version_to_json(v), sort_keys=True)`. **Tombstones in** (deletes must spread, DEC-06). **Hints out** (they live apart in `_hints`; `storage.keys()` lists own data only). Full 32-byte sha256 everywhere (no truncation: one less thing to defend). |
| Q3 | Fixed **256 buckets** (`BUCKETS = 256`, constant in `merkle.py`, not config): `bucket = first 2 bytes of sha256(key) mod 256`. Shape: **root → 256 bucket hashes → key digests**. ~10k keys → ~2.8k shared per pair → ~11 keys per bucket. |
| Q4 | **Rebuild every round** from the dict, in one synchronous pass (no `await` inside, so no write can change the dict half way). No incremental updates. |
| Q5 | Two RPCs: **`CompareTree`** sends the root only; reply = "same", or all 256 bucket hashes. Then **`ExchangeBuckets`** sends my entries for the differing buckets and gets the partner's entries for the same buckets back. Identical replicas: **one RPC, root only**. |
| Q6 | **Both directions in one round** (push-pull, like gossip): the partner applies what I send, I apply what it returns. |
| Q7 | New config key **`anti_entropy_ms`** (default **10000**). Partner = **round-robin over the other nodes** (sorted ids), skipping any that is not ALIVE. First round **one interval after start** (not at boot). Every pair is checked by each side at least once every 4 intervals. |
| Q8 | Proto messages / RPCs in §17; handlers in Meet's `replica.py` are one line each and call `node.anti_entropy.on_compare(...)` / `on_exchange(...)`. |
| — | **At most 32 differing buckets per round** (`MAX_BUCKETS_PER_ROUND = 32`). A node that lost all its data catches up over ~8 rounds instead of one huge message (gRPC's default limit is 4 MB). |
| — | Receiving side applies only keys **it owns together with the sender** (same filter); anything else is skipped with one WARNING per sender. |
| — | `ring_fingerprint` is sent in `CompareTree`. Mismatch → reply "same" (nothing to sync with a node that has a different ring) + one ERROR per sender. Syncing across different rings would copy keys to non-owners. |
| — | A failed or timed-out RPC ends the round (DEBUG log); the loop never dies (same pattern as gossip). Each RPC uses `rpc_timeout_ms`. |
| — | Rounds started by A and by B at the same time are fine: `apply` is idempotent and order-free. |

### Why round-robin and not random (Q7)

Random (like gossip) works, but a pair can go unchecked for a long time by bad luck. Round-robin over 4 partners guarantees each pair is compared within 4 intervals by each side, so "converges within a few rounds" is a promise, not a probability — and tests can wait a fixed bound.

## 14. `dynamo/merkle.py` — pure functions (no I/O, no asyncio)

```python
BUCKETS = 256

@dataclass(frozen=True)
class Tree:
    root: bytes
    buckets: list[bytes]          # BUCKETS hashes, index = bucket id

def bucket_of(key: str) -> int:
    return int.from_bytes(hashlib.sha256(key.encode()).digest()[:2], "big") % BUCKETS

def key_digest(key: str, versions: list[Version]) -> bytes:
    """Same key + same set of versions (any order) -> same digest."""
    encoded = sorted(json.dumps(version_to_json(v), sort_keys=True) for v in versions)
    return hashlib.sha256(json.dumps([key, encoded]).encode()).digest()

def build(entries: dict[str, list[Version]]) -> Tree:
    per_bucket: list[list[tuple[str, bytes]]] = [[] for _ in range(BUCKETS)]
    for key, versions in entries.items():
        per_bucket[bucket_of(key)].append((key, key_digest(key, versions)))
    buckets = [hashlib.sha256(b"".join(d for _, d in sorted(items))).digest()
               for items in per_bucket]
    return Tree(root=hashlib.sha256(b"".join(buckets)).digest(), buckets=buckets)

def diff(mine: list[bytes], theirs: list[bytes]) -> list[int]:
    """Bucket ids whose hashes differ. Raises ValueError if the lengths differ."""
    if len(mine) != len(theirs):
        raise ValueError(f"bucket count {len(mine)} != {len(theirs)}")
    return [i for i, (a, b) in enumerate(zip(mine, theirs)) if a != b]
```

- Keys inside a bucket are sorted, so insertion order never matters.
- An empty bucket hashes `sha256(b"")`; two empty trees are equal.
- `version_to_json` is imported from `storage.py` (already deterministic: clock via `vclock.to_json`, sorted).
- `key_digest` covers counters, `updated_ms`, value and tombstone flag — the same things `version.fingerprint` covers, so "same digest" = "same fingerprint".

## 15. `dynamo/storage.py` — one new method

```python
def keys(self) -> list[str]:
    """Keys of this node's own data (tombstoned keys included, hints not included)."""
    self._require_loaded()
    return list(self._data)
```

## 16. `dynamo/antientropy.py`

```python
MAX_BUCKETS_PER_ROUND = 32

class AntiEntropy:
    def __init__(self, self_id, config, storage, ring, membership, peers) -> None: ...
    async def start(self) -> None          # create task running _run()
    async def stop(self) -> None           # cancel + await; safe twice / before start
    async def sync_once(self) -> bool      # one round with the next partner; True if anything was exchanged
    def on_compare(self, req: TreeRoot) -> TreeBuckets          # server side, called by replica.py
    async def on_exchange(self, req: BucketData) -> BucketData   # server side, called by replica.py
```

### 16.1 Helpers

```python
def _owns_both(self, key: str, peer: str) -> bool:
    owners = self.ring.preference_list(key, self.config.n)
    return self.self_id in owners and peer in owners

def _shared(self, peer: str) -> dict[str, list[Version]]:
    return {k: self.storage.get(k) for k in self.storage.keys() if self._owns_both(k, peer)}

def _entries(self, peer: str, bucket_ids: list[int]) -> list[KeyVersions]:
    wanted = set(bucket_ids)
    return [KeyVersions(key=k, versions=[version_to_proto(v) for v in vs])
            for k, vs in self._shared(peer).items() if merkle.bucket_of(k) in wanted]

async def _apply_entries(self, peer: str, entries) -> int:
    changed = 0
    for entry in entries:
        if not self._owns_both(entry.key, peer):
            self._warn_once(peer, "%s sent key %s that we do not share; skipped", peer, entry.key)
            continue
        for v in entry.versions:
            if await self.storage.apply(entry.key, version_from_proto(v)):
                changed += 1
    return changed
```

### 16.2 The loop and one round (client side)

```python
async def _run(self) -> None:
    interval_s = self.config.anti_entropy_ms / 1000
    while True:
        await asyncio.sleep(interval_s)            # first round one interval after start
        try:
            await self.sync_once()
        except Exception:
            logger.warning("anti-entropy round failed", exc_info=True)

def _next_partner(self) -> str | None:
    """Round-robin over the other nodes (sorted), skipping those that are not ALIVE."""
    others = sorted(n for n in self.config.nodes if n != self.self_id)
    for _ in range(len(others)):
        peer = others[self._cursor % len(others)]
        self._cursor += 1
        if self.membership.is_alive(peer):
            return peer
    return None

async def sync_once(self) -> bool:
    peer = self._next_partner()
    if peer is None:
        return False
    timeout = self.config.rpc_timeout_ms / 1000
    stub = self.peers.stub(peer)
    tree = merkle.build(self._shared(peer))
    try:
        reply = await stub.CompareTree(TreeRoot(sender=self.self_id, root=tree.root,
                                                ring_fingerprint=self.ring_fingerprint), timeout=timeout)
        if reply.same:
            return False
        differing = merkle.diff(tree.buckets, list(reply.buckets))[:MAX_BUCKETS_PER_ROUND]
        if not differing:
            return False                   # data changed between the two builds; next round
        answer = await stub.ExchangeBuckets(BucketData(sender=self.self_id, buckets=differing,
                                                       entries=self._entries(peer, differing)), timeout=timeout)
    except grpc.aio.AioRpcError as error:
        logger.debug("anti-entropy with %s failed: %s", peer, error.code())
        return False
    changed = await self._apply_entries(peer, answer.entries)
    logger.info("anti-entropy with %s: %d buckets differed, got %d keys, %d versions changed here",
                peer, len(differing), len(answer.entries), changed)
    return True
```

`self.ring_fingerprint = ring.fingerprint(config.n)` (same value membership uses).

### 16.3 Server side (called by Meet's handlers)

```python
def on_compare(self, req: TreeRoot) -> TreeBuckets:
    if req.ring_fingerprint != self.ring_fingerprint:
        self._warn_once(req.sender, "ring of %s differs from mine; not syncing", req.sender, level=logging.ERROR)
        return TreeBuckets(same=True)
    tree = merkle.build(self._shared(req.sender))
    if tree.root == req.root:
        return TreeBuckets(same=True)
    return TreeBuckets(same=False, buckets=tree.buckets)

async def on_exchange(self, req: BucketData) -> BucketData:
    bucket_ids = list(req.buckets)[:MAX_BUCKETS_PER_ROUND]
    mine = self._entries(req.sender, bucket_ids)       # read BEFORE applying theirs
    await self._apply_entries(req.sender, req.entries)
    return BucketData(sender=self.self_id, buckets=bucket_ids, entries=mine)
```

Reading `mine` before applying is not required for correctness (merge is order-free) — it just keeps the reply to "what I had", which is easier to reason about in logs.

### 16.4 The round, as a picture

```
A (picks B, round-robin)                         B
build tree(A∩B keys)
CompareTree(root_A) ───────────────────────────► build tree(B∩A keys)
                                                 root_B == root_A ?  → same=True   (end: 1 RPC)
                     ◄─────────────────────────── same=False + 256 bucket hashes
diff → buckets [7, 91]
ExchangeBuckets([7,91], A's keys in 7,91) ─────► mine = B's keys in 7,91
                                                 apply A's versions (storage.apply)
                     ◄─────────────────────────── mine
apply B's versions (storage.apply)
next round with this pair: roots equal → 1 RPC
```

## 17. Shared changes (DEC-15 — Meet must agree)

**DEC-15 — Merkle anti-entropy: RPCs, config key, wiring** (from D6c)
- Decision: two new `NodeService` RPCs `CompareTree` and `ExchangeBuckets`; config `anti_entropy_ms` (default 10000); `Node` gets an `anti_entropy` part started after `handoff` and stopped before `coordinator.drain`; handlers in `replica.py` call `node.anti_entropy`.
- Why: replicas that drift (lost hint, unread key, write to < N replicas) must converge without a read (SPEC C11).
- Rejected: per-range trees (needs a range API in `ring.py`); one RPC with all bucket hashes every round (identical replicas would send 256 hashes for nothing); sending whole data sets (no tree at all).
- Affects: `proto/dynamo/dynamo.proto`, `dynamo/replica.py`, `dynamo/server.py`, `dynamo/config.py`, `cluster.yaml`, `cluster.local.yaml`, SPEC settings table, INTERFACES.

### Proto (add to `dynamo.proto`)

```proto
service NodeService {
  ...
  rpc CompareTree(TreeRoot) returns (TreeBuckets);          // phase 4, anti-entropy
  rpc ExchangeBuckets(BucketData) returns (BucketData);     // phase 4, anti-entropy
}

message TreeRoot {
  string sender = 1;
  string ring_fingerprint = 2;
  bytes root = 3;                       // sha256 of the 256 bucket hashes
}
message TreeBuckets {
  bool same = 1;                        // true: roots equal (or rings differ) -> nothing to do
  repeated bytes buckets = 2;           // 256 bucket hashes, only when same = false
}
message KeyVersions {
  string key = 1;
  repeated Version versions = 2;        // every version, tombstones included
}
message BucketData {
  string sender = 1;
  repeated uint32 buckets = 2;          // bucket ids being exchanged
  repeated KeyVersions entries = 3;     // sender's keys in those buckets
}
```

### Meet's files (send him exactly this)

`replica.py` — two handlers, same decorator as the others:
```python
@rpc_handler("node")
async def CompareTree(self, request, context):
    """Compare Merkle roots for the keys we share with the sender (6c)."""
    return self._node.anti_entropy.on_compare(request)

@rpc_handler("node")
async def ExchangeBuckets(self, request, context):
    """Swap the keys of the differing buckets (6c)."""
    return await self._node.anti_entropy.on_exchange(request)
```

`server.py`:
- `Node` gets a field `anti_entropy: AntiEntropy`.
- `build_node`: `anti_entropy = AntiEntropy(node_id, config, storage, ring, membership, peers)`.
- `start()`: `await self.anti_entropy.start()` after `handoff.start()`.
- `stop()`: `await self.anti_entropy.stop()` after `handoff.stop()`.

### Config + yamls

- `dynamo/config.py`: `anti_entropy_ms: int = 10000`; validation: `> 0`.
- `cluster.yaml`, `cluster.local.yaml`: `anti_entropy_ms: 10000   # Merkle sync with one partner`.
- SPEC settings table: add the row.

### Mine

- `tests/conftest.py`: `TEST_TIMERS["anti_entropy_ms"] = 60000` (effectively off, so Meet's read-repair tests still see stale replicas); 6c tests opt in with `make_cluster(5, anti_entropy_ms=200, sloppy_quorum=False)`.
- `Cluster.kill`: add `await node.anti_entropy.stop()` after `handoff.stop()`.
- INTERFACES: my section gets `merkle.py`, `antientropy.py`, `storage.keys()`.

## 18. Edge cases

| Case | What happens | Test |
|---|---|---|
| Replicas identical | `CompareTree` says same → 1 RPC, no `ExchangeBuckets` | `test_identical_only_root` |
| Key missing on one side | Its bucket differs → the missing side receives it | `test_missing_key_both_directions` |
| Different values, concurrent clocks | Both get both → same siblings on both | `test_siblings_converge` |
| Tombstone on one side only | Tombstone travels; old value dropped where it is BEFORE | `test_delete_spreads` |
| Same counters, different `updated_ms` | Digest differs → exchanged → `reconcile` takes max → equal | unit `test_digest_includes_updated_ms` |
| Hints on a stand-in | Not in `storage.keys()` → never in any tree | `test_hints_not_in_tree` |
| Key this node does not own in its dict | Filtered out of every tree | `test_non_owned_key_ignored` |
| Partner sends a key we do not share | Skipped, one WARNING | `test_exchange_skips_unshared` |
| Ring fingerprint differs | `same=True` + one ERROR; nothing copied | `test_ring_mismatch_no_sync` |
| > 32 buckets differ (node wiped) | 32 per round; converges over more rounds | `test_large_drift_converges` |
| Partner not ALIVE | Skipped by round-robin | `test_skips_dead_partner` |
| Partner dies mid-round | RPC error → DEBUG, round ends, loop continues | `test_partner_down_loop_survives` |
| A write lands between A's build and B's | Roots or buckets differ for a moment → fixed next round | (covered by random test) |
| A→B and B→A rounds at the same time | Both apply; merge is idempotent and order-free | (covered by random test) |
| Both trees empty | Equal roots → same | unit `test_empty_trees_equal` |

## 19. Tests

### `tests/test_merkle.py` (pure)

- Same entries inserted in different orders, versions in different list orders → same root and buckets.
- Change one value → root differs and `diff` returns exactly `[bucket_of(key)]`.
- Remove one key → only its bucket differs. Tombstone vs no key → differs.
- Same counters, different `updated_ms` → digest differs.
- `bucket_of` is in `range(256)` and stable; empty trees equal; `diff` raises on length mismatch.
- Random: 200 random keys/versions, two random insertion orders → equal trees.

### `tests/test_antientropy.py` (cluster: `make_cluster(5, anti_entropy_ms=200, sloppy_quorum=False)`, no client reads)

Inject differences with `cluster.replica(node).ReplicaPut(...)` straight to **one** owner (no coordinator, no read repair, no hints), then check owners with `LocalGet` (a local read — no read repair).

- **Identical → root only:** after writes through the coordinator, patch/count `ExchangeBuckets` calls on all nodes over a few rounds → 0.
- **Missing key:** put `k` only on owner 1 → `wait_until` all 3 owners have it (3 s).
- **Stopped replica:** `stop_node(owner3)`, write 20 keys via coordinator (W = 2 succeeds), `restart(owner3)`, no reads → `wait_until` owner3 has all 20 (5 s: the others skip owner3 until gossip shows it ALIVE again, ~1 s).
- **Delete spreads:** tombstone on one owner only → all owners end with the tombstone.
- **Siblings:** two concurrent versions on two different owners → all three end with both.
- **Hints untouched:** a stand-in's `hints()` content is unchanged by anti-entropy rounds.
- **Random convergence:** 300 random ReplicaPuts (random key, random single owner, random clocks incl. tombstones) → `wait_until` for every key all owners have the same `version.fingerprint` (10 s).
- **Large drift:** put 1000 keys on one node only (so nearly all 256 buckets differ for its pairs) → converges over several rounds, at most 32 buckets per round (`wait_until`, 15 s).

### Unit (`AntiEntropy` with fake storage/ring/peers, no network)

- `_next_partner` round-robin order and skipping non-ALIVE; `None` when nobody is ALIVE.
- `on_compare`: equal root → same; different → 256 buckets; ring mismatch → same + ERROR once.
- `on_exchange`: applies only shared keys; returns own entries of the asked buckets; caps at 32.

## 20. For Meet (send with DEC-15)

1. DEC-15 — please confirm (proto, `replica.py`, `server.py`, config, yamls, SPEC).
2. The proto block (§17) and the two handlers + four `server.py` lines (§17), or tell me to add them with your OK, as with D3.
3. Read repair stays as is; anti-entropy is the background safety net for keys nobody reads.
4. Test default `anti_entropy_ms = 60000`, so your read-repair tests still see stale replicas.

## 21. Done when (6c)

- Two replicas made to differ on purpose converge within a few rounds with **no client read**.
- Identical replicas exchange only the root (`ExchangeBuckets` never called).
- Random test: same fingerprint on every owner for every key after sync.
- `make test` green; "As built" for 6c filled; **D6 → REVIEW** (6a + 6c; 6b is Meet's D6b row).

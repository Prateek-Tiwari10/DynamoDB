# D6 — Failure Detection + Replica Sync: Implementation Instructions

D6 has three parts, built and pushed in order: **6a gossip + failure detector** → 6b hinted handoff → 6c Merkle anti-entropy (DEC-12).

This file is updated as each part is decided. **Only 6a is decided. Implement only 6a.**

Note: this file is a design/instructions note — do NOT commit it to the code repo. Save it as `.team/design/D6-failure-detection.md` (notes commit) before coding.

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

_Filled after implementation._

# D10 — Chaos + Cluster Test Suite: Implementation Instructions

Owner: Prateek · Reviewer: Meet · Phase 4 · Needs D8, D6b, D6c (all built) · Status: DESIGNED 2026-11-21

Note: design/instructions note — do NOT commit it to the code repo. Save it as `.team/design/D10-chaos-cluster-tests.md` (notes commit) before coding.

**Before starting:** make a **Python 3.11 venv** (same as Docker) and run everything from it. REVIEW-LATER #17b: Python 3.14 + grpcio sometimes segfaults; a 5-minute chaos run would hit it.

> **Changed 2026-11-21 (Prateek's call):** Python 3.11 cannot be downloaded on this laptop's network
> (`uv python install 3.11` times out). `.venv` was rebuilt on the cached **Python 3.12.15** (grpcio
> 1.85.3): full suite 382 passed, 3 runs out of 3, no segfault. Docker stays on 3.11.

---

## 1. What D10 does (one paragraph)

So far every test runs 5 nodes inside one pytest process with fast timers. D10 runs the **real** cluster — 5 separate OS processes (or Docker containers) with the **real timers** — and attacks it from outside: kill, stop, pause, restart. Part A is 9 scripted tests (the SPEC's 7 scenario tests + a Merkle test; the SPEC's 8th is chaos). Part B is a 5-minute chaos run: clients write and read non-stop while a chaos loop crashes one node at a time; at the end we **prove** that no write the cluster said OK to was lost, and that all replicas converged. The report goes to `results/`.

## 2. Decisions (final)

| # | Decision |
|---|---|
| T1 | **One suite, two targets**: `CLUSTER_TARGET=local` (default — 5 processes via `tools/cluster_local.sh`, works without Docker) or `CLUSTER_TARGET=docker` (`docker compose`). Same tests, same chaos script; only the "how to kill/start" layer differs. |
| T2 | **Real timers** (`cluster.local.yaml` / `cluster.yaml`: gossip 1 s, SUSPECT 5 s, DEAD 10 s, handoff 5 s, anti-entropy 10 s). Slower (~4 min for the tests), but it tests what we demo. No fast-timer config. |
| T3 | **Test 3 (sloppy quorum off)** uses a second config with `sloppy_quorum: false`. The harness restarts the whole cluster when a test needs a different config (`ensure(config)`), so test order does not matter. |
| T4 | Tooling: **`subprocess`** (`cluster_local.sh` / `docker compose`), **no Docker SDK** (no new dependency). |
| T5 | Never sleep a fixed time to "let it settle": **poll** (`wait_until`) for the state the next step needs (ALIVE in every node's view, value on an owner, …). |
| T6 | **9th test**: Merkle anti-entropy — a key on one replica only reaches all 3 owners with **no read**. |
| T7 | Cluster tests are **skipped by `make test`** (skip unless `RUN_CLUSTER=1`), run by **`make cluster-test`**. Chaos is test 8 and also runs alone via **`make chaos`**. |
| T8 | Every test uses **its own fresh keys** (`<test>-<uuid>`), so tests never disturb each other on the shared cluster. |
| C1 | **No-loss proof** = write log + final check (§6). An acked value is **lost** only if it is missing at the end **and** no later acked write by its writer had seen it. **Changed 2026-11-21, see §6: "later write of any outcome that is itself accounted for".** |
| C2 | **Chaos rule: at most 1 node down at any time**; the next fault starts only after the last node is ALIVE again in **every** node's view + 5 s. With N=3, W=2, two nodes down can legitimately fail writes, which would make the test meaningless. |
| C3 | Fault mix: **60 % crash** (`kill -9` / `docker kill`), **25 % clean stop** (`SIGTERM` / `docker stop`), **15 % pause** 3–8 s (`SIGSTOP`/`SIGCONT` / `docker pause`/`unpause` — a frozen, slow node). Down time random **5–20 s** (covers both SUSPECT and DEAD). |
| C4 | Chaos is **reproducible**: one random seed (printed and saved in the report) drives node choice, fault type and timings. |
| C5 | **Small values** (~100 bytes) — known 6c limit with huge values (OPT-2) is out of scope. |

## 3. Files

| File | Owner | What |
|---|---|---|
| `tests/cluster/conftest.py` | me | `RealCluster` harness + fixtures (§4) |
| `tests/cluster/test_cluster_basic.py` | me | tests 1, 2, 5, 6, 7 |
| `tests/cluster/test_cluster_quorum.py` | me | tests 3, 4 |
| `tests/cluster/test_cluster_antientropy.py` | me | test 9 |
| `tests/cluster/test_cluster_chaos.py` | me | test 8 = runs `tools/chaos.py` with `--seconds` and asserts `lost == 0` |
| `tools/chaos.py` | me | load + chaos loop + final check + report (§5–§7) |
| `results/chaos-<date>.md` + `.json` | me | the report |
| `cluster.local.nosloppy.yaml`, `cluster.nosloppy.yaml` | shared → Meet OK | copies with `sloppy_quorum: false` |
| `docker-compose.nosloppy.yml` | shared → Meet OK | override: mount `cluster.nosloppy.yaml`, `CONFIG=cluster.nosloppy.yaml` |
| `tools/cluster_local.sh` | Meet → OK | one line: `CONFIG=${CONFIG:-cluster.local.yaml}` instead of the fixed name |
| `Makefile` | shared → Meet OK | `cluster-test`, `chaos` targets (§9) |

### Alternatives rejected

| Option | Why not |
|---|---|
| Fast-timer config for the real cluster | Tests would not test what we demo (T2). |
| Docker only | Docker is not installed on Prateek's laptop; the local target runs the same tests (T1). |
| Docker SDK for Python | New dependency for what three `docker compose` commands do (T4). |
| Fixed sleeps to "let it settle" | Slow and flaky; poll for the needed state instead (T5). |
| Two nodes down at once during chaos | With N=3, W=2 writes may fail legitimately; the no-loss test would prove nothing (C2). |
| "Superseded only by a later **acked** write" (first C1 wording) | False LOST when the superseding write was stored but its coordinator crashed before replying (outcome `unknown`) — see §6. |

## 4. The harness: `tests/cluster/conftest.py`

```python
class RealCluster:
    """The real 5-node cluster on localhost:50051-55, controlled from outside."""
    def __init__(self, target: str, data_dir: str) -> None
    async def ensure(self, config: str) -> None      # start all with this config (restart all if a different one runs); wait all ALIVE
    async def heal(self) -> None                     # start/unpause any node that is down; wait all ALIVE everywhere
    async def kill(self, node) / stop(node) / start(node) / pause(node) / unpause(node)
    def client(self, node) -> KVStoreStub            # one channel per node, reopened after start
    def peer(self, node) -> NodeServiceStub          # for test 9 (ReplicaPut to one node)
    async def owners(self, key) -> list[str]         # Locate
    async def state_seen_by(self, observer, node) -> MemberState   # ClusterInfo
    async def wait_all_alive(self, timeout=30) -> None              # every running node sees every running node ALIVE
    async def shutdown(self) -> None                 # stop all (end of session)
```

**Two backends behind it** (one small class each):

| Action | local (`cluster_local.sh`, env `DATA_DIR=./data-cluster`, `CONFIG=…`) | docker |
|---|---|---|
| start all / one | `cluster_local.sh start [node]` | `docker compose [-f override] up -d` / `start node` |
| crash | `cluster_local.sh kill node` | `docker compose kill node` |
| clean stop | `cluster_local.sh stop node` | `docker compose stop node` |
| pause / unpause | `os.kill(pid, SIGSTOP/SIGCONT)` with pid from `data-cluster/<node>.pid` | `docker compose pause/unpause node` |
| wipe data | `rm -rf ./data-cluster` | `docker compose down` + `docker compose run --rm --entrypoint sh node1 -c 'rm -rf /data/*'` (files are root-owned on Linux) |

**Fixtures:**
- `cluster` (session): wipes data once, `ensure("normal")`, yields, `shutdown()` at the end.
- `healthy` (function, autouse in cluster tests): `await cluster.heal()` **before** each test — a test that failed half way can't break the next one.
- `strict` (function): `await cluster.ensure("nosloppy")` (test 3 only); the next `healthy` test switches back with `ensure("normal")`.

**Skip unless asked:** in `tests/cluster/conftest.py`, `pytest_collection_modifyitems` adds a skip mark to every item in `tests/cluster/` unless `RUN_CLUSTER=1`. (No `pyproject.toml` change.)

**Safety:** before starting, if something already answers on 50051–55 that we did not start → stop with a clear message (never kill someone else's cluster). Meet's bench uses 50061+, so no clash.

**Timeouts:** every client call `timeout=3` s (request budget is 1.5 s); every wait has a limit and fails with a message saying what it was waiting for.

## 5. The 9 tests

Real timers: SUSPECT 5 s, DEAD 10 s. "Coordinator view" = `state_seen_by(coordinator, node)`.

| # | Test | Steps | Pass when |
|---|---|---|---|
| 1 | Write via node1, read via node4 | Put `k=v` to node1; Get `k` from node4 | OK, values == [v] |
| 2 | Kill 1 node | `owners(k)` for 20 fresh keys; crash `owners[0]` of the first; **straight away and for 15 s** (covers the SUSPECT window and DEAD) put + get all 20 keys through random live nodes; then `start`, `wait_all_alive` | every put and get OK, every get returns the value just written |
| 3 | 2 replicas down, sloppy off | `strict` fixture; key `k`, owners `[a,b,c]`; crash `b` and `c`; Put through `a` | `QUORUM_FAILED` |
| 4 | 2 replicas down, sloppy on | key `k`, owners `[a,b,c]`; crash `b`,`c`; **wait until `a` sees both not ALIVE** (stand-ins are used only then — Meet's D8); Put through `a`; start `b`,`c` | Put OK and `written_to` holds stand-ins; then (≤ 30 s) `LocalGet` on `b` **and** `c` shows the value with **no client Get** (hint delivered) |
| 5 | Siblings | owners `[a,b,c]`; Put `draft` via `a`; Get → ctx; Put `x` with ctx via `a`; crash `a` (so the next write is coordinated by `b` — through the same coordinator the second write would look newer, DEC-02); Put `y` with the **same old ctx** via `b`; Get via `b` | values == {x, y} (2 siblings); then Put `xy` with that Get's ctx → Get == [xy] |
| 6 | Read repair | owners `[a,b,c]`; Put `v1`; crash `c`; Put `v2` **at once** (`c` still ALIVE in views → no stand-in, W=2 by a,b); start `c`; **check `LocalGet(c)` is still `v1`** (if anti-entropy already fixed it, retry with a new key, max 3 tries); **wait until `a` sees `c` ALIVE** (D7: reads skip DEAD nodes); Get via `a` | then `LocalGet(c)` == v2 within 3 s |
| 7 | Failure detector | crash node `x`; poll the 4 others | all show SUSPECT within ≤ 8 s and DEAD within ≤ 14 s (5 s / 10 s + gossip spread); after `start`, all ALIVE within ≤ 5 s; report the measured times |
| 8 | Chaos | run `tools/chaos.py --seconds $CHAOS_SECONDS` (default 300) | `lost == 0`, `converged == true`, report file written |
| 9 | Merkle anti-entropy | key `k`, owners `[a,b,c]`; `peer(a).ReplicaPut(k, version)` (one copy only, a clock `{a:1}`); **no client Get** | within ≤ 60 s `LocalGet` on `b` and `c` show the value (anti-entropy 10 s, round-robin over 4 partners) |

## 6. The no-loss proof (C1) — the heart of D10

### Workload (`tools/chaos.py`, 8 async clients, each sends every request to a **random** node, so coordinators get killed too)

1. **Write-once keys** (half the writes): key `once-<client>-<seq>`, empty context, unique value. Each key is written once.
2. **Overwrite keys** (other half): each client **owns** 25 keys `ow-<client>-<j>` (no other client touches them). Loop: `Get` → remember `seen` = values returned + context → `Put` new unique value `"<client>:<seq>"` with that context.
3. Some plain `Get`s on random own keys (load on reads; a failed Get is only counted, never a loss).

### Write log (one JSON line per Put, kept in memory + `results/chaos-<date>.writes.jsonl`)

`{key, value, outcome, seen}` with `outcome` = `ok` | `failed` (QUORUM_FAILED) | `unknown` (RPC error / timeout: may or may not be stored).

### Final check (after §7 settle)

For each key: `Get(key, r=3)` (all 3 owners must answer) → `final_values`.

An **acked** (`ok`) value `v` of key `k` is:
- **present** if `v ∈ final_values(k)`, else
- **superseded** if a **later acked** write of the same key had `v` in its `seen` (the writer read `v` and replaced it on purpose — its context covered `v`), else
- **LOST** → test fails, listed in the report.

Extra checks (report, and fail the run):
- **last acked write of every key is present** (nothing can supersede it);
- **no unknown value**: every value in `final_values` was written by the load (ok, failed or unknown) — the system never invents data.

Why `failed`/`unknown` writes are not counted: the cluster did not promise anything. They may show up later as siblings — allowed.

Why siblings are fine: a write that reached only one replica (unknown outcome) can become a sibling of a later acked write. Both are kept, nothing is lost — that is Dynamo's deal.

> **Changed 2026-11-21 (Prateek's call) — the rule as built.** The wording above gives a false LOST:
> a client reads acked `v`, writes `w` with that context, and the coordinator crashes *after* storing
> `w` → `w` is `unknown`, yet `w` really replaced `v`; `v` is missing at the end and no *acked* write
> superseded it. A `failed` write can also be stored on one replica and spread. So:
>
> A value `v` of key `k` (any outcome) is **accounted for** if
> - it is **present** in `final_values(k)`, or
> - it is **superseded**: a **later write of the same key, of any outcome**, had `v` in its `seen`,
>   **and that later write is itself accounted for** (checked newest write first, so the chain ends
>   in a present value).
>
> **LOST** = an **acked** value that is not accounted for. The extra check becomes **"the last acked
> write of every key is accounted for"** (present, or replaced by a later stored write).
> A `failed`/`unknown` value that is not accounted for is simply not stored — fine.

### Convergence check (proves handoff + Merkle did their job)

For every key: `LocalGet` on all 3 owners must give the **same set of versions** (compare encoded versions as a set). Poll up to the settle limit; record the time it took.

## 7. Chaos loop and settle

```
seed = --seed or random; rng = Random(seed); print(seed)
start load (8 clients)
until --seconds have passed:
    node  = rng.choice(5 nodes)
    fault = rng.choices([crash, stop, pause], weights=[60, 25, 15])
    down  = rng.uniform(5, 20)          # pause: rng.uniform(3, 8)
    apply fault; sleep(down); start / unpause
    wait_all_alive(timeout=60)           # C2: next fault only after full recovery
    sleep(5)
stop load (let in-flight requests finish, ≤ 3 s)
settle: heal(); wait_all_alive(); poll convergence (§6) up to 120 s
final check (§6) → report
```

- The chaos loop and the load run in the same asyncio program; fault commands run via `asyncio.create_subprocess_exec`.
- If `wait_all_alive` times out → stop chaos, mark the report **ABORTED** (a bug to look at), still run the final check.

## 8. Report: `results/chaos-<YYYYMMDD-HHMM>.md` (+ `.json` with the same numbers)

```
Chaos run 2026-11-22 14:05   target: local   seed: 81723   duration: 300 s
Faults: 11 (crash 7, stop 3, pause 1)   max nodes down at once: 1
Writes: 18,420 attempted · 18,301 ok · 61 failed · 58 unknown   (availability 99.4 %)
Reads:  9,870 attempted · 9,812 ok
Final check: 18,301 acked → 9,402 present, 8,899 superseded, LOST 0
Last acked write present for every key: yes   Unknown values: 0
Converged: yes, 23 s after the last fault (all owners identical for 3,712 keys)
Per fault: time, node, type, down time, time until ALIVE everywhere
```

(Numbers above are only an example of the format.) These numbers are interview material: "5-minute chaos, 11 crashes, 18k acked writes, zero lost."

## 9. Shared changes (tell Meet; one OK)

- `Makefile`:
  ```make
  cluster-test:
  	RUN_CLUSTER=1 $(PYTHON) -m pytest -q tests/cluster
  chaos:
  	$(PYTHON) tools/chaos.py --seconds $${CHAOS_SECONDS:-300}
  ```
  (`CLUSTER_TARGET=docker make cluster-test` for Docker.)
- `tools/cluster_local.sh` (Meet's): `CONFIG=${CONFIG:-cluster.local.yaml}` in `start_node`, so the harness can pass the no-sloppy config.
- New config files `cluster.local.nosloppy.yaml`, `cluster.nosloppy.yaml` (copies, `sloppy_quorum: false`) and `docker-compose.nosloppy.yml`:
  ```yaml
  x-nosloppy: &nosloppy
    environment: { CONFIG: cluster.nosloppy.yaml }
    volumes: [ "./cluster.nosloppy.yaml:/app/cluster.nosloppy.yaml:ro" ]
  services: { node1: *nosloppy, node2: *nosloppy, node3: *nosloppy, node4: *nosloppy, node5: *nosloppy }
  ```
  Compose merges `environment` and `volumes` of an override file with the base file, so each node keeps its `NODE_ID` and `./data` mount. Check once with `docker compose -f docker-compose.yml -f docker-compose.nosloppy.yml config`.
- `results/` folder (code side per CLAUDE.md).

No DEC needed unless Meet objects (no proto/config-key/SPEC change); add the line to PROGRESS Blockers.

## 10. Edge cases

| Case | Handling |
|---|---|
| Write right after a kill (node still ALIVE in views) | Coordinator waits for it until the RPC timeout, W=2 met by the others → OK (tests 2, 6 rely on this) |
| Write after owners turned SUSPECT, sloppy on | Stand-ins used (test 4 must wait for this state) |
| Coordinator itself is killed mid-request | Client sees UNAVAILABLE / timeout → outcome `unknown`, not a loss |
| Paused node (frozen, port open) | Calls to it hang until timeout → slow, but W/R met by others; it goes SUSPECT/DEAD, then back after unpause |
| Restarted node DEAD in others' views for ~1 s | Every test waits for ALIVE in the needed view before acting (D7 note) |
| Anti-entropy fixes the stale replica before the read-repair test reads | Precondition check + retry with a new key (test 6) |
| A test fails half way, a node left down | `healthy` fixture heals before the next test |
| Ports already in use by another cluster | Stop with a message, never kill it |
| Old data from a previous run | Wiped at session start (`data-cluster/` local, `data/` docker) |
| Clean stop during chaos | Graceful drain (2 s) → fewer `unknown` outcomes than crash; both fine |
| Unknown-outcome write that was stored replaces an acked value | Accounted for through the chain rule (§6 change) — not a false LOST |

## 11. Done when

- `make cluster-test` (local target) runs all 9 tests green; Meet runs it once with `CLUSTER_TARGET=docker`.
- A full 5-minute chaos run: **LOST 0**, converged, report in `results/` (commit the `.md` and `.json`, not the big `.writes.jsonl`).
- "As built" filled; D10 → REVIEW; REVIEW-LATER line.

## 12. As built

_(filled after implementation)_

# TASKS — design-first work queue

Two people: **Prateek (P)** and **Meet (M)**. Every task goes through the same loop:

```
TODO → DESIGNING → DESIGNED → IMPLEMENTING → REVIEW → DONE
         ↑ owner designs and discusses with Claude (claude.ai project chat)
                     ↑ design doc committed in .team/design/
                                    ↑ Claude Code implements from the design doc
                                                 ↑ partner reads design doc + code
```

How to pick the next task: take the **first task in your column below whose status is TODO and
whose "Needs" are at least DESIGNED** (D0's frozen interfaces are enough to start most tasks —
you build against the interface, not against the partner's finished code).

Guiding rule for every design: **pick the simplest solution that is correct and that you can explain
in an interview**. Not the most optimal one. Every design doc must list the alternative you rejected
and why — that is what interviewers ask.

## Order and owners

| Round | Prateek | Meet | Sync point at end of round |
|---|---|---|---|
| 0 | **D0-P** Data, membership & test-harness contracts | **D0-M** Request path, server wiring & build contracts | Interfaces frozen |
| 1 | D1 Storage engine (WAL + snapshot) | D2 Single-node server + CLI | Phase 1 demo |
| 2 | D3 Vector clocks & reconciliation | D4 Ring + static cluster + Docker | — |
| 3 | D6 Membership + gossip + failure detector | D5 Coordinator: forwarding + quorum | Phase 2 demo |
| 4 | D9 Hinted handoff store + delivery | D7 Read repair + membership-aware routing | Phase 3 demo |
| 5 | D10 Chaos + cluster test suite | D8 Sloppy quorum | Phase 4 demo |
| 6 | D12 Demo runbook + README + interview notes | D11 Benchmarks + graphs | Final demo |
| opt | S1 Merkle anti-entropy | S2 Live dashboard | — |

Rough effort: P ≈ 32 h, M ≈ 32 h. Coordinator (D5) is the heaviest single task, so Meet's other
tasks are lighter; gossip (D6) is the heaviest on Prateek's side.

Cross-review rule: each person **reviews the other's design doc before implementation starts** and
reads the code at REVIEW. In an interview either of you must be able to explain every part.

## Where things live

One GitHub repo, `main` branch. **Code** (`proto/`, `dynamo/`, `client/`, `tools/`, `tests/`,
`results/`, `Dockerfile`, `docker-compose.yml`, `cluster.yaml`, `Makefile`, `requirements.txt`,
`README.md`) and, for now, the **notes** in `.team/` (this file, PROGRESS, DECISIONS, SPEC,
INTERFACES, HISTORY, design docs, hooks) plus `CLAUDE.md` and `.claude/settings.json`, so each
person's Claude Code sees the same board after `git pull`. Code and notes are never mixed in one
commit. The notes are removed from the repo before submission.

## File ownership (avoids merge conflicts)

| Owner | Files |
|---|---|
| Shared — change only after both agree, log in `.team/DECISIONS.md` | `proto/dynamo.proto`, `dynamo/config.py`, `cluster.yaml`, `.team/INTERFACES.md`, `.team/SPEC.md`, `Makefile`, `requirements.txt` |
| Prateek | `dynamo/storage.py`, `dynamo/vclock.py`, `dynamo/membership.py`, `dynamo/gossip.py`, `dynamo/handoff.py`, `tools/chaos.py`, `tests/cluster/`, `README.md`, `.team/DEMO.md` |
| Meet | `dynamo/server.py`, `dynamo/replica.py`, `dynamo/peers.py`, `dynamo/ring.py`, `dynamo/coordinator.py`, `client/cli.py`, `docker-compose.yml`, `Dockerfile`, `tools/bench.py` |
| Everyone | own test files `tests/test_<module>.py`, own design docs, own rows in `.team/PROGRESS.md` |

---

## D0 — Contracts & skeleton  ·  split in two halves  ·  Needs: nothing

D0 freezes everything both of us build against, so afterwards nobody waits for the other. It is
split by file ownership: **each person designs only their own half**, then reviews the other's.
D0 is DONE when both halves are DESIGNED, merged into one proto / one `INTERFACES.md`, and
`make proto && make test` passes with every node booting and answering stub RPCs.

Rule for the shared files (proto, `config.py`, `cluster.yaml`, `Makefile`, `requirements.txt`,
`INTERFACES.md`): each half writes **only its own sections**; the other person must agree before
it is frozen (log in DECISIONS.md).

Starting interface proposal (each half refines its own part):

```python
# storage.py (P)
class Storage:
    def __init__(self, data_dir: str, max_vclock: int): ...
    def load(self) -> None                                   # snapshot + WAL replay
    def get(self, key: str) -> list[Version]                 # live siblings, may include tombstones
    async def apply(self, key: str, version: Version) -> bool  # reconcile, WAL+fsync, True if stored
    async def put_hint(self, owner: str, key: str, version: Version) -> None
    def hints(self) -> dict[str, list[tuple[str, Version]]]
    async def drop_hint(self, owner: str, key: str, version: Version) -> None

# vclock.py (P) — pure functions
def increment(clock, node_id) -> VectorClock
def compare(a, b) -> Order          # BEFORE / AFTER / EQUAL / CONCURRENT
def merge(a, b) -> VectorClock
def prune(clock, max_entries) -> VectorClock
def reconcile(versions: list[Version]) -> list[Version]   # drop dominated, dedup equal
def context_of(versions) -> bytes; def parse_context(b: bytes) -> VectorClock

# ring.py (M)
class Ring:
    def __init__(self, node_ids: list[str], vnodes: int): ...
    def preference_list(self, key: str, n: int) -> list[str]   # n distinct physical nodes
    def walk(self, key: str) -> Iterator[str]                  # all distinct nodes in ring order

# membership.py (P)
class Membership:
    def is_alive(self, node_id) -> bool; def alive_nodes(self) -> list[str]
    def address(self, node_id) -> str; def snapshot(self) -> list[Member]
    def on_gossip(self, msg: GossipMessage) -> GossipMessage
    def on_contact(self, sender: str) -> None                  # any RPC from sender
    def subscribe(self, cb: Callable[[str, MemberState], None]) -> None

# peers.py (M)
class Peers:
    def stub(self, node_id) -> NodeServiceStub                 # cached channel per node

# coordinator.py (M)
class Coordinator:
    async def put(self, req: PutRequest) -> PutResponse
    async def get(self, req: GetRequest) -> GetResponse
    async def delete(self, req: DeleteRequest) -> DeleteResponse

# handoff.py (P)
class Handoff:
    async def start(self) -> None; async def stop(self) -> None
```

### D0-P — Data, membership & test-harness contracts  ·  Prateek  ·  Needs: nothing

**Design doc:** `.team/design/D0-P-contracts.md`

**Requirement.**
- Stubs (signatures, type hints, one-line docstring, `raise NotImplementedError`) for `storage.py`,
  `vclock.py`, `membership.py`, `gossip.py`, `handoff.py` — final signatures in `INTERFACES.md`.
- Proto sections: `VectorClock`, `Version`, `Member`, `MemberState`, `GossipMessage`, and the
  `Gossip` / `DeliverHint` RPCs.
- `config.py` dataclass + `cluster.yaml` (all settings from the SPEC table).
- In-process cluster pytest fixture (`tests/conftest.py`): k nodes on localhost ports, one event
  loop, with kill / restart of a node.
- Logging format helper (node id + request id prefix) used by every module.

**Design questions**
1. **fsync vs event loop:** blocking `fsync` inside asyncio (simple, slower) or `run_in_executor`
   (faster, but two writes to the same key need ordering)? Meet must agree — affects D5.
2. Should `GetResponse` / `ReplicaGetResponse` carry tombstone information, or is "only tombstones
   left" just `NOT_FOUND`?
3. `config.py`: one frozen dataclass; how per-node values (node id, port, data dir) override the file
   (CLI flags / env vars) so Docker and the test fixture can both use it.
4. Logging format: exact prefix, where the request id comes from, log level defaults — the demo
   depends on readable logs.
5. In-process cluster: how a node is "killed" (stop server without closing storage = crash) vs
   "stopped" (graceful), and how it restarts on the same port and data dir.
6. Branch strategy: everything on `main` with small rebased commits (recommended) or one branch per task.

**Done when:** design doc DESIGNED and reviewed by Meet; stubs + fixture + config committed; its
sections of `INTERFACES.md` and the proto agreed.

### D0-M — Request path, server wiring & build contracts  ·  Meet  ·  Needs: nothing

**Design doc:** `.team/design/D0-M-contracts.md`

**Requirement.**
- Repo layout exactly as in `.team/SPEC.md`; `requirements.txt`; `Makefile` with `proto`, `test`,
  `run NODE=node1`, `up`, `down`; `Dockerfile` / `docker-compose.yml` skeleton.
- Proto sections: `KVStore` (Put/Get/Delete/ClusterInfo + messages, `Status`), `Forward`,
  `ReplicaPut`, `ReplicaGet`; `make proto` compiles it.
- Stubs for `server.py`, `replica.py`, `peers.py`, `ring.py`, `coordinator.py`, `client/cli.py`.
  **`server.py` and `replica.py` already wire every RPC to the module functions**, so later tasks
  never touch the wiring.
- A `Node` object holding `config, storage, ring, membership, coordinator, handoff`, passed to
  handlers (no globals).

**Design questions**
1. Proto changes from the spec: `Forward` hop count / "forwarded" flag to stop loops? Is
   `ReplicaPut` reused for read repair? Proto path `proto/dynamo.proto` (spec) vs
   `proto/dynamo/dynamo.proto` (so protoc emits `from dynamo import ...`)?
2. **Error convention:** which gRPC errors / timeouts map to which `Status`; what a replica returns
   on an internal error.
3. `Node` object: construction order (config → storage.load → ring → membership → coordinator →
   handoff) and start/stop order.
4. Where the request id is created (server entry point) and how it travels through `Forward`.
5. `make run NODE=node1` vs `make up`: how a node finds its own entry in `cluster.yaml`.

**Done when:** design doc DESIGNED and reviewed by Prateek; `make proto && make test` passes;
every node boots and answers stub RPCs cleanly; its sections of `INTERFACES.md` and the proto agreed.

---

## D1 — Storage engine  ·  Prateek  ·  Phase 1  ·  Needs: D0

**Files:** `dynamo/storage.py`, `tests/test_storage.py`

**Requirement.** In-memory `dict[str, list[Version]]` plus a JSON-lines WAL fsynced before acking;
snapshot every 1,000 lines then truncate the WAL; on startup load snapshot then replay WAL. Hints are
persisted too. A write is only acked once it is on disk.

**Design questions**
1. **Torn last line:** the process dies halfway through writing a WAL line. How does replay detect it, and what happens to the bytes after it?
2. **Crash during snapshot:** write temp file → fsync → `os.replace` → truncate WAL. What if the crash happens *between* replace and truncate? (Hint: is replaying an already-snapshotted line harmful if `apply` reconciles by vector clock?)
3. **Idempotent replay:** is applying the same line twice always safe? Prove it with the reconcile rule.
4. **fsync vs event loop:** per D0 decision — and if writes go to a thread, how do you keep two writes to the same key in order?
5. Values are bytes → base64 in JSON. Any size limit?
6. Tombstones: stored like any version; when and where are they purged (24 h)? Does purge need a WAL entry?
7. Hints: same WAL with `hint_for` set, or a separate `hints.log`? Which is easier to replay and to delete from?
8. Until D3 lands, `apply` needs *some* rule — use "last write replaces" behind the same signature, then swap in `vclock.reconcile`.

**Done when:** tests for write → `kill -9` → restart → data present; torn-line replay; snapshot +
replay; crash between replace and truncate; a 10k-writes timing printed (put the number in the doc).

---

## D2 — Single-node server + CLI  ·  Meet  ·  Phase 1  ·  Needs: D0

**Files:** `dynamo/server.py`, `dynamo/replica.py`, `dynamo/peers.py`, `client/cli.py`, `tests/test_cli.py`

**Requirement.** `grpc.aio` server exposing `KVStore` and `NodeService` on one port. With N=1 it
serves Put/Get/Delete/ClusterInfo against local storage. CLI as in the spec, with a context file.
Clean shutdown on SIGTERM/SIGINT.

**Design questions**
1. Build Put/Get on top of the `Coordinator` interface already (a "local only" coordinator for N=1) so D5 extends it instead of rewriting? Or call storage directly now?
2. **CLI context file:** where (e.g. `~/.kv_context.json`), keyed by key or by (node, key)? What does `put` do when no context is stored (creates a sibling — is that what the user expects)? How are siblings printed?
3. **Shutdown:** stop accepting new RPCs, wait for in-flight ones (grace period), close WAL. What does Docker's `stop` vs `kill` do to this?
4. **Validation:** empty key, value over gRPC's 4 MB default, `r`/`w` = 0 means default, `r`/`w` > N → `INVALID`.
5. **Peers:** one channel per peer, created lazily and reused; what happens when a peer restarts on the same address (channel reconnects?); per-call deadline from `rpc_timeout_ms`.
6. Every NodeService handler calls `membership.on_contact(sender)` — keep it a no-op until D6.

**Done when:** Phase 1 demo works (put → kill -9 → restart → get); CLI tests cover put/get/delete/siblings output.

---

## D3 — Vector clocks & reconciliation  ·  Prateek  ·  Phase 3 logic, built early  ·  Needs: D0

**Files:** `dynamo/vclock.py`, `tests/test_vclock.py` (and swap `reconcile` into `storage.apply`)

**Requirement.** Pure functions from the spec plus `reconcile(versions)` (remove every version that is
BEFORE another, dedup EQUAL) and context encode/decode. Built in round 2 so the coordinator (D5) can use
it from day one instead of being rewritten for phase 3.

**Design questions**
1. Clocks are keyed by **coordinator node id**, not client id. Why? What does that cost when two coordinators handle the same key?
2. **Pruning** drops the oldest entry: can that make two causally ordered versions look CONCURRENT? Is that a safe or unsafe direction (false siblings vs lost writes)? This is a classic interview question.
3. **Delete concurrent with a put:** the result is a tombstone sibling + a value sibling. What does `Get` return — the value only, or report the conflict? Pick one rule and write it down.
4. Same clock, different value (only on a bug or a retried write that got a different value) — how is it handled?
5. Invalid `context` bytes from a client → `INVALID`, or treat as empty?
6. `updated_ms` uses wall clock — does clock skew between nodes break anything beyond prune order?

**Done when:** table-driven tests for every `compare` case; merge/prune tests; random test that
`reconcile` always returns versions that are pairwise CONCURRENT.

---

## D4 — Ring + static cluster + Docker  ·  Meet  ·  Phase 2  ·  Needs: D0

**Files:** `dynamo/ring.py`, `docker-compose.yml`, `Dockerfile`, `tests/test_ring.py`

**Requirement.** MD5 ring, 32 vnodes per node, `preference_list(key, n)` returning n distinct physical
nodes, `walk(key)` for sloppy quorum later. Five-node Docker Compose with a volume per node so data
survives container restarts.

**Design questions**
1. **Does the ring change when a node is DEAD?** (Recommended: no. Ownership stays fixed; DEAD only changes *routing* and triggers sloppy quorum. Otherwise every flaky node moves keys around.) Write down why.
2. Lookup: sorted list + `bisect`, wrap-around at the end; what if two vnodes hash to the same position?
3. `n` larger than the number of physical nodes → return all nodes, or error?
4. Measure distribution: keys per node for 10k keys with 8 / 32 / 128 vnodes (standard deviation) — one small table in the doc, great interview material.
5. Docker: node addresses by service name (`node1:50051`); `docker kill` (crash) vs `docker stop` (graceful) for demos; how the CLI on the host reaches each node.

**Done when:** ring tests (deterministic, distinct, ~1/5 of keys move when a node is removed);
`docker compose up` → `kv info` against any node lists 5 members.

---

## D5 — Coordinator: forwarding + quorum  ·  Meet  ·  Phase 2  ·  Needs: D2, D3, D4

**Files:** `dynamo/coordinator.py`, `tests/test_coordinator.py` (in-process cluster)

**Requirement.** The Put / Get / Delete flows from the spec: forward to the first live node of the
preference list, write locally (1 ack), fan out `ReplicaPut`/`ReplicaGet`, return after W/R replies,
`QUORUM_FAILED` on timeout, merge reads with `vclock.reconcile`. Delete = put of a tombstone.

**Design questions**
1. **Who coordinates?** Only the first live node in the preference list (spec), or any node that is in the top N (saves a hop)? Simplicity vs latency.
2. **Forward loops:** node A forwards to B, but B thinks A should coordinate (different views). How do you stop ping-pong? (hop count / "forwarded" flag / coordinate anyway.)
3. **Quorum wait:** `asyncio.wait(FIRST_COMPLETED)` loop counting successes and failures; **fail early** when `acks + still_pending < W`. Write the loop in pseudocode in the doc.
4. **Stragglers:** after W acks, let remaining replica writes finish in the background (don't cancel) — how do you avoid leaking tasks / unhandled exceptions? For reads, cancel or keep (keeping them is needed for read repair in D7)?
5. Local write first and then fan out, or all in parallel?
6. Per-RPC timeout vs one overall deadline for the request.
7. For reads: a replica answering "not found" counts toward R. When is the answer NOT_FOUND vs OK?
8. If forwarding fails (coordinator just died, gossip hasn't noticed), try the next node in the list?

**Done when:** in-process tests: write via node1 / read via node4; 1 node down with N=3 R=2 W=2 still works;
2 replicas down → `QUORUM_FAILED`; Phase 2 demo in Docker.

---

## D6 — Membership + gossip + failure detector  ·  Prateek  ·  Phase 3  ·  Needs: D0

**Files:** `dynamo/membership.py`, `dynamo/gossip.py`, `tests/test_membership.py`

**Requirement.** Every `gossip_interval_ms` push-pull the membership table with one random peer.
ALIVE → SUSPECT after 3 s without heartbeat progress → DEAD after 10 s. Incarnation-based refutation.
Expose the `Membership` interface from D0, including `subscribe` (handoff needs "owner is ALIVE again").

**Design questions**
1. **Whose clock decides "3 s without heartbeat"?** Use the *local* time at which you last saw the heartbeat number increase, never the sender's timestamp — why?
2. **Is SUSPECT/DEAD a local opinion or gossiped?** Option A: each node decides from heartbeat age (simple, recommended). Option B: states spread through gossip (needs incarnation rules for every state). Pick and justify.
3. **Restart:** the incarnation must be larger than before the crash. Persist it, or use start-up time in ms? Edge case: what if the clock went backwards?
4. Peer choice: random ALIVE peer; occasionally gossip to a DEAD one so recovery is noticed — how often?
5. **Boot:** all nodes start at once and know nobody's heartbeat — start everyone as ALIVE, or as SUSPECT until first contact?
6. A failed Gossip RPC: mark the peer immediately, or only let heartbeat age decide (avoids false positives)?
7. `on_contact(sender)` from any RPC: does it count as a heartbeat?

**Done when:** in-process tests for merge rule and refutation; kill a node → all others show SUSPECT
by ~3 s and DEAD by ~10 s; restart → ALIVE again; `kv info` shows states.

---

## D7 — Read repair + membership-aware routing  ·  Meet  ·  Phase 3  ·  Needs: D5, D6

**Files:** `dynamo/coordinator.py`, tests

**Requirement.** After a Get, push the reconciled version(s) to replicas that were missing them or
behind, in the background. Coordinator skips DEAD nodes; fast `QUORUM_FAILED` when too few nodes are alive.

**Design questions**
1. Repair before replying (slower, simpler to reason about) or after (spec)? Do you also repair replicas whose answer arrived *after* the reply was sent?
2. How is "stale" decided: the replica's version set differs from the reconciled set?
3. Hot key read 1,000 times while one replica is behind — how do you avoid 1,000 repair writes?
4. Repair a SUSPECT node or skip it?
5. Fast-fail rule: exactly which count is compared with W before sloppy quorum exists, and how will D8 change it?

**Done when:** test makes one replica stale, does a Get, and sees the replica updated; DEAD-skip test; Phase 3 demo.

---

## D8 — Sloppy quorum  ·  Meet  ·  Phase 4  ·  Needs: D7, D9 interface

**Files:** `dynamo/coordinator.py`, tests

**Requirement.** When a preferred replica is DEAD, walk the ring past the top N and use the next
healthy node as a stand-in, sending `ReplicaPut` with `hint_for = <dead owner>`. A config flag turns
sloppy quorum on/off (needed for cluster tests 3 and 4).

**Design questions**
1. Stand-in must not already be one of the N replicas — how does `walk()` make this easy?
2. Two owners dead → two stand-ins, each with a different `hint_for`.
3. **Reads:** do reads also go to stand-ins? (Dynamo: reads use the first N healthy nodes, so yes.) What does a stand-in return for a key it holds only as a hint?
4. Does a hinted write count toward W? (Spec: yes.) What does that mean for durability if the stand-in then dies?
5. The owner is SUSPECT, not DEAD: write to it or to a stand-in?

**Done when:** cluster tests 3 (off → `QUORUM_FAILED`) and 4 (on → success) pass in-process.

---

## D9 — Hinted handoff store + delivery  ·  Prateek  ·  Phase 4  ·  Needs: D1, D6

**Files:** `dynamo/handoff.py` (+ hint functions in `storage.py`), tests

**Requirement.** Stand-ins persist hints (survive restart), retry delivery every `handoff_retry_ms` to
owners that are ALIVE, delete only after the owner acks.

**Design questions**
1. Delivered twice (ack lost) — why is that harmless? (Reconcile again.)
2. Deliver one hint per RPC, or batch per owner? Order of delivery?
3. Owner never comes back (permanently removed) — keep hints forever, expire after X, or out of scope? State it.
4. Stand-in restarts before delivering — hints must replay from disk; how do you delete a hint durably?
5. Use the `subscribe` callback to deliver immediately when an owner turns ALIVE, or only the periodic loop? (Simplest correct option is fine.)
6. Does the stand-in serve hinted data on reads in the meantime? (Must match D8's answer.)

**Done when:** kill owner → write with sloppy quorum → restart owner → within ~5 s owner has the data and the hint is gone (also after a stand-in restart).

---

## D10 — Chaos + cluster test suite  ·  Prateek  ·  Phase 4  ·  Needs: D8, D9 (start on phase-3 system)

**Files:** `tools/chaos.py`, `tests/cluster/`

**Requirement.** Automate all 8 cluster tests from the spec against Docker Compose, plus a 5-minute
chaos run that proves "zero acknowledged writes lost".

**Design questions**
1. How do you *prove* no acked write was lost? (Client logs every OK'd write with key/value/context; at the end read every key with R=N and check the acked value is present among siblings or superseded by a later acked write.)
2. Chaos rules: never more than 1 node down at a time with N=3, W=2 — otherwise losses are expected and the test means nothing. Write the rule down.
3. Kill style: `docker kill` (crash) vs `docker stop`; pause/unpause to simulate a slow node?
4. Docker SDK for Python vs `subprocess` calls to `docker compose` — pick the simpler.
5. How long to wait after a restart before checking (gossip + handoff timers)?

**Done when:** `make cluster-test` runs all 8 tests green; chaos report saved in `results/`.

---

## D11 — Benchmarks + graphs  ·  Meet  ·  Phase 4  ·  Needs: D5 (full runs after D8)

**Files:** `tools/bench.py`, `results/`

**Requirement.** Workload from the spec; ops/s, p50, p99; vary (N,R,W), cluster size, and 1 node
killed; CSV + matplotlib graphs.

**Design questions**
1. Closed loop (16 clients each waiting for its reply) vs open loop (fixed request rate)? Closed loop is simpler — note that it hides queueing delay ("coordinated omission").
2. N is a cluster setting (restart needed), R/W are per request — how does the script sweep both?
3. Warm-up period and how many repetitions; laptop noise.
4. Running the load generator on the same machine as 5 containers — say so in the results.

**Done when:** graphs for every row of the spec's benchmark plan in `results/`, with one-line takeaways.

---

## D12 — Demo runbook + README + interview notes  ·  Prateek  ·  Final  ·  Needs: D10

**Files:** `README.md`, `.team/DEMO.md`, `.team/INTERVIEW.md`

**Requirement.** Step-by-step live-demo script for each phase (commands + what the audience should
see), README (architecture diagram, how to run), and an interview sheet: for every design doc, the
decision, the rejected alternative and why.

**Design questions**
1. Demo order and timing for the live demo; what to pre-run in case something fails.
2. Which 3 numbers from benchmarks go on the slide / resume.

**Done when:** a dry run of the full demo by the other person, following only `.team/DEMO.md`, works.

---

## Stretch (only after D1–D12 are DONE)

- **S1 Merkle anti-entropy** (Prateek) — per-key-range Merkle trees, compare roots, sync differing ranges.
- **S2 Live dashboard** (Meet) — small FastAPI page: ring, node states, key counts per node.

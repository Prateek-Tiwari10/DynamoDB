# HISTORY — what happened in each Claude Code session

Newest session at the bottom. One section per session: date, who, what was done, state left behind,
next step. Keep each section short; details belong in PROGRESS / DECISIONS / design docs.

---

## Session 1 — 2026-10-23 · Prateek · orientation, no code changed

**Done:** read every file in the workspace (notes, code, `readme/`, `PHASE1_IMPLEMENTATION.md`,
project write-up PDF) and created this file. No commits, no code edits.

**What the project is:** Dynamo-style KV store (IIIT-H Distributed Systems, Monsoon 2026; Prateek
2025201037 + Meet 2025201096). 5 nodes, N=3/R=2/W=2, MD5 ring with 32 vnodes, vector clocks + siblings,
read repair, gossip ALIVE→SUSPECT→DEAD, sloppy quorum + hinted handoff, WAL + snapshot, chaos tests,
benchmarks. Work queue D0–D12 in `TASKS.md`, design-first.

**State found:**
- **Notes repo not set up yet.** There is no `.team/`. The notes live in `dynamo-team-notes/`, which
  has no `.git` and no remote. `setup.sh` has not run: root `CLAUDE.md` is a full copy instead of
  `@.team/CLAUDE.md`, `.git/info/exclude` is empty, no `.claude/settings.local.json`, so the
  SessionStart sync hook did not run. `.team/me` does not exist.
- `dynamo-team-setup/` is an **older version** of the notes (paths `docs/`, hook in `.claude/hooks/`).
  Root `TASKS.md` is the same older copy. `dynamo-team-notes/` is the current one.
- **Code repo:** one commit (`69149f1 first commit`, empty README), remote
  `git@github.com:Prateek-Tiwari10/DynamoDB.git`. Everything else is untracked.
- **An untracked Phase 1 implementation exists** (Oct 4, built from `PHASE1_IMPLEMENTATION.md`, before
  the D0–D12 split): `dynamo/storage.py` (dict of bytes, WAL+fsync, snapshot every 1000, torn-line
  truncate, `threading.Lock`), `dynamo/server.py` (sync thread-pool gRPC, KVStore only, key ≤256 B,
  value ≤1 MiB), `client/cli.py` (exit codes 0/1/2/3, no context file), `tests/` (storage, server,
  CLI), single-node Docker, `scripts/demo_phase1.sh`. It overlaps D1 (Prateek) and D2 (Meet) but does
  **not** match the D0 interfaces (`list[Version]`, `async apply`, `Node` object, `NodeService`,
  `ClusterInfo`). PROGRESS board still shows every task TODO.
- Proto is at `proto/dynamo/dynamo.proto` (so protoc emits `from dynamo import ...`); SPEC says
  `proto/dynamo.proto`. A D0 question.
- `readme/phase1/`, `readme/phase2/` are earlier design write-ups (phase 2 used LWW timestamps and
  `handle_put(req, forwarded)`) — they differ from the TASKS.md plan. They, `PHASE1_IMPLEMENTATION.md`
  and the PDF are notes, not code: they must not be committed to the code repo.
- No `.gitignore`; generated `dynamo_pb2*.py` and `dynamo_kv.egg-info/` sit in the tree.
- Environment: system Python 3.14.4 (spec says 3.11), no venv, pytest not installed — tests were not run.
- Git identity in code repo: `Prateek-Tiwari10 <tprateek2001@gmail.com>`.

**Open questions for Prateek:**
1. Create the private notes repo (GitHub), move `dynamo-team-notes/` to `.team/`, run `setup.sh Prateek`?
2. Delete `dynamo-team-setup/` and root `TASKS.md` once `.team/` exists?
3. Existing Phase 1 code: keep as the starting point for D1/D2 (and tell Meet), or redo from D0?
4. Confirm the git email `tprateek2001@gmail.com` is the one you want on commits.

**Next step:** finish setup, then D0 (contracts & skeleton) together with Meet.

## Session 2 — 2026-10-23 · Prateek · notes set up locally, restart from D0

**Done:**
- Moved `dynamo-team-notes/` → `.team/`, `git init` there, ran `setup.sh Prateek` (`.team/me`,
  root `CLAUDE.md` = `@.team/CLAUDE.md`, SessionStart hook in `.claude/settings.local.json`,
  `.team/`, `CLAUDE.md`, `.claude/` added to `.git/info/exclude`).
- **Decision (Prateek): no private notes repo.** `.team/` is a local-only git repo — no remote,
  never pushed. `CLAUDE.md` and `hooks/sync.sh` updated to match (hook no longer pulls `.team/`).
- Deleted the old duplicate notes (`dynamo-team-setup/`, root `TASKS.md`).
- **Restart from D0:** moved all pre-D0 work (Phase 1 code, tests, scripts, Docker files, proto,
  `readme/`, `PHASE1_IMPLEMENTATION.md`, write-up PDF, old README text) to `.team/archive/pre-d0/`
  for reference. Code repo now holds only the committed empty `README.md`.
- D0 → DESIGNING; design brief printed for the claude.ai chat.

**State left:** code repo clean (`git status` empty). Git identity in both repos:
`Prateek-Tiwari10 <tprateek2001@gmail.com>` (global config).

**Next step:** design D0 together with Meet in claude.ai, save it as `.team/design/D0-contracts.md`.
Note: Meet has no access to `.team/` now that it is local-only — share PROGRESS/design some other way.
- Later in session 2: **D0 split by ownership** (Prateek's request: each person designs different
  parts). D0-P (Prateek: storage/vclock/membership/gossip/handoff stubs, data + gossip proto
  messages, config + cluster.yaml, in-process test fixture, logging) and D0-M (Meet: layout,
  Makefile/Docker, KVStore + Forward/ReplicaPut/ReplicaGet proto, server/replica wiring, Node object,
  ring/peers/coordinator/cli stubs). D0-P → DESIGNING; D0-M TODO for Meet. Meet must confirm the split.
- Later in session 2: **notes moved into git on `main`** (Prateek's choice, so Meet's Claude Code
  sees the same board after `git pull`). `.team/` is now a normal tracked folder (nested `.team/.git`
  removed), plus root `CLAUDE.md` and `.claude/settings.json` (SessionStart hook). Code and notes are
  never mixed in one commit. Notes get removed from the repo before submission (CLAUDE.md section 7).
  CLAUDE.md got a "What do I need to do?" section that answers per `.team/me`.
  **Meet's setup:** `git pull` (or clone), then `bash .team/setup.sh Meet`, then open Claude Code
  and ask "what do I need to do?" — first task: D0-M.

## Session 3 — 2026-11-11 · Prateek · D1 storage designed, built, in REVIEW

**Done:**
- Pulled Meet's D0-M draft (3 commits). `.team/INTERFACES.md` and the D0-P design do not exist yet.
- Saved `.team/design/D1-storage.md`; storage names follow the D0-M draft (`recover / get / apply / close`).
- Built `dynamo/storage.py`, `tests/test_storage.py`, `tests/test_storage_crash.py` (26 tests pass).
- Timing: 10,000 puts in 34.90 s = 287 writes/s. Kill -9 demo run by hand: 3 keys back, deleted key NOT_FOUND.

**State left:** D1 = REVIEW (Meet sets DONE). D0-P still DESIGNING, D0-M DESIGNING.
No `Makefile`, `requirements.txt` or `dynamo/__init__.py` yet; tests run with `python -m pytest tests`.
SSH to github.com on port 22 timed out on this network; pushes went through `ssh.github.com:443`.

**Next step:** Prateek answers Meet's 17 "Needs Prateek" items and designs D0-P, so INTERFACES.md can be frozen.

## Session 4 — 2026-11-11/12 · Prateek · 17 D0-M answers recorded, D1 updated, back in REVIEW

**Done:**
- Recorded Prateek's answers to Meet's 17 "Needs Prateek" items as DEC-02 to DEC-07 ("agreed by
  Prateek; Meet to confirm"). Updated `SPEC.md`, `TASKS.md`, `CLAUDE.md`; created `INTERFACES.md`
  (storage block final, the rest draft). Meet's D0-M doc was not edited.
- Two answers differ from Meet's doc: item 8 (no `DeliverHint`, reuse `ReplicaPut`) and item 14
  (no tombstone purge, no `last_alive` guard). Both are listed under Blockers in PROGRESS.md.
- D1 code: `recover` renamed to `load`, log write / `fsync` error exits the node, `counts()` added.
  28 tests pass, including the kill -9 tests.

**Working rule from Prateek (2026-11-11):** do not wait for Meet's review. After implementing, set
REVIEW and move to Prateek's next task; Meet's change requests are done later as follow-ups.

**State left:** D1 = REVIEW. D0-P = DESIGNING with no design doc. D0-M = DESIGNING (Meet).

**Next step:** Prateek designs D0-P (config, logging format, test fixture, gossip proto half), then D3.

## Session 5 — 2026-11-12 to 2026-11-20 · Meet · D0-M designed, built, in REVIEW

**Done:**
- Confirmed DEC-02 to DEC-08; D0-M doc updated (no `DeliverHint`, no tombstone purge, Prateek's final names); DEC-09 added for Meet's half (Prateek to confirm).
- Built and pushed D0-M: proto (both halves), `Makefile`, `pyproject.toml`, `requirements.txt`, `.gitignore`, `reqid.py`, `ring.py`, `client/cli.py`, coordinator / peers / replica skeleton, `server.py`, `Dockerfile`, `docker-compose.yml`. 64 new tests; suite: 121 passed, 1 skipped.
- Checked Prateek's D0-P files against Meet's code: no conflicts.

**State left:** D0-M = REVIEW (REVIEW-LATER #5). D0-P = IMPLEMENTING (Prateek: vclock, version, gossip stub, conftest fixture). D1 = REVIEW.

**Next step:** Meet designs D2 (single-node Put/Get/Delete, Phase 1 demo) and D4 (5-node Docker cluster).

## Session 5 — 2026-11-20/21 · Prateek · D0-P files pushed, D1 done, D3 built, review-later rule

**Done:**
- Pushed the D0-P files Meet's `server.py` waited for: `config.py`, `membership.py` and `handoff.py`
  skeletons, `cluster.yaml`, `cluster.local.yaml`, `Storage(data_dir, config)`. (Written in an
  earlier session that left no history entry.)
- D1 follow-up: a failed snapshot keeps the node running; Prateek changed the retry to "after the
  next 1000 writes" (counter reset, log kept). Failed log clear exits. D1 doc §5.3, §6, §8 Q9, §9
  updated. D1 → REVIEW.
- New working rule (Prateek): finish the project first, review later. Reviews are collected in
  `.team/REVIEW-LATER.md` and done after the first full version runs. Stop only for things that
  are wrong or hard to defend in an interview; simple over complex.
- D3 from Prateek's design chat (saved as `.team/design/D3-vector-clocks.md`): `vclock.py`,
  `version.py`, storage on `merge_into`, JSON context, no same-value collapse. With Prateek's OK,
  also swapped Meet's `coordinator.py` / `replica.py` / coordinator tests so main stayed green.
  DEC-11 (Meet to confirm). D3 → REVIEW. 217 tests pass.

**State left:** D0-P IMPLEMENTING (left: gossip stub, test cluster fixture). D1, D3 = REVIEW.
Meet: D0-M, D2, D4 in REVIEW.

**Next step:** finish D0-P (gossip stub, `tests/conftest.py` cluster fixture, using Meet's
`server.py` / D4 in-process cluster), then D6 (membership + gossip + failure detector).

## Session 6 — 2026-11-21 · Prateek · D0-P finished

**Done:**
- `dynamo/gossip.py` stub: `async def gossip_loop(membership) -> None` (logic in D6).
- Shared test cluster in `tests/conftest.py`: `cluster` (5 nodes) and `make_cluster(k, **overrides)`,
  short timers, `stop_node` / `kill` / `restart`, `client` / `replica` stubs, `wait_until`.
  The five open questions are answered in D0-P §9 (killed log closed at teardown; backoff and
  dead timer both unchanged, peer reconnect measured ~0.45 s; `wait_until` added; port race
  accepted, start() checks each port answers with its own node id because gRPC uses SO_REUSEPORT;
  factory fixture tracks every cluster for teardown).
- `kill` also cancels the node's coordinator tasks (`drain(0)`) so a dead node never writes to a
  log its restarted copy has opened.
- 12 harness tests; 256 tests pass. D0-P → REVIEW (REVIEW-LATER #10).

**State left:** D0-P, D1, D3 = REVIEW. Meet: D0-M, D2, D4, D5 in REVIEW; next D7 (needs D6) or D11.

**Next step:** D6 (membership + gossip + failure detector), tested with the new cluster fixture.

## Session 7 — 2026-11-21 · Prateek · D6 combined, 6a gossip built

**Done:**
- D6, D9 and stretch S1 combined into one task D6 (6a gossip, 6b hinted handoff, 6c Merkle
  anti-entropy), DEC-12; Merkle moved into the main plan (SPEC C11).
- 6a design from Prateek's chat saved as `.team/design/D6-failure-detection.md`; DEC-13 (refutation
  `>` rule, `suspect_after_ms` 3000 → 5000), Meet to confirm.
- 6a built: `membership.py` (merge, refutation, states, callbacks), `gossip.py` (fixed-rate loop,
  `send_gossip`), 46 new/updated tests; full suite 292 passed. Live 5-process check: SUSPECT ~5 s,
  DEAD ~10 s, ALIVE after restart.
- Found and fixed: gossip at t=0 put peer channels in reconnect backoff and broke Meet's D5 cluster
  tests; first round now one interval after start. Meet's `test_server.py` Gossip test updated.

**State left:** D6 IMPLEMENTING (6a done, 6b/6c to design). D0-P, D1, D3 = REVIEW.
Meet: D0-M, D2, D4, D5 in REVIEW; D7 can start now.

**Next step:** design 6b (hinted handoff) in the design chat, then 6c (Merkle).

## Session 8 — 2026-11-21 · Meet · D2, D4, D5 built; DEC-12/13 confirmed; D7 and D6b built

**Done:**
- D2 single node, D4 static cluster, D5 coordinator (routing, forwarding, quorum loop) designed,
  built and set to REVIEW; Phase 1 and Phase 2 demos pass locally and in Docker.
- DEC-12 and DEC-13 confirmed. `grpc.so_reuseport = 0` in `build_node`: a second node on a taken
  port now fails to start (test fails without the fix).
- D7 designed (Q5: fast fail counts only ALIVE, Meet's choice) and built: read repair after every
  get (late answers included, only missing versions sent, one repair per (key, node) in flight),
  fan-out skips DEAD replicas. 20 new tests; full suite 313 passed, 1 skipped.
  `tools/demo_phase3.sh` passes on the local cluster.

**State left:** Meet: D0-M, D2, D4, D5, D7, D6b in REVIEW (REVIEW-LATER #5–#8, #12, #13). Docker run of the
Phase 3 demo still to do by Meet.

- 6b hinted handoff moved from Prateek to Meet (DEC-14, Prateek agreed). D6b designed (all
  recommended options) and built: hints kept apart from own data (log + `hints.json`), delivery
  loop every 5 s and on "owner ALIVE", delete after ack. 331 passed, 1 skipped.

**Next step:** D8 sloppy quorum (D6b is in now); its read answer must match D6b Q6 (hints are not
visible to reads).


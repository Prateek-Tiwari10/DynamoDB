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

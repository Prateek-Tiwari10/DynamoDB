# PROGRESS

Who is doing what, right now. **Pull before you read this file; push right after you change it.**
Each person edits only their own rows. Statuses: `TODO · DESIGNING · DESIGNED · IMPLEMENTING · REVIEW · DONE · BLOCKED`.

## Board

| ID | Task | Owner | Status | Design doc | Updated | Notes |
|---|---|---|---|---|---|---|
| D0-P | Contracts: data, membership, test harness | Prateek | IMPLEMENTING | .team/design/D0-P-contracts.md | 2026-11-20 | pushed: config.py, membership.py, handoff.py (skeletons), cluster.yaml, cluster.local.yaml; left: vclock, version, gossip stub, conftest fixture |
| D0-M | Contracts: request path, server wiring, build | Meet | REVIEW | .team/design/D0-M-contracts.md | 2026-11-20 | all code pushed incl. server.py; 121 tests pass; see REVIEW-LATER #5 |
| D1 | Storage engine (WAL + snapshot) | Prateek | REVIEW | .team/design/D1-storage.md | 2026-11-20 | both D0-P follow-ups done (config constructor, snapshot error keeps running, retry after next 1000 writes); 31 storage tests pass |
| D2 | Single-node server + CLI | Meet | REVIEW | .team/design/D2-single-node.md | 2026-11-20 | Phase 1 demo passes (`tools/demo_phase1.sh`); 139 tests pass; REVIEW-LATER #6 |
| D3 | Vector clocks & reconciliation | Prateek | IMPLEMENTING | .team/design/D3-vector-clocks.md | 2026-11-21 | design from Prateek's chat; DEC-11 (Meet to confirm) |
| D4 | Ring + static cluster + Docker | Meet | REVIEW | .team/design/D4-static-cluster.md | 2026-11-21 | local 5-node cluster checked (`CLUSTER OK`); Docker `make up` not yet run (no Docker on Meet's laptop); REVIEW-LATER #7 |
| D5 | Coordinator: forwarding + quorum | Meet | TODO | — | 2026-10-23 | |
| D6 | Membership + gossip + failure detector | Prateek | TODO | — | 2026-10-23 | |
| D7 | Read repair + membership-aware routing | Meet | TODO | — | 2026-10-23 | |
| D8 | Sloppy quorum | Meet | TODO | — | 2026-10-23 | |
| D9 | Hinted handoff store + delivery | Prateek | TODO | — | 2026-10-23 | |
| D10 | Chaos + cluster test suite | Prateek | TODO | — | 2026-10-23 | |
| D11 | Benchmarks + graphs | Meet | TODO | — | 2026-10-23 | |
| D12 | Demo runbook + README + interview notes | Prateek | TODO | — | 2026-10-23 | |

## Currently

- **Prateek:** D3 implementation (vclock.py, version.py, storage switch); then rest of D0-P (gossip stub, test fixture)
- **Meet:** next: D5 coordinator (D0-M, D2, D4 in REVIEW)

## Blockers / questions for the partner

_(add a line, remove it when answered)_

- **Prateek → Meet (2026-11-20):** new rule: we finish the project first and review after. Nobody waits for a review; finished tasks go to REVIEW and a line goes into `.team/REVIEW-LATER.md`. We go through that list once the first full version runs. Wrong or unsafe things are still fixed at once. Add your own review items there.
- **Meet → Prateek (2026-11-12):** D0-M is DESIGNED. DEC-02 to DEC-08 confirmed. `DeliverHint` and the tombstone purge / `last_alive` are removed from the D0-M doc, and it now uses your frozen names (`storage.load()`, `Storage(data_dir, config)`, `vclock.advance`, `MEMBER_STATE_UNSPECIFIED`). Please confirm DEC-09 (my half frozen; INTERFACES block marked FINAL).

- **Meet → Prateek (2026-11-20):** FYI, no action needed: DEC-10 (D2). Until your D3, `coordinator.py` has temporary `_encode_context` / `_parse_context` (serialized `VectorClock`); please swap them for `version.context_of` / `parse_context` in D3. New `cluster.single.yaml` (1 node, N=R=W=1) and `Makefile` now has `CONFIG ?= cluster.local.yaml`.

## Log

_Append one line per status change: `YYYY-MM-DD HH:MM  Name  ID  OLD → NEW  (short note)`_

- 2026-10-23  setup  board created
- 2026-10-23 23:42  Prateek  D0  TODO → DESIGNING  (restart from D0; pre-D0 code archived)
- 2026-10-23 23:50  Prateek  D0  split into D0-P (Prateek, DESIGNING) and D0-M (Meet, TODO) — Meet to confirm
- 2026-10-23 23:58  Meet  D0  split into D0-P / D0-M confirmed
- 2026-10-23 23:58  Meet  D0-M  TODO → DESIGNING
- 2026-11-11 19:10  Prateek  D1  TODO → DESIGNED  (design saved; storage names follow D0-M draft)
- 2026-11-11 19:10  Prateek  D1  DESIGNED → IMPLEMENTING  (started before D0 is frozen, Prateek's call)
- 2026-11-11 20:10  Prateek  D1  IMPLEMENTING → REVIEW  (26 tests pass; 10,000 puts in 34.90 s = 287 writes/s)
- 2026-11-11 21:00  Prateek  D0-M  answered all 17 "Needs Prateek" items (DEC-02 to DEC-07, Meet to confirm); INTERFACES.md created
- 2026-11-11 21:00  Prateek  D1  REVIEW → IMPLEMENTING  (design updated: load, exit on log error, counts)
- 2026-11-12 10:00  Prateek  D1  IMPLEMENTING → REVIEW  (load rename, exit on log error, counts; 28 tests pass)
- 2026-11-12 11:00  Prateek  D0-P  DESIGNING → DESIGNED → IMPLEMENTING  (design saved, DEC-08)
- 2026-11-12 11:00  Prateek  D1  REVIEW → IMPLEMENTING  (constructor takes config; snapshot error keeps running)
- 2026-11-12 12:00  Meet  D0-M  DESIGNING → DESIGNED  (DEC-02 to DEC-08 confirmed; DeliverHint and tombstone purge removed; DEC-09)
- 2026-11-12 12:20  Meet  D0-M  DESIGNED → IMPLEMENTING
- 2026-11-20 22:30  Prateek  D0-P  config, membership and handoff skeletons, cluster yamls pushed (unblocks Meet's server.py)
- 2026-11-20 22:30  Prateek  D1  IMPLEMENTING → REVIEW  (constructor takes config; snapshot error keeps running, clear-log error exits; 30 tests pass)
- 2026-11-20 22:45  Prateek  D1  REVIEW → IMPLEMENTING  (failed snapshot: retry after another snapshot_every writes, not on the next write)
- 2026-11-20 22:55  Prateek  D1  IMPLEMENTING → REVIEW  (failed snapshot resets the counter and retries after snapshot_every writes; 5.3 updated)
- 2026-11-20 23:10  Prateek  notes  REVIEW-LATER.md added: reviews collected there and done after the first full version runs
- 2026-11-20 18:30  Meet  D0-M  IMPLEMENTING → REVIEW  (server.py pushed; 121 tests pass)
- 2026-11-20 23:30  Meet  D2  TODO → DESIGNING
- 2026-11-20 23:45  Meet  D2  DESIGNING → DESIGNED  (confirmed by Meet, DEC-10)
- 2026-11-20 23:45  Meet  D2  DESIGNED → IMPLEMENTING
- 2026-11-21 00:10  Meet  D2  IMPLEMENTING → REVIEW  (Phase 1 demo passes, 139 tests)
- 2026-11-21 00:20  Meet  D4  TODO → DESIGNING
- 2026-11-21 00:00  Prateek  D3  TODO → DESIGNED → IMPLEMENTING  (instructions from Prateek's design chat saved as design doc)
- 2026-11-21 00:40  Meet  D4  DESIGNING → DESIGNED  (confirmed by Meet)
- 2026-11-21 00:40  Meet  D4  DESIGNED → IMPLEMENTING
- 2026-11-21 01:10  Meet  D4  IMPLEMENTING → REVIEW  (local cluster CLUSTER OK; Docker not run)

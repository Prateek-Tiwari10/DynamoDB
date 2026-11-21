# PROGRESS

Who is doing what, right now. **Pull before you read this file; push right after you change it.**
Each person edits only their own rows. Statuses: `TODO · DESIGNING · DESIGNED · IMPLEMENTING · REVIEW · DONE · BLOCKED`.

## Board

| ID | Task | Owner | Status | Design doc | Updated | Notes |
|---|---|---|---|---|---|---|
| D0-P | Contracts: data, membership, test harness | Prateek | REVIEW | .team/design/D0-P-contracts.md | 2026-11-21 | all parts in: gossip stub + shared test cluster (`tests/conftest.py`: `cluster`, `make_cluster(k)`, kill/restart, `wait_until`); 256 tests pass; REVIEW-LATER #10 |
| D0-M | Contracts: request path, server wiring, build | Meet | REVIEW | .team/design/D0-M-contracts.md | 2026-11-20 | all code pushed incl. server.py; 121 tests pass; see REVIEW-LATER #5 |
| D1 | Storage engine (WAL + snapshot) | Prateek | REVIEW | .team/design/D1-storage.md | 2026-11-20 | both D0-P follow-ups done (config constructor, snapshot error keeps running, retry after next 1000 writes); 31 storage tests pass |
| D2 | Single-node server + CLI | Meet | REVIEW | .team/design/D2-single-node.md | 2026-11-20 | Phase 1 demo passes (`tools/demo_phase1.sh`); 139 tests pass; REVIEW-LATER #6 |
| D3 | Vector clocks & reconciliation | Prateek | REVIEW | .team/design/D3-vector-clocks.md | 2026-11-21 | vclock, version, storage switch; Meet's coordinator/replica swapped (DEC-11, confirmed by Meet); 217 tests pass |
| D4 | Ring + static cluster + Docker | Meet | REVIEW | .team/design/D4-static-cluster.md | 2026-11-21 | local and Docker 5-node cluster checked (`CLUSTER OK`, also after kill + restart of node3); REVIEW-LATER #7 |
| D5 | Coordinator: forwarding + quorum | Meet | REVIEW | .team/design/D5-coordinator.md | 2026-11-21 | Phase 2 demo passes locally and in Docker; 244 tests; REVIEW-LATER #8 |
| D6 | Failure detection + replica sync: 6a gossip, 6b hinted handoff, 6c Merkle anti-entropy | Prateek | IMPLEMENTING | .team/design/D6-failure-detection.md | 2026-11-21 | **6a built and pushed** (gossip + failure detector; 292 tests pass; live: SUSPECT ~5 s, DEAD ~10 s); next: design 6b |
| D7 | Read repair + membership-aware routing | Meet | REVIEW | .team/design/D7-read-repair.md | 2026-11-21 | Phase 3 demo passes locally; 313 tests pass |
| D6b | Hinted handoff: hint store + delivery loop (moved from Prateek, DEC-14) | Meet | REVIEW | .team/design/D6b-hinted-handoff.md | 2026-11-21 | built; 331 tests pass; hints in own `hints.json` (see §9) |
| D8 | Sloppy quorum | Meet | REVIEW | .team/design/D8-sloppy-quorum.md | 2026-11-21 | built; 343 tests pass; `demo_sloppy.sh` passes locally |
| D9 | ~~Hinted handoff~~ merged into D6 (part 6b) | Prateek | — | — | 2026-11-21 | DEC-12 |
| D10 | Chaos + cluster test suite | Prateek | TODO | — | 2026-10-23 | |
| D11 | Benchmarks + graphs | Meet | TODO | — | 2026-10-23 | |
| D12 | Demo runbook + README + interview notes | Prateek | TODO | — | 2026-10-23 | |

## Currently

- **Prateek:** D6a done; next design 6b (hinted handoff), then 6c. D0-P, D1, D3 in REVIEW
- **Meet:** D8, D6b, D7, D0-M, D2, D4, D5 in REVIEW; next: D11 benchmarks

## Blockers / questions for the partner

_(add a line, remove it when answered)_

- **Prateek → Meet (2026-11-21):** **6a gossip is on main** — your D7 can start (states are real now; `subscribe` contract in my line below). Two things touch you: (1) I changed **your** `tests/test_server.py` (commit `685c1bf`): `test_stub_rpcs_unimplemented` used Gossip as the unbuilt-RPC example; it is now `test_gossip_answers_with_its_table` (your hinted-ReplicaPut test still covers UNIMPLEMENTED). (2) The first gossip round runs one interval after start: a round at t=0 hit not-yet-started peers and put the channels in your 1 s reconnect backoff, which made 4 of your `test_cluster_requests.py` tests fail (UNAVAILABLE / QUORUM_FAILED). Worth knowing for D7/D8 tests: right after a peer restarts, its channel can need up to ~1 s.
- **Prateek → Meet (2026-11-21):** shared test cluster is in `tests/conftest.py` (`cluster` = 5 nodes, `make_cluster(k)`, `kill` / `stop_node` / `restart`, `cluster.wait_until`). Use it for D5/D7/D8 tests. Your `test_static_cluster.py` and `test_cluster_requests.py` still have their own local fixtures; they work as is, switch when convenient. Also found: gRPC binds with `SO_REUSEPORT`, so a second node on a taken port starts *silently* and shares the port; `grpc.aio.server(options=[("grpc.so_reuseport", 0)])` in `build_node` would make that fail loudly (REVIEW-LATER #10, your call).
- **Prateek → Meet (2026-11-20):** new rule: we finish the project first and review after. Nobody waits for a review; finished tasks go to REVIEW and a line goes into `.team/REVIEW-LATER.md`. We go through that list once the first full version runs. Wrong or unsafe things are still fixed at once. Add your own review items there.
- **Meet → Prateek (2026-11-12):** D0-M is DESIGNED. DEC-02 to DEC-08 confirmed. `DeliverHint` and the tombstone purge / `last_alive` are removed from the D0-M doc, and it now uses your frozen names (`storage.load()`, `Storage(data_dir, config)`, `vclock.advance`, `MEMBER_STATE_UNSPECIFIED`). Please confirm DEC-09 (my half frozen; INTERFACES block marked FINAL).



- **Meet → Prateek (2026-11-21):** DEC-12 and DEC-13 confirmed. D8 now needs D6b instead of D9. Thanks for the gossip start-delay fix and the test_server.py change. Taking `grpc.so_reuseport = 0` in `build_node` (REVIEW-LATER #10). Starting D7 on top of 6a.
- **Meet → Prateek (2026-11-21):** **D7 is on main** (REVIEW). Things that touch you: (1) fan-out now skips nodes your membership says are DEAD (SUSPECT still asked); fast fail still counts only ALIVE. (2) `_wait_for` returns `(answers, late)` now, so if 6b code calls it, unpack two values. (3) `tests/test_read_repair.py` uses your `cluster` fixture; it reads again until the repair shows up, because right after `restart` the node can still be DEAD in the coordinator's view for ~1 s. (4) New `tools/demo_phase3.sh` (failure detection, read repair, siblings).
- **Meet → Prateek (2026-11-21):** as agreed, I took **6b** (DEC-14, please read): row D6b, own design doc, I own `handoff.py` and only the hint parts of your `storage.py` (put_hint / hints / drop_hint, hint lines in replay, hints in the snapshot); you review those. I did not touch your D6 row or D6 doc; please mark 6b there as moved to Meet.
- **Meet → Prateek (2026-11-21):** **D6b is on main** (REVIEW, REVIEW-LATER #13). In your `storage.py` I added only the hint parts; to keep your snapshot format and your tests unchanged, hints go to their own `hints.json` written in the same snapshot step (D6b §9). I also moved the log-write and line-count steps of `apply` into two small helpers shared with the hint functions; all your storage and crash tests pass unchanged. INTERFACES still says handoff.py is yours / "logic in D9" — shared file, OK if I update it to DEC-14?
- **Meet → Prateek (2026-11-21):** **D8 is on main** (REVIEW, REVIEW-LATER #14). For your D10 cluster tests 3/4: with `sloppy_quorum` on, every owner that is not ALIVE (SUSPECT too, my choice) gets a stand-in; reads stay strict (owners only, R real copies). A test that wants the D5/D7 behaviour must use `make_cluster(5, sloppy_quorum=False)` — a write ~0.5 s after a kill in test timers goes through stand-ins. New `tools/demo_sloppy.sh`.

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
- 2026-11-21 01:30  Prateek  D3  IMPLEMENTING → REVIEW  (vclock, version, storage merge_into; Meet's coordinator/replica swapped with Prateek's OK; DEC-11; 217 tests pass)
- 2026-11-21 01:40  Meet  D5  TODO → DESIGNING
- 2026-11-21 02:00  Meet  DEC-11  confirmed
- 2026-11-21 02:00  Meet  D5  DESIGNING → DESIGNED → IMPLEMENTING  (confirmed by Meet)
- 2026-11-21 19:20  Meet  D5  IMPLEMENTING → REVIEW  (Phase 2 demo passes locally and in Docker)
- 2026-11-21 00:52  Prateek  D0-P  IMPLEMENTING → REVIEW  (gossip stub, tests/conftest.py cluster fixture, wait_until; 256 tests pass)
- 2026-11-21 00:57  Prateek  D6  TODO → DESIGNING  (D6 + D9 + S1 combined into D6, DEC-12)
- 2026-11-21 01:21  Prateek  D6  DESIGNING → IMPLEMENTING  (6a designed and saved, DEC-13; 6b and 6c designed later in the same doc)
- 2026-11-21 01:30  Prateek  D6  6a built and pushed (gossip + failure detector, DEC-13); D6 stays IMPLEMENTING until 6c
- 2026-11-21 20:00  Meet  DEC-12, DEC-13  confirmed
- 2026-11-21 20:20  Meet  D7  TODO → DESIGNING
- 2026-11-21 11:25  Meet  D7  DESIGNING → DESIGNED → IMPLEMENTING  (confirmed by Meet; fast fail counts only ALIVE)
- 2026-11-21 11:33  Meet  D7  IMPLEMENTING → REVIEW  (read repair, DEAD-aware fan-out, demo_phase3.sh; 313 tests pass)
- 2026-11-21 11:49  Meet  D6b  TODO → DESIGNING  (6b moved from Prateek, DEC-14)
- 2026-11-21 11:52  Meet  D6b  DESIGNING → DESIGNED → IMPLEMENTING  (confirmed by Meet)
- 2026-11-21 11:59  Meet  D6b  IMPLEMENTING → REVIEW  (hint store + delivery loop; 331 tests pass)
- 2026-11-21 12:03  Meet  D8  TODO → DESIGNING
- 2026-11-21 12:10  Meet  D8  DESIGNING → DESIGNED → IMPLEMENTING  (confirmed by Meet)
- 2026-11-21 12:20  Meet  D8  IMPLEMENTING → REVIEW  (stand-ins, demo_sloppy.sh; 343 tests pass)

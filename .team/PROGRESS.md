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
| D7 | Read repair + membership-aware routing | Meet | IMPLEMENTING | .team/design/D7-read-repair.md | 2026-11-21 | design confirmed (fast fail counts only ALIVE) |
| D8 | Sloppy quorum | Meet | TODO | — | 2026-10-23 | |
| D9 | ~~Hinted handoff~~ merged into D6 (part 6b) | Prateek | — | — | 2026-11-21 | DEC-12 |
| D10 | Chaos + cluster test suite | Prateek | TODO | — | 2026-10-23 | |
| D11 | Benchmarks + graphs | Meet | TODO | — | 2026-10-23 | |
| D12 | Demo runbook + README + interview notes | Prateek | TODO | — | 2026-10-23 | |

## Currently

- **Prateek:** D6a done; next design 6b (hinted handoff), then 6c. D0-P, D1, D3 in REVIEW
- **Meet:** D7 implementing (D0-M, D2, D4, D5 in REVIEW)

## Blockers / questions for the partner

_(add a line, remove it when answered)_

- **Prateek → Meet (2026-11-21):** **6a gossip is on main** — your D7 can start (states are real now; `subscribe` contract in my line below). Two things touch you: (1) I changed **your** `tests/test_server.py` (commit `685c1bf`): `test_stub_rpcs_unimplemented` used Gossip as the unbuilt-RPC example; it is now `test_gossip_answers_with_its_table` (your hinted-ReplicaPut test still covers UNIMPLEMENTED). (2) The first gossip round runs one interval after start: a round at t=0 hit not-yet-started peers and put the channels in your 1 s reconnect backoff, which made 4 of your `test_cluster_requests.py` tests fail (UNAVAILABLE / QUORUM_FAILED). Worth knowing for D7/D8 tests: right after a peer restarts, its channel can need up to ~1 s.
- **Prateek → Meet (2026-11-21):** shared test cluster is in `tests/conftest.py` (`cluster` = 5 nodes, `make_cluster(k)`, `kill` / `stop_node` / `restart`, `cluster.wait_until`). Use it for D5/D7/D8 tests. Your `test_static_cluster.py` and `test_cluster_requests.py` still have their own local fixtures; they work as is, switch when convenient. Also found: gRPC binds with `SO_REUSEPORT`, so a second node on a taken port starts *silently* and shares the port; `grpc.aio.server(options=[("grpc.so_reuseport", 0)])` in `build_node` would make that fail loudly (REVIEW-LATER #10, your call).
- **Prateek → Meet (2026-11-20):** new rule: we finish the project first and review after. Nobody waits for a review; finished tasks go to REVIEW and a line goes into `.team/REVIEW-LATER.md`. We go through that list once the first full version runs. Wrong or unsafe things are still fixed at once. Add your own review items there.
- **Meet → Prateek (2026-11-12):** D0-M is DESIGNED. DEC-02 to DEC-08 confirmed. `DeliverHint` and the tombstone purge / `last_alive` are removed from the D0-M doc, and it now uses your frozen names (`storage.load()`, `Storage(data_dir, config)`, `vclock.advance`, `MEMBER_STATE_UNSPECIFIED`). Please confirm DEC-09 (my half frozen; INTERFACES block marked FINAL).



- **Meet → Prateek (2026-11-21):** DEC-12 and DEC-13 confirmed. D8 now needs D6b instead of D9. Thanks for the gossip start-delay fix and the test_server.py change. Taking `grpc.so_reuseport = 0` in `build_node` (REVIEW-LATER #10). Starting D7 on top of 6a.

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

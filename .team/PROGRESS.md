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
| D6 | Failure detection + replica sync: 6a gossip, ~~6b~~ (Meet, row D6b, DEC-14), 6c Merkle anti-entropy | Prateek | REVIEW | .team/design/D6-failure-detection.md | 2026-11-21 | 6a + 6c built (6b is Meet's D6b row); Merkle anti-entropy: `merkle.py`, `antientropy.py`, DEC-15 (Meet to confirm); 382 tests pass; live check: one-copy key on all 3 owners in 18 s with no read; REVIEW-LATER #15–#17 |
| D7 | Read repair + membership-aware routing | Meet | REVIEW | .team/design/D7-read-repair.md | 2026-11-21 | Phase 3 demo passes locally; 313 tests pass |
| D6b | Hinted handoff: hint store + delivery loop (moved from Prateek, DEC-14) | Meet | REVIEW | .team/design/D6b-hinted-handoff.md | 2026-11-21 | built; 331 tests pass; hints in own `hints.json` (see §9) |
| D8 | Sloppy quorum | Meet | REVIEW | .team/design/D8-sloppy-quorum.md | 2026-11-21 | built; 343 tests pass; `demo_sloppy.sh` passes locally and in Docker |
| D9 | ~~Hinted handoff~~ merged into D6 (part 6b) | Prateek | — | — | 2026-11-21 | DEC-12 |
| D10 | Chaos + cluster test suite | Prateek | REVIEW | .team/design/D10-chaos-cluster-tests.md | 2026-11-22 | `make cluster-test` 9/9 green on local processes (7 min); 5-min chaos: 15 faults, 31,146 acked writes, **LOST 0**, converged in 22 s (`results/chaos-20261121-2355.md`); Docker target not run yet (no Docker here); REVIEW-LATER #21–#23 |
| D11 | Benchmarks + graphs | Meet | REVIEW | .team/design/D11-benchmarks.md | 2026-11-21 | built; `results/README.md` (4 graphs + takeaways); found restart slowdown (REVIEW-LATER #19); 400 tests pass |
| S2 | Live dashboard (optional) | Meet | IMPLEMENTING | .team/design/S2-dashboard.md | 2026-11-22 | built: `python -m tools.dashboard` (127.0.0.1:8080, ssh tunnel); node cards, who-sees-whom grid, ring, key lookup, put/get/delete; 418 tests pass; running on the Azure VM |
| D12 | Demo runbook + README + interview notes | Prateek | TODO | — | 2026-10-23 | |

## Currently

- **Prateek:** next: D12 demo runbook + README + interview notes. D10, D6, D0-P, D1, D3 in REVIEW
- **Meet:** S2, D11, D8, D6b, D7, D0-M, D2, D4, D5 in REVIEW

## Blockers / questions for the partner

_(add a line, remove it when answered)_

- **Prateek → Meet (2026-11-22, D10):** **D10 is on main** (REVIEW, REVIEW-LATER #21–#23). `make cluster-test` (9 tests on the real cluster, real timers, incl. a 5-min chaos run) is green on local processes; the chaos report says 31,146 acked writes, LOST 0. Please (1) run it once on Docker: `CLUSTER_TARGET=docker make cluster-test PYTHON=.venv/bin/python` (I have no Docker; it uses `docker-compose.nosloppy.yml` for test 3 and wipes `./data`), and (2) OK the shared bits (#22): `CONFIG=${CONFIG:-…}` in your `tools/cluster_local.sh`, `cluster.local.nosloppy.yaml`, `cluster.nosloppy.yaml`, `docker-compose.nosloppy.yml`, the `cluster-test` / `chaos` Makefile targets. (3) For D7 (#23): right after a restart, a coordinator's channel to the node can be in reconnect backoff ≤ 1 s while gossip already says ALIVE, so a read then skips it and read repair misses it — fine as best-effort, just so you know. Your #20: my `.venv` is Python 3.12.15 now (uv had it cached), 3/3 full runs green — try that. DEC-16 (matplotlib): see Prateek's answer when he reads it.
- **Meet → Prateek (2026-11-21):** **DEC-15 confirmed**, and your §17 edits to my `server.py` / `replica.py` are fine (I will still look at them under REVIEW-LATER #16). Thanks for updating the handoff.py heading in INTERFACES. Segfault: yes, I see it too on Python 3.14 (it hit me during D8), so a 3.11 venv is worth a try (#17b). I removed your 6c line and your DEC-14 line.
- **Meet → Prateek (2026-11-21):** **D11 is on main** (REVIEW, REVIEW-LATER #18): `python -m tools.bench run` / `plot`, results and takeaways in `results/README.md`. Worth a look for D10/D12: the timeline run shows that a node's **restart** (not its kill) drops throughput ~5–20x for ~20 s (`results/timeline.png`, #19). Also: the full suite in one process segfaults for me too on 3.14 (#20); in two halves all 400 pass.
- **Meet → Prateek (2026-11-21):** D11 is designed (`.team/design/D11-benchmarks.md`). Please agree to **DEC-16**: `matplotlib` in `requirements.txt` (shared file; SPEC already lists it). Until then I only install it in my venv. The bench starts its own local cluster on ports 50061+ and a temp data dir, so it never touches a cluster on 50051–55.
- **Prateek → Meet (2026-11-20):** new rule: we finish the project first and review after. Nobody waits for a review; finished tasks go to REVIEW and a line goes into `.team/REVIEW-LATER.md`. We go through that list once the first full version runs. Wrong or unsafe things are still fixed at once. Add your own review items there.
- **Meet → Prateek (2026-11-12):** D0-M is DESIGNED. DEC-02 to DEC-08 confirmed. `DeliverHint` and the tombstone purge / `last_alive` are removed from the D0-M doc, and it now uses your frozen names (`storage.load()`, `Storage(data_dir, config)`, `vclock.advance`, `MEMBER_STATE_UNSPECIFIED`). Please confirm DEC-09 (my half frozen; INTERFACES block marked FINAL).



- **Meet → Prateek (2026-11-21):** **D7 is on main** (REVIEW). Things that touch you: (1) fan-out now skips nodes your membership says are DEAD (SUSPECT still asked); fast fail still counts only ALIVE. (2) `_wait_for` returns `(answers, late)` now, so if 6b code calls it, unpack two values. (3) `tests/test_read_repair.py` uses your `cluster` fixture; it reads again until the repair shows up, because right after `restart` the node can still be DEAD in the coordinator's view for ~1 s. (4) New `tools/demo_phase3.sh` (failure detection, read repair, siblings).
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
- 2026-11-21 12:26  Meet  D7, D8  Docker demos passed (demo_phase3.sh, demo_sloppy.sh)
- 2026-11-21 20:08  Prateek  D6  6b marked as moved to Meet (DEC-14); 6c next to design
- 2026-11-21 22:30  Prateek  D6  6c DESIGNED (§11–§21, DEC-15 agreed by Prateek; Meet to confirm), implementing
- 2026-11-21 22:55  Prateek  D6  IMPLEMENTING → REVIEW  (6c built: merkle.py, antientropy.py, DEC-15; 382 tests pass)
- 2026-11-21 23:10  Meet  D11  TODO → DESIGNING
- 2026-11-21 23:25  Meet  D11  DESIGNING → DESIGNED → IMPLEMENTING  (confirmed by Meet; DEC-15 confirmed, DEC-16 proposed)
- 2026-11-21 23:20  Prateek  D10  TODO → DESIGNING → DESIGNED → IMPLEMENTING  (design saved; §6 no-loss rule fixed: chain of stored writes; venv on Python 3.12)
- 2026-11-21 23:59  Meet  D11  IMPLEMENTING → REVIEW  (bench.py, results/ with 4 graphs; 400 tests pass in two halves)
- 2026-11-22 00:10  Prateek  D10  IMPLEMENTING → REVIEW  (9/9 cluster tests green locally; 5-min chaos LOST 0; Docker run left to Meet)
- 2026-11-22 00:40  Meet  S2  TODO → DESIGNING
- 2026-11-22 00:55  Meet  S2  DESIGNING → DESIGNED → IMPLEMENTING  (confirmed by Meet: put/get/delete box, no kill buttons)
- 2026-11-22 10:35  Meet  S2  IMPLEMENTING → REVIEW  (dashboard built, 16 tests; deployed on the Azure VM)
- 2026-11-22 11:00  Meet  S2  REVIEW → IMPLEMENTING  (design change §10: node controls kill/stop/pause/resume/start, confirmed by Meet)

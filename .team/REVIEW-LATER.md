# Review later

Rule (Prateek, 2026-11-20): **finish the project first, review after.** Nobody waits for a review
to start the next task. A finished task is set to `REVIEW` on the board and a line is added here.
Once the first full version runs end to end (5 nodes, put/get/delete, a node killed and brought
back), we go through this list together.

What does **not** wait for this list: anything that is wrong, unsafe, or that we could not defend
in an interview. Those are fixed at once.

Style we agreed on: this is a two-person student project. When a simple design is correct and we
can explain it, we keep it simple and write down why (design doc section 3 and section 8), instead
of copying what a production system like Dynamo or Cassandra does.

## To review

| # | What | Built by | Reviewer | Added | Notes |
|---|---|---|---|---|---|
| 1 | D1 storage: code, tests, design doc | Prateek | Meet | 2026-11-20 | Snapshot retry rule changed 2026-11-20 (retry after another 1000 writes); see D1 §5.3 and §9 |
| 2 | D0-P code: `config.py`, `membership.py`, `handoff.py` skeletons, `cluster*.yaml` | Prateek | Meet | 2026-11-20 | rest of D0-P (vclock, version, gossip stub, test fixture) still being built |
| 3 | DEC-09 (Meet's half of the interfaces) | Meet | Prateek | 2026-11-20 | from Meet's blocker line of 2026-11-12 |
| 4 | Prateek's messages in `proto/dynamo/dynamo.proto` (copied by Meet from D0-P 4.3) | Meet | Prateek | 2026-11-20 | checked anyway when `vclock.py` / `version.py` are built |
| 5 | D0-M code (proto, build, reqid, ring, cli, coordinator/peers/replica, server.py, Docker) | Meet | Prateek | 2026-11-20 | done 2026-11-20; changes from the design in D0-M §9 |
| 6 | D2 single node: coordinator local path, temporary context helpers, `cluster.single.yaml`, `Makefile` CONFIG, demo script, tests | Meet | Prateek | 2026-11-21 | DEC-10; helpers to swap for `version.py` in D3 |
| 7 | D4 static cluster: Q1–Q5 decisions, `tools/cluster_local.sh`, `tools/check_cluster.sh`, `test_static_cluster.py` | Meet | Prateek | 2026-11-21 | Docker checked by Meet 2026-11-21 (CLUSTER OK); compose image-tag fix |
| 8 | D5 coordinator: routing, forwarding, quorum loop `_wait_for`, `wire.py`, CLI context-file fix, `demo_phase2.sh`, tests | Meet | Prateek | 2026-11-21 | known limitation until D6: Forward to a killed first replica in Docker waits the whole budget (see D5 §9) |
| 8 | D3 vector clocks: `vclock.py`, `version.py`, storage switch, DEC-11 | Prateek | Meet | 2026-11-21 | JSON context replaces proto context; no same-value collapse |
| 9 | Prateek's edits to Meet's `coordinator.py`, `replica.py`, `tests/test_coordinator.py` (commit `c70fa57`) | Prateek | Meet | 2026-11-21 | made so main stayed green; check they match your D5 plans |
| 10 | D0-P rest: `gossip.py` stub, shared test cluster `tests/conftest.py`, `tests/test_cluster_harness.py` | Prateek | Meet | 2026-11-21 | 5 questions answered in D0-P §9; kill also cancels coordinator tasks; suggestion for Meet: `grpc.so_reuseport = 0` in `build_node` |
| 11 | Prateek's edit to Meet's `tests/test_server.py` (commit `685c1bf`): Gossip-unimplemented test → Gossip answers with table | Prateek | Meet | 2026-11-21 | needed because 6a built the Gossip RPC |
| 12 | D7 read repair + DEAD-aware routing: `repair_plan`, `_repair`, `_reachable`, `_wait_for` returns late tasks, `test_read_repair.py`, `demo_phase3.sh`; also the `so_reuseport = 0` fix + test (commit `874682d`) | Meet | Prateek | 2026-11-21 | Q5: fast fail counts only ALIVE (Meet); deviations in D7 §9 |
| 13 | D6b hinted handoff: hint parts of `storage.py` (`put_hint`, `hints`, `drop_hint`, replay, `hints.json`), `handoff.py` loop, `replica.py` hinted ReplicaPut, tests | Meet | Prateek | 2026-11-21 | touches Prateek's `storage.py` (DEC-14); deviation: `hints.json` instead of snapshot format 2 (D6b §9) |
| 14 | D8 sloppy quorum: `_targets`, `_stand_ins`, hinted `_replica_put`, `test_sloppy_quorum.py`, `demo_sloppy.sh`, demo_phase3 step 2 change | Meet | Prateek | 2026-11-21 | Q3 strict reads, Q5 SUSPECT owners get a stand-in (Meet); see D8 §9 |
| 15 | D6 (6a gossip + 6c Merkle anti-entropy): `membership.py`, `gossip.py`, `merkle.py`, `antientropy.py`, `storage.keys()`, tests, design doc §1–§21 + both "As built" | Prateek | Meet | 2026-11-21 | DEC-13, DEC-15 (Meet to confirm); deviations in the two As-built sections |
| 16 | Prateek's edits to Meet's `server.py` and `replica.py` for 6c (commit `22675ff`): `anti_entropy` wiring + `CompareTree` / `ExchangeBuckets` handlers | Prateek | Meet | 2026-11-21 | exactly the D6 §17 lines |
| 17 | Known limits from 6c: (a) `ExchangeBuckets` of 32 buckets with 1 MiB values can pass gRPC's 4 MB limit (cap by bytes or raise the limit?); (b) Python 3.14 + grpcio 1.84 venv: full suite sometimes segfaults in grpc GC (also before 6c) — try a 3.11 venv like Docker | Prateek | both | 2026-11-21 | not a bug in our logic; decide after the first full version |
| 18 | D11 benchmarks: `tools/bench.py`, `tests/test_bench.py`, `results/` (README with takeaways) | Meet | Prateek | 2026-11-21 | deviations in D11 §9; DEC-16 matplotlib still open |
| 19 | Found by D11 timeline: after a killed node restarts, throughput drops ~5–20x for ~20 s, p99 1–3 s (`results/timeline.png`). Candidates: hinted handoff (one ReplicaPut + fsync per hint, all owners' stand-ins at once), read repair on every stale key, anti-entropy round. Measure each (turn one off at a time) before fixing | Meet | both | 2026-11-21 | not fixed in D11 (measurement task); touches handoff.py (Meet), storage fsync (Prateek), antientropy (Prateek) |
| 20 | Full suite in one process segfaults in grpc GC on Python 3.14 (2 of 2 runs today); split in two halves it passes: 400 passed, 1 skipped. Same as #17b | Meet | both | 2026-11-21 | Prateek's venv is on 3.12 now (D10 row) — I will try that too |
| 21 | S2 dashboard: `tools/dashboard.py`, `tools/dashboard.html`, `tests/test_dashboard.py` | Meet | Prateek | 2026-11-22 | also node controls, disk view of hints, public mode (self-heal, max 2 down, rate limits); S2 §9–§12 |
| 22 | `tests/test_gossip.py::test_partition_heals` is timing-flaky: failed 1 of 4 runs on Meet's laptop (`assert 3 == 1` at line 131: node1 saw node2 DEAD) | Prateek | Prateek | 2026-11-22 | not touched by S2; maybe a longer wait or test timers |
| 23 | Azure deployment (VM-only, not in git): Caddy, `dynamo-cluster` + `dynamo-dashboard` systemd units, NSG `allow-web` 80/443 and ssh limited to Meet's networks; costs ~$17/month (B1ms 24/7) | Meet | both | 2026-11-22 | `az group delete -n dynamo-rg` removes everything |
| 21 | D10 chaos + cluster tests: `tools/cluster_control.py`, `tools/chaos.py` (no-loss chain rule §6), `tests/cluster/` (9 tests), reports in `results/` | Prateek | Meet | 2026-11-22 | deviations in D10 §12; Docker target written but not run |
| 22 | D10 shared bits: `CONFIG` in Meet's `tools/cluster_local.sh`, `cluster.local.nosloppy.yaml`, `cluster.nosloppy.yaml`, `docker-compose.nosloppy.yml`, Makefile `cluster-test` / `chaos` | Prateek | Meet | 2026-11-22 | design §9 said "one OK from Meet" |
| 23 | Found by D10 test 6: just after a restart, a coordinator's outgoing channel can be in reconnect backoff (≤ 1 s) while gossip already shows the node ALIVE → a read skips it, read repair misses it (anti-entropy fixes it later). Maybe reset/try the channel when a peer turns ALIVE? Possibly related to #19 | Prateek | Meet | 2026-11-22 | best-effort today; not a data-loss bug |

## Done

| # | What | Reviewed by | Date | Result |
|---|---|---|---|---|

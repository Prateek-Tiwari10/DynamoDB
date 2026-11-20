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

## Done

| # | What | Reviewed by | Date | Result |
|---|---|---|---|---|

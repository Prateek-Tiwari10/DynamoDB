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
| 5 | D0-M code (proto, build, reqid, ring, cli, coordinator/peers/replica, Docker) | Meet | Prateek | 2026-11-20 | |

## Done

| # | What | Reviewed by | Date | Result |
|---|---|---|---|---|

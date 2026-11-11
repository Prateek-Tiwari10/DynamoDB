# PROGRESS

Who is doing what, right now. **Pull before you read this file; push right after you change it.**
Each person edits only their own rows. Statuses: `TODO · DESIGNING · DESIGNED · IMPLEMENTING · REVIEW · DONE · BLOCKED`.

## Board

| ID | Task | Owner | Status | Design doc | Updated | Notes |
|---|---|---|---|---|---|---|
| D0-P | Contracts: data, membership, test harness | Prateek | DESIGNING | .team/design/D0-P-contracts.md | 2026-11-11 | no design doc yet; the 17 D0-M answers (DEC-02 to DEC-07) already fix most of it; storage block of INTERFACES.md is final |
| D0-M | Contracts: request path, server wiring, build | Meet | DESIGNING | .team/design/D0-M-contracts.md | 2026-10-23 | split confirmed by Meet |
| D1 | Storage engine (WAL + snapshot) | Prateek | IMPLEMENTING | .team/design/D1-storage.md | 2026-11-11 | design updated (DEC-04, DEC-06): rename to `load`, exit on log error, `counts()` |
| D2 | Single-node server + CLI | Meet | TODO | — | 2026-10-23 | |
| D3 | Vector clocks & reconciliation | Prateek | TODO | — | 2026-10-23 | |
| D4 | Ring + static cluster + Docker | Meet | TODO | — | 2026-10-23 | |
| D5 | Coordinator: forwarding + quorum | Meet | TODO | — | 2026-10-23 | |
| D6 | Membership + gossip + failure detector | Prateek | TODO | — | 2026-10-23 | |
| D7 | Read repair + membership-aware routing | Meet | TODO | — | 2026-10-23 | |
| D8 | Sloppy quorum | Meet | TODO | — | 2026-10-23 | |
| D9 | Hinted handoff store + delivery | Prateek | TODO | — | 2026-10-23 | |
| D10 | Chaos + cluster test suite | Prateek | TODO | — | 2026-10-23 | |
| D11 | Benchmarks + graphs | Meet | TODO | — | 2026-10-23 | |
| D12 | Demo runbook + README + interview notes | Prateek | TODO | — | 2026-10-23 | |

## Currently

- **Prateek:** D1 code changes after the 17 answers; D0-P design still open
- **Meet:** D0-M design

## Blockers / questions for the partner

_(add a line, remove it when answered)_

- **Prateek → Meet (2026-11-11):** Prateek answered all 17 D0-M items, see DEC-02 to DEC-07. Item 8: reuse ReplicaPut. Item 14: changed to no purge. The entries say "agreed by Prateek; Meet to confirm"; `SPEC.md`, `TASKS.md`, `CLAUDE.md` and the new `INTERFACES.md` are updated to match. Your D0-M doc is not edited.
- **Prateek → Meet (2026-11-11): two answers differ from your D0-M doc, please look at these first.**
  1. **Item 8 (DEC-04):** no `DeliverHint` RPC. Hint delivery uses `ReplicaPut` with an empty `hint_for`. Remove `DeliverHint` from your `NodeServiceServicer` and proto half.
  2. **Item 14 (DEC-06):** no tombstone purge and no `last_alive` guard. Tombstones are kept forever (known limitation). Your Q8 and the `last_alive` edge-case row no longer apply.
- **Prateek → Meet (2026-11-11):** storage names are final in `.team/INTERFACES.md`: `load / get / apply / counts / close`. `Node.start()` calls `storage.load()`. `Version` lives in `dynamo/storage.py` for now. There is no `Makefile` yet: tests run with `python -m pytest tests` from the repo root.

- **Meet → Prateek (2026-11-10):** D0-M draft is in `.team/design/D0-M-contracts.md`. Please review the "Needs Prateek" list at the top (17 items, updated 2026-11-10: vector clocks + `version.py` owner, coordinator counter rule, NOT_FOUND returns context, gossip states/incarnation, ring fingerprint in gossip entry, `Membership.state()`/`members()`, drop `on_contact`, DeliverHint own RPC?, proto path, `STATUS_UNSPECIFIED = 0`, `request_timeout_ms`, env-var settings rule, WAL-failure exit, tombstone purge + `last_alive` guard, `Locate`/`LocalGet`, `pyproject.toml` in allowed paths, logging filter vs your logging helper). Agree or comment so we can add DEC entries.

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

# PROGRESS

Who is doing what, right now. **Pull before you read this file; push right after you change it.**
Each person edits only their own rows. Statuses: `TODO · DESIGNING · DESIGNED · IMPLEMENTING · REVIEW · DONE · BLOCKED`.

## Board

| ID | Task | Owner | Status | Design doc | Updated | Notes |
|---|---|---|---|---|---|---|
| D0-P | Contracts: data, membership, test harness | Prateek | IMPLEMENTING | .team/design/D0-P-contracts.md | 2026-11-12 | design saved (DEC-08, Meet to confirm); INTERFACES.md: all Prateek blocks final |
| D0-M | Contracts: request path, server wiring, build | Meet | DESIGNED | .team/design/D0-M-contracts.md | 2026-11-12 | design frozen (DEC-09, Prateek to confirm); DEC-02 to DEC-08 confirmed |
| D1 | Storage engine (WAL + snapshot) | Prateek | IMPLEMENTING | .team/design/D1-storage.md | 2026-11-12 | two follow-ups from D0-P: `Storage(data_dir, config)`, snapshot error keeps the node running |
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

- **Prateek:** D0-P implementation, plus two small D1 follow-ups
- **Meet:** D0-M designed; implementation next

## Blockers / questions for the partner

_(add a line, remove it when answered)_

- **Meet → Prateek (2026-11-12):** D0-M is DESIGNED. DEC-02 to DEC-08 confirmed. `DeliverHint` and the tombstone purge / `last_alive` are removed from the D0-M doc, and it now uses your frozen names (`storage.load()`, `Storage(data_dir, config)`, `vclock.advance`, `MEMBER_STATE_UNSPECIFIED`). Please confirm DEC-09 (my half frozen; INTERFACES block marked FINAL).

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

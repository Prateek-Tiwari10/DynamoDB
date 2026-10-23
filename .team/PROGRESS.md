# PROGRESS

Who is doing what, right now. **Pull before you read this file; push right after you change it.**
Each person edits only their own rows. Statuses: `TODO · DESIGNING · DESIGNED · IMPLEMENTING · REVIEW · DONE · BLOCKED`.

## Board

| ID | Task | Owner | Status | Design doc | Updated | Notes |
|---|---|---|---|---|---|---|
| D0-P | Contracts: data, membership, test harness | Prateek | DESIGNING | .team/design/D0-P-contracts.md | 2026-10-23 | restarted clean, old Phase 1 work in .team/archive/pre-d0/ |
| D0-M | Contracts: request path, server wiring, build | Meet | TODO | — | 2026-10-23 | |
| D1 | Storage engine (WAL + snapshot) | Prateek | TODO | — | 2026-10-23 | |
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

- **Prateek:** D0-P design
- **Meet:** —

## Blockers / questions for the partner

_(add a line, remove it when answered)_

## Log

_Append one line per status change: `YYYY-MM-DD HH:MM  Name  ID  OLD → NEW  (short note)`_

- 2026-10-23  setup  board created
- 2026-10-23 23:42  Prateek  D0  TODO → DESIGNING  (restart from D0; pre-D0 code archived)
- 2026-10-23 23:50  Prateek  D0  split into D0-P (Prateek, DESIGNING) and D0-M (Meet, TODO) — Meet to confirm

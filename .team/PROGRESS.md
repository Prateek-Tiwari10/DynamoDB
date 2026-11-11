# PROGRESS

Who is doing what, right now. **Pull before you read this file; push right after you change it.**
Each person edits only their own rows. Statuses: `TODO · DESIGNING · DESIGNED · IMPLEMENTING · REVIEW · DONE · BLOCKED`.

## Board

| ID | Task | Owner | Status | Design doc | Updated | Notes |
|---|---|---|---|---|---|---|
| D0-P | Contracts: data, membership, test harness | Prateek | DESIGNING | .team/design/D0-P-contracts.md | 2026-10-23 | restarted clean, old Phase 1 work in .team/archive/pre-d0/ |
| D0-M | Contracts: request path, server wiring, build | Meet | DESIGNING | .team/design/D0-M-contracts.md | 2026-10-23 | split confirmed by Meet |
| D1 | Storage engine (WAL + snapshot) | Prateek | REVIEW | .team/design/D1-storage.md | 2026-11-11 | Meet to review; names follow D0-M draft: recover / get / apply / close; 287 writes/s |
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

- **Prateek:** D1 in REVIEW; D0-P design still open
- **Meet:** D0-M design

## Blockers / questions for the partner

_(add a line, remove it when answered)_

- **Prateek → Meet (2026-11-11):** D1 is in REVIEW (`dynamo/storage.py`, design + "As built" in `.team/design/D1-storage.md`). It was built before D0 is frozen, using your names `recover / get / apply / close`. Open from your list: item 13 (today a failed log write returns False, the node keeps running), item 14 (no tombstone purge yet), `put_hint` (not built, comes with D9), `Version` lives in `dynamo/storage.py` for now (not `version.py`). There is no `Makefile` yet: tests run with `python -m pytest tests` from the repo root.

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

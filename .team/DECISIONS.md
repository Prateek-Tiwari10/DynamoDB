# DECISIONS

Cross-cutting decisions that both people agreed on. Anything that changes a shared file
(proto, config, interfaces, spec) or changes behaviour another task depends on goes here.
Never edit an old entry — add a new one that supersedes it.

Format:

```
## DEC-NN — short title  (YYYY-MM-DD, agreed by Prateek + Meet, from Dx)
Decision: one or two lines.
Why: one line.
Rejected: the alternative and why.
Affects: tasks / files.
```

---

## DEC-01 — Design first, simplest explainable solution  (2026-10-23, both)
Decision: every task is designed and discussed before any code; we pick the simplest correct option we can explain in an interview, not the most optimal one.
Why: same approach as our previous project; interviews reward clear reasoning over cleverness.
Rejected: coding first and designing as we go — leads to rewrites when parts meet.
Affects: all tasks.

## DEC-02 — Versioning and clocks  (2026-11-11, agreed by Prateek; Meet to confirm, from D0-M items 1, 2, 3)
Decision: vector clocks as in the SPEC. A replica merges an incoming version with what it holds (drop dominated, keep concurrent); siblings with the same value and tombstone flag collapse into one with merged clocks. `dynamo/version.py` is added, owned by Prateek. The coordinator picks the new counter as `max(context[self], highest own counter among its local versions of the key) + 1` under a per-key lock; the clock code accepts that counter. Get returns a context even when the answer is NOT_FOUND.
Why: concurrent writes must not be lost silently; two writes with the same context through one coordinator must not get the same clock; a put after a delete must replace the tombstone.
Rejected: (timestamp, writer) last-write-wins — clock skew can let an older write win. Counter = `context[self] + 1` only — identical clocks for different values.
Known limitation: two such writes get counters 4 and 5 and look ordered; dotted version vectors would fix it.
Affects: `version.py`, `vclock.py`, coordinator, SPEC data model and Get rule; D3, D5.

## DEC-03 — Membership and gossip  (2026-11-11, agreed by Prateek; Meet to confirm, from D0-M items 4, 5, 6, 7)
Decision: states as in the SPEC (ALIVE, SUSPECT after 3 s, DEAD after 10 s). `incarnation` is the node's boot time in ms. All peers start ALIVE and the timer starts at boot. Each gossip entry carries the node's `ring_fingerprint`. `Membership` offers `state(node_id) -> MemberState` and `members() -> list[Member]`. `membership.on_contact(sender)` is dropped: liveness comes only from gossip.
Why: one source of liveness is easier to reason about; a replica only sees the caller's IP, not its node id; the fingerprint lets every node notice a ring mismatch.
Rejected: `on_contact` on every RPC — a second liveness source that can disagree with gossip.
Affects: `membership.py`, `gossip.py`, proto `Member`, INTERFACES, SPEC; D6.

## DEC-04 — Proto and RPC set  (2026-11-11, agreed by Prateek; Meet to confirm, from D0-M items 8, 9, 10, 15)
Decision: no `DeliverHint` RPC — hint delivery uses `ReplicaPut` with an empty `hint_for`. The proto stays at `proto/dynamo/dynamo.proto`. `Status` is `STATUS_UNSPECIFIED = 0, OK = 1, NOT_FOUND = 2, QUORUM_FAILED = 3, INVALID = 4`. New local RPCs `Locate` and `LocalGet`; `ClusterInfo` also reports live-key and tombstone counts and each member's state, incarnation and heartbeat (storage offers `counts()`).
Why: `DeliverHint` would do exactly what `ReplicaPut` does; the nested proto path makes the generated code import `from dynamo import dynamo_pb2`; a status that was never set must not read as OK.
Rejected: a separate `DeliverHint` RPC (this differs from Meet's D0-M doc, which still lists it as a question); `OK = 0`; proto at `proto/dynamo.proto`.
Affects: proto, SPEC, TASKS ownership table, `storage.py`; D2, D9.

## DEC-05 — Config and build  (2026-11-11, agreed by Prateek; Meet to confirm, from D0-M items 11, 12, 16)
Decision: `request_timeout_ms` (default 1500) is added to the config table. Every flag defaults from its env var (`NODE_ID`, `CONFIG`, `PORT`, `DATA_DIR`); `CONFIG` defaults to `cluster.local.yaml`; startup fails only if the node id is missing from both flag and env var, or is not in the config. `pyproject.toml` is an allowed code path.
Why: `make run` and Docker pass only env vars; `pyproject.toml` gives the `kv` command and the pytest settings.
Rejected: `--node-id` required as a flag; tests with path hacks instead of `pyproject.toml`.
Affects: `config.py`, `cluster.yaml`, SPEC config table, `.team/CLAUDE.md`, TASKS.

## DEC-06 — Storage failure and tombstones  (2026-11-11, agreed by Prateek; Meet to confirm, from D0-M items 13, 14)
Decision: if a log write or `fsync` fails, the node logs CRITICAL and exits with `os._exit(1)`, with no retry. Tombstones are never purged and there is no `last_alive` startup guard.
Why: a failed write can leave half a line, and a retried `fsync` can report success for lost data; restart is cheap. In a course demo no tombstone lives 24 h, so the purge and its guard would be code that never runs.
Rejected: retrying the write; the SPEC's 24 h purge with a `last_alive` guard (this differs from Meet's D0-M doc: its Q8 and the `last_alive` edge-case row no longer apply).
Known limitation: tombstones stay in the dict and in every snapshot forever.
Affects: `storage.py`, SPEC tombstone rule and on-disk section; D1.

## DEC-07 — Logging ownership  (2026-11-11, agreed by Prateek; Meet to confirm, from D0-M item 17)
Decision: `dynamo/reqid.py` (Meet) holds the request-id logging filter. Prateek's D0-P logging part is only the line format and the default level.
Why: one owner for the filter, no overlap.
Rejected: a second logging helper in D0-P that also adds the request id.
Affects: `reqid.py`, D0-P.

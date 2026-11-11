# INTERFACES

The function signatures both of us build against. Shared file: change it only after both agree,
and log the change in `.team/DECISIONS.md`.

Each block is marked:
- **FINAL** — agreed, build against it.
- **DRAFT** — not frozen yet; says what it is waiting for.

---

## storage.py (Prateek) — FINAL  (DEC-06, design: `.team/design/D1-storage.md`)

```python
@dataclass
class Version:
    value: bytes = b""              # empty for a tombstone
    clock: dict[str, int] = {}      # vector clock, node_id -> counter; empty until D3
    tombstone: bool = False         # True = deleted marker

class Storage:
    def __init__(self, data_dir: str, snapshot_every: int = 1000) -> None   # no disk work
    def load(self) -> None                                      # startup: snapshot + log replay
    def get(self, key: str) -> list[Version]                    # all versions, tombstones included; [] if unknown
    async def apply(self, key: str, version: Version) -> bool   # log + fsync + dict; exits the node on a log error
    def counts(self) -> tuple[int, int]                         # (live keys, tombstones) for ClusterInfo
    async def close(self) -> None                               # wait for a running write, close the log
    # added in D9: put_hint / hints / drop_hint
```

- `load()` must finish before the server binds its port. `get` and `apply` raise `RuntimeError` before it.
- `apply` returns True once the write is on disk and in the dict. If the log write or `fsync`
  fails, the node logs CRITICAL and exits with `os._exit(1)`; `apply` never returns False for that.
- `Version` lives in `dynamo/storage.py` until D3. DEC-02 adds `dynamo/version.py` (Prateek) for the
  comparison and merge rules; where the dataclass ends up is decided in the D3 design.

## membership.py (Prateek) — DRAFT until D0-P is designed  (DEC-03)

```python
class Membership:
    def state(self, node_id: str) -> MemberState               # used by Coordinator._route_order
    def members(self) -> list[Member]                          # used by ClusterInfo
    def on_gossip(self, msg: GossipMessage) -> GossipMessage   # the only source of liveness
    # removed: on_contact(sender) — no RPC other than Gossip updates liveness
```

Still open in D0-P: constructor, `address()`, `subscribe()`, the gossip loop start/stop.

## vclock.py, version.py, gossip.py, handoff.py, config.py (Prateek) — DRAFT until D0-P / D3 are designed

Starting proposal is in `.team/TASKS.md` (section D0). Fixed so far by decisions:
- The coordinator computes the new counter (DEC-02); the clock code accepts it.
- Hint delivery uses `ReplicaPut` with an empty `hint_for`; there is no `DeliverHint` RPC (DEC-04).
- Every flag defaults from its env var: `NODE_ID`, `CONFIG`, `PORT`, `DATA_DIR` (DEC-05).

## server.py, reqid.py, ring.py, peers.py, coordinator.py, replica.py, client/cli.py (Meet) — DRAFT

Meet's signatures are in section 4 of `.team/design/D0-M-contracts.md` and are his draft until
D0-M is DESIGNED. What they call in Prateek's modules:

| Meet's code | Calls |
|---|---|
| `Node.start()` | `storage.load()` before the port is bound |
| `Node.stop()` | `storage.close()` last |
| `ReplicaPut` handler | `storage.apply(key, version)`; with `hint_for` set → `storage.put_hint` (D9) |
| `ReplicaGet` / `LocalGet` handlers | `storage.get(key)` |
| `ClusterInfo` handler | `storage.counts()`, `membership.members()` |
| `Coordinator._route_order` | `membership.state(node_id)` |
| `Gossip` handler | `membership.on_gossip(msg)` |

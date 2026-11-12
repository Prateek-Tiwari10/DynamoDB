# INTERFACES

The function signatures both of us build against. Shared file: change it only after both agree,
and log the change in `.team/DECISIONS.md`.

Each block is marked:
- **FINAL** — designed by its owner, build against it. Prateek's blocks are final from
  `.team/design/D0-P-contracts.md` (DEC-08). Meet's blocks are final from `.team/design/D0-M-contracts.md` (DEC-09).
- **DRAFT** — not frozen yet; says what it is waiting for.

---

## config.py (shared, Prateek's D0-P) — FINAL

```python
@dataclass(frozen=True)
class ClusterConfig:
    nodes: dict[str, str]          # node_id -> "host:port" (what peers use to reach it)
    n: int = 3
    r: int = 2
    w: int = 2
    vnodes: int = 32
    rpc_timeout_ms: int = 500
    request_timeout_ms: int = 1500
    gossip_interval_ms: int = 1000
    suspect_after_ms: int = 3000
    dead_after_ms: int = 10000
    handoff_retry_ms: int = 5000
    vclock_max_entries: int = 10
    snapshot_every: int = 1000
    sloppy_quorum: bool = True

def load_config(path: str) -> ClusterConfig      # reads the yaml, validates, raises ValueError

LOG_FORMAT: str                                  # "HH:MM:SS.mmm LEVEL [node_id] [req=xxxxxxxx] module: message"
def setup_logging() -> None                      # stderr, level from LOG_LEVEL (default INFO)
```

- Per-node values are not in the config object: `--node-id` / `NODE_ID`, `--config` / `CONFIG`,
  `--port` / `PORT`, `--data-dir` / `DATA_DIR` (DEC-05). Data folder: `<data-dir>/<node_id>/`.
- `LOG_FORMAT` needs `node_id` and `request_id` on every log record. Meet's `RequestIdFilter`
  (`reqid.py`) sets them; `request_id` is `-` outside a request and is printed as 8 characters.

## storage.py (Prateek) — FINAL  (design: `.team/design/D1-storage.md`)

```python
@dataclass
class Version:
    value: bytes = b""              # empty for a tombstone
    clock: dict[str, int] = {}      # vector clock, node_id -> counter; empty until D3
    tombstone: bool = False         # True = deleted marker

class Storage:
    def __init__(self, data_dir: str, config: ClusterConfig) -> None   # no disk work; uses config.snapshot_every
    def load(self) -> None                                      # startup: snapshot + log replay
    def get(self, key: str) -> list[Version]                    # all versions, tombstones included; [] if unknown
    async def apply(self, key: str, version: Version) -> bool   # merge_into + log + fsync + dict; True if stored
    def counts(self) -> tuple[int, int]                         # (live keys, tombstones) for ClusterInfo
    async def close(self) -> None                               # wait for a running write, close the log
    # added in D9:
    async def put_hint(self, owner: str, key: str, version: Version) -> None
    def hints(self) -> dict[str, list[tuple[str, Version]]]
    async def drop_hint(self, owner: str, key: str, version: Version) -> None
```

- `load()` must finish before the server binds its port. `get`, `apply` and `counts` raise
  `RuntimeError` before it.
- An error on the log file (write, `fsync`, clear) → CRITICAL + `os._exit(1)`, no retry (DEC-06).
  An error on the snapshot files → logged, the node keeps running, the next write tries again.
- Until D3, `apply` always stores (newest write replaces) and returns True. The `Version` dataclass
  moves to its final shape (a `VectorClock` instead of a plain dict) in the D3 design.

## vclock.py (Prateek) — FINAL signatures, logic in D3

```python
@dataclass
class VectorClock:
    counters: dict[str, int]        # node_id -> counter
    updated_ms: dict[str, int]      # node_id -> last update time, used only to choose what to prune

class Order(Enum): BEFORE, AFTER, EQUAL, CONCURRENT

def compare(a: VectorClock, b: VectorClock) -> Order
def merge(a: VectorClock, b: VectorClock) -> VectorClock          # per node, the higher counter
def advance(clock: VectorClock, node_id: str, counter: int, now_ms: int) -> VectorClock
    # returns a copy with counters[node_id] = counter; the coordinator computes `counter` (DEC-02)
def prune(clock: VectorClock, max_entries: int) -> VectorClock    # drop entries with the oldest updated_ms
```

## version.py (Prateek) — FINAL signatures, logic in D3

```python
def reconcile(versions: list[Version]) -> list[Version]
    # drop every version that is BEFORE another; collapse versions with the same value and
    # tombstone flag into one with merged clocks; result is pairwise CONCURRENT
def merge_into(existing: list[Version], incoming: Version) -> tuple[list[Version], bool]
    # what a replica stores after receiving `incoming`; bool = whether anything changed
def context_of(versions: list[Version]) -> bytes        # merged clock of all versions, tombstones included
def parse_context(data: bytes) -> VectorClock           # empty bytes -> empty clock; bad bytes -> ValueError (server answers INVALID)
```

## membership.py, gossip.py (Prateek) — FINAL signatures, logic in D6

```python
class Membership:
    def __init__(self, self_id: str, config: ClusterConfig, peers: Peers, ring_fingerprint: str) -> None   # no I/O
    async def start(self) -> None                    # starts the gossip loop
    async def stop(self) -> None
    def state(self, node_id: str) -> MemberState     # this node's opinion; itself is always ALIVE; unknown id -> KeyError
    def is_alive(self, node_id: str) -> bool         # state == ALIVE
    def alive_nodes(self) -> list[str]
    def members(self) -> list[Member]                # full table with states, for ClusterInfo
    def address(self, node_id: str) -> str
    def on_gossip(self, msg: GossipMessage) -> GossipMessage    # merge the sender's table, reply with ours
    def subscribe(self, callback: Callable[[str, MemberState, MemberState], None]) -> None
        # called as callback(node_id, old_state, new_state) on every state change

# gossip.py
async def gossip_loop(membership: Membership) -> None           # one round every gossip_interval_ms
```

- No `on_contact(sender)`: only gossip updates liveness (DEC-03).
- `MemberState`, `Member`, `GossipMessage` are the proto messages.

## handoff.py (Prateek) — FINAL signatures, logic in D9

```python
class Handoff:
    def __init__(self, self_id: str, config: ClusterConfig, storage: Storage,
                 membership: Membership, peers: Peers) -> None    # no I/O
    async def start(self) -> None        # starts the retry loop
    async def stop(self) -> None
```

- Hint delivery uses `ReplicaPut` with an empty `hint_for`; there is no `DeliverHint` RPC (DEC-04).

## tests/conftest.py (Prateek) — FINAL signatures

```python
class Cluster:
    nodes: dict[str, Node]                       # running nodes by id
    def client(self, node_id: str) -> KVStoreStub
    async def start(self) -> None
    async def stop_node(self, node_id: str) -> None    # clean stop: node.stop()
    async def kill(self, node_id: str) -> None         # crash: no storage.close()
    async def restart(self, node_id: str) -> None      # same id, same port, same data folder
    async def stop(self) -> None

def make_cluster(tmp_path, size: int = 5) -> Cluster   # free localhost ports, short timers
@pytest_asyncio.fixture
async def cluster(tmp_path) -> Cluster                  # 5 nodes, started; stopped at teardown
```

- Uses Meet's `build_node(config, node_id, port, data_dir)`, `Node.start()`, `Node.stop()`,
  `node.server`, `node.peers`, `node.membership`, `node.handoff`.

## server.py, reqid.py, ring.py, peers.py, coordinator.py, replica.py, client/cli.py (Meet) — FINAL

Meet's signatures are in section 4 of `.team/design/D0-M-contracts.md` and are final (DEC-09, Prateek to confirm). What they call in Prateek's modules:

| Meet's code | Calls |
|---|---|
| `build_node` | `Storage(data_dir, config)`, `Membership(self_id, config, peers, ring_fingerprint)`, `Handoff(self_id, config, storage, membership, peers)` |
| `Node.start()` | `storage.load()` before the port is bound, then `membership.start()`, `handoff.start()` |
| `Node.stop()` | `membership.stop()`, `handoff.stop()`, `storage.close()` last |
| `ReplicaPut` handler | `storage.apply(key, version)`; with `hint_for` set → `storage.put_hint` (D9) |
| `ReplicaGet` / `LocalGet` handlers | `storage.get(key)` |
| `ClusterInfo` handler | `storage.counts()`, `membership.members()` |
| `Coordinator._route_order` | `membership.state(node_id)` |
| `Gossip` handler | `membership.on_gossip(msg)` |

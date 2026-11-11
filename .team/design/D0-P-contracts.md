# D0-P — Data, membership, config and test-harness contracts

Owner: Prateek · Reviewer: Meet · Status: DESIGNED · Date: 2026-11-12

> My half of D0. It fixes the function names and message shapes that Meet's code and my later
> tasks (D3, D6, D9) build against. It contains no feature logic; the logic of each part is designed
> in its own task. Everything already settled by the 17 answers to D0-M is used here as given.

## 1. Requirement

- Final signatures for `Storage`, `vclock`, `version`, `Membership`, gossip and `Handoff`.
- My half of the proto: `VectorClock`, `Version`, `Member`, `MemberState`, `GossipMessage`.
- `config.py`: one config object loaded from `cluster.yaml`, and how per-node values are set.
- The log line format and default level.
- A test fixture that runs a small cluster inside one test process, and can stop, crash and
  restart a node.

## 2. Chosen approach (plain words)

- **Config:** one frozen (read-only) dataclass built once at startup from `cluster.yaml`. Per-node
  values come from flags that default to env vars, as agreed in D0-M item 12.
- **Logs:** one line format for every module, with the node id and request id in every line. Meet's
  `reqid.py` supplies those two values; this doc only fixes how the line looks.
- **Versions and clocks:** `vclock.py` holds pure functions on clocks. `version.py` holds the rules
  for sets of versions (drop older ones, keep concurrent ones as siblings, collapse identical ones).
  Storage calls `version.py` when it applies a write.
- **Membership:** one table per node with every member's heartbeat, incarnation and state. Gossip is
  the only thing that changes it. Other modules only read it.
- **Test cluster:** nodes run in the test process on free localhost ports with short timers, so a
  test of "node dies, others notice" takes about a second instead of ten.
- **Branches:** everything on `main`, small commits, tests pass before every push.

## 3. Alternatives we rejected

| Option | Why not |
|---|---|
| Config as a plain dict or module-level globals | No type checking, typos in key names fail late, and tests cannot run two nodes with different settings in one process |
| Per-node settings inside `cluster.yaml` only | Docker and `make run` pass the node id as an env var; one shared file for all nodes is simpler |
| Request id as part of the log message text, added by hand | Easy to forget in one place; a logging filter adds it to every line automatically |
| One file for clocks and version sets | Clock functions are pure maths and easy to test alone; the sibling rules are a separate idea and a separate interview answer |
| Updating liveness from any incoming RPC (`on_contact`) | Dropped in D0-M item 7: one liveness source is simpler than two |
| Gossiping SUSPECT / DEAD states between nodes | Needs refutation rules for every state. Each node deciding from heartbeat age is simpler and gives the same result in a 5-node cluster. Final choice is made in D6 |
| Test cluster with real Docker containers | Slow to start and hard to debug. Docker is kept for the demo and the final cluster tests (D10) |
| Test cluster on fixed ports 50051–50055 | Clashes with a running local cluster and with parallel test runs |
| One branch per task | Extra merge work for two people; file ownership already prevents conflicts |

## 4. Interfaces and data structures

### 4.1 Config (`dynamo/config.py`, shared file)

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

def load_config(path: str) -> ClusterConfig      # reads the yaml, then validates
```

- Validation at load, fail with a clear message: at least one node; `1 <= r <= n`, `1 <= w <= n`;
  `n <= number of nodes`; `suspect_after_ms < dead_after_ms`; every address is `host:port`.
- Per-node values are not in this object. They come from `--node-id` / `NODE_ID`,
  `--config` / `CONFIG`, `--port` / `PORT`, `--data-dir` / `DATA_DIR` (D0-M item 12). The node's
  data folder is `<data-dir>/<node_id>/`.

### 4.2 Log format (constant in `dynamo/config.py`)

```
HH:MM:SS.mmm LEVEL [node_id] [req=xxxxxxxx] module: message
20:15:03.412 INFO [node1] [req=a3f9c2d1] coordinator: put cart:42 acks 2/2
20:15:04.001 INFO [node3] [req=-] gossip: node2 ALIVE -> SUSPECT
```

- `node_id` and `request_id` are added by Meet's `RequestIdFilter`. The request id is shown as its
  first 8 characters; lines outside a request show `-`.
- Default level INFO; `LOG_LEVEL` env var overrides it. Logs go to stderr.

### 4.3 Proto, my half (`proto/dynamo/dynamo.proto`)

```protobuf
message VectorClock {
  map<string, uint64> counters = 1;    // node_id -> counter
  map<string, int64> updated_ms = 2;   // node_id -> last update time, used only to choose what to prune
}
message Version {
  bytes value = 1;                     // empty for a tombstone
  VectorClock clock = 2;
  bool tombstone = 3;
}
enum MemberState { MEMBER_STATE_UNSPECIFIED = 0; ALIVE = 1; SUSPECT = 2; DEAD = 3; }
message Member {
  string node_id = 1;
  string address = 2;
  uint64 heartbeat = 3;                // raised by the owner every gossip tick
  MemberState state = 4;               // this node's own opinion; filled for ClusterInfo
  uint64 incarnation = 5;              // owner's boot time in ms
  string ring_fingerprint = 6;         // D0-M item 5
}
message GossipMessage {
  string sender = 1;
  repeated Member members = 2;
}
```

- `ReplicaPutRequest.hint_for` stays as in the SPEC. `DeliverHint` is removed (D0-M item 8).
- `MemberState` gets an unset value 0, for the same reason as `Status` (D0-M item 10).
- The client `context` is a serialized `VectorClock`.

### 4.4 Clocks (`dynamo/vclock.py`) — pure functions, no I/O

```python
class Order(Enum): BEFORE, AFTER, EQUAL, CONCURRENT

def compare(a: VectorClock, b: VectorClock) -> Order
def merge(a: VectorClock, b: VectorClock) -> VectorClock          # per node, the higher counter
def advance(clock: VectorClock, node_id: str, counter: int, now_ms: int) -> VectorClock
    # returns a copy with counters[node_id] = counter; the coordinator computes `counter` (D0-M item 2)
def prune(clock: VectorClock, max_entries: int) -> VectorClock    # drop entries with the oldest updated_ms
```

### 4.5 Version sets (`dynamo/version.py`)

```python
def reconcile(versions: list[Version]) -> list[Version]
    # drop every version that is BEFORE another; collapse versions with the same value and
    # tombstone flag into one with merged clocks; result is pairwise CONCURRENT
def merge_into(existing: list[Version], incoming: Version) -> tuple[list[Version], bool]
    # what a replica stores after receiving `incoming`; bool = whether anything changed
def context_of(versions: list[Version]) -> bytes        # merged clock of all versions, tombstones included
def parse_context(data: bytes) -> VectorClock           # empty bytes -> empty clock; bad bytes -> ValueError (server answers INVALID)
```

### 4.6 Storage (`dynamo/storage.py`) — designed in D1

```python
class Storage:
    def __init__(self, data_dir: str, config: ClusterConfig) -> None    # no I/O
    def load(self) -> None                                      # startup: snapshot + log replay
    def get(self, key: str) -> list[Version]                    # all versions, tombstones included
    async def apply(self, key: str, version: Version) -> bool   # merge_into + log + fsync + dict; True if stored
    def counts(self) -> tuple[int, int]                         # (live keys, tombstones) for ClusterInfo
    async def close(self) -> None
    # added in D9:
    async def put_hint(self, owner: str, key: str, version: Version) -> None
    def hints(self) -> dict[str, list[tuple[str, Version]]]
    async def drop_hint(self, owner: str, key: str, version: Version) -> None
```

### 4.7 Membership and gossip (`dynamo/membership.py`, `dynamo/gossip.py`) — logic designed in D6

```python
class Membership:
    def __init__(self, self_id: str, config: ClusterConfig, peers: Peers, ring_fingerprint: str) -> None   # no I/O
    async def start(self) -> None                    # starts the gossip loop
    async def stop(self) -> None
    def state(self, node_id: str) -> MemberState     # this node's opinion; itself is always ALIVE
    def is_alive(self, node_id: str) -> bool         # state == ALIVE
    def alive_nodes(self) -> list[str]
    def members(self) -> list[Member]                # full table with states, for ClusterInfo
    def address(self, node_id: str) -> str
    def on_gossip(self, msg: GossipMessage) -> GossipMessage    # merge the sender's table, reply with ours
    def subscribe(self, callback: Callable[[str, MemberState, MemberState], None]) -> None
        # called as callback(node_id, old_state, new_state) on every state change
```

- All peers from `cluster.yaml` start ALIVE, and the timers start at boot (D0-M item 4).
- `gossip.py` holds the loop: every `gossip_interval_ms`, raise own heartbeat, pick one random
  peer, send the table, merge the reply, then re-check every member's state.

### 4.8 Handoff (`dynamo/handoff.py`) — logic designed in D9

```python
class Handoff:
    def __init__(self, self_id: str, config: ClusterConfig, storage: Storage,
                 membership: Membership, peers: Peers) -> None    # no I/O
    async def start(self) -> None        # starts the retry loop
    async def stop(self) -> None
```

### 4.9 Test cluster (`tests/conftest.py`)

```python
class Cluster:
    nodes: dict[str, Node]                       # running nodes by id
    def client(self, node_id: str) -> KVStoreStub
    async def start(self) -> None                # start every node
    async def stop_node(self, node_id: str) -> None    # clean stop: node.stop()
    async def kill(self, node_id: str) -> None         # crash (see section 5)
    async def restart(self, node_id: str) -> None      # same id, same port, same data folder
    async def stop(self) -> None                 # stop everything, used by the fixture teardown

@pytest.fixture
async def cluster(tmp_path) -> Cluster           # 5 nodes by default; `make_cluster(k)` for other sizes
```

- Test timers: gossip 100 ms, suspect 300 ms, dead 1000 ms, rpc timeout 200 ms, handoff retry 300 ms.

## 5. Flow

**Node startup (what my parts do inside Meet's `Node.start`)**
1. `load_config` reads and validates `cluster.yaml`.
2. `build_node` creates Storage, Membership and Handoff. No files, no network.
3. `storage.load()` rebuilds the dict (D1).
4. Meet's server opens its port.
5. `membership.start()` begins gossip; `handoff.start()` begins the retry loop.

**Test cluster**
1. The fixture picks k free ports on localhost and builds one `ClusterConfig` with them and the
   short timers. Each node gets its own folder under pytest's `tmp_path`.
2. `start()` builds and starts every node in the same event loop.
3. `kill(id)`: stop the node's server with no grace period, cancel its gossip and handoff loops,
   close its peer channels, and drop the Node object **without** calling `storage.close()`.
4. `restart(id)`: build a new Node with the same id, port and data folder, and start it. It gets a
   new incarnation because that is its boot time.
5. Teardown stops every node and closes every channel so no test leaks tasks.

## 6. Edge cases

| Case | What happens | Test |
|---|---|---|
| `cluster.yaml` has `w` or `r` larger than `n`, or `n` larger than the node count | `load_config` fails with a clear message; the node does not start | `test_config_validation` |
| `NODE_ID` not in the config | Startup fails (D0-M item 12) | Meet's `test_startup_without_node_id` |
| Code tries to change a config value at runtime | Error: the dataclass is frozen | `test_config_frozen` |
| Log line written outside any request (gossip, handoff, startup) | Shows `[req=-]` | `test_log_format` |
| Client sends a `context` that is not a valid clock | `parse_context` raises `ValueError`; the server answers INVALID | `test_parse_context_bad_bytes` |
| Empty `context` | Treated as an empty clock (first write) | `test_parse_context_empty` |
| `state()` asked about the node itself | Always ALIVE | `test_self_always_alive` |
| `state()` asked about an id not in the config | `KeyError`: a bug in the caller, not a runtime case | `test_unknown_member` |
| In-process `kill` is not a real `kill -9` | Python may still flush buffers, so torn-write behaviour is not tested here. Real crashes are covered by the subprocess tests in D1 and by `docker kill` in D10 | — |
| Restarted node comes back on the same port | Peers' channels reconnect by themselves (Meet sets a 1 s reconnect backoff) | `test_restart_same_port` |
| Two test clusters in one test run | Each picks its own free ports and its own temp folders | `test_two_clusters` |
| A test forgets to stop a node | The fixture teardown stops all nodes | fixture |

## 7. Tests

- Unit: `test_config.py` (load, validation, frozen), `test_log_format.py`, and the `parse_context`
  cases in `test_version.py`.
- In-process cluster: 5 nodes start and stop cleanly; `kill` then `restart` brings a node back with
  its data; two clusters can run in one session.
- Docker: none in D0.

## 8. Interview questions on this part

1. Q: Why is the config object frozen?
   A: Every module shares it. If one module could change N or a timeout at runtime, nodes would
   silently disagree. Read-only config makes that impossible.
2. Q: Why split clock functions from version-set rules?
   A: Comparing two clocks is pure maths. Deciding which versions to keep is a policy built on top
   of it. Separating them makes each easy to test and explain.
3. Q: How do you test failure detection without waiting 10 seconds per test?
   A: All timers come from the config, so tests use the same code with 100 ms gossip and a 1 s
   dead timeout.
4. Q: Is your in-process "kill" a real crash test?
   A: No. It stops the node without a clean shutdown, but the process stays alive. Real crashes
   are tested by killing a subprocess with SIGKILL in the storage tests, and with `docker kill`.
5. Q: Why does every node start by assuming all peers are alive?
   A: The member list is fixed in the config, so there is nothing to discover. Starting ALIVE
   lets the cluster serve at once; a peer that never answers goes SUSPECT after 3 s and DEAD
   after 10 s.

## 9. As built

_Filled after implementation: what changed from this design and why._

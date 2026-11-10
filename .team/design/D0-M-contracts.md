# D0-M — Request path, server wiring & build contracts

Owner: Meet · Reviewer: Prateek · Status: DESIGNING (draft) · Date: 2026-11-10

> Draft. The list under "Needs Prateek" changes the SPEC, a shared file, or a file Prateek owns;
> each needs his agreement and a `DEC-NN` entry before this doc is DESIGNED.

### Needs Prateek (agreement + DEC entry)
1. Vector clocks as in the SPEC (Meet now follows the SPEC); `version.py` added to the file list, owned by Prateek; replicas merge incoming versions; identical siblings collapse.
2. Coordinator counter rule (Q10): `max(context[self], highest own counter among local versions) + 1` under a per-key lock; known limitation listed in 6.1 (dotted version vectors would fix it). Touches his `vclock.py` / D3.
3. Get returns a context even when the answer is NOT_FOUND (Q11); extends the SPEC's Get rule.
4. Gossip states as in the SPEC (ALIVE, SUSPECT 3 s, DEAD 10 s); `incarnation` = boot time in ms; all peers start ALIVE with the timer starting at boot (his D6 / `membership.py` / `gossip.py`).
5. Each gossip entry carries the node's `ring_fingerprint` (his half of the proto).
6. `Membership` needs `state(node_id) -> MemberState` and `members() -> list[Member]` (used by routing and `ClusterInfo`).
7. Drop `membership.on_contact(sender)`: a replica cannot know the sender's node ID (`context.peer()` gives an IP), and it would be a second liveness source fighting gossip. Conflicts with the SPEC line "every call carries the sender's ID so receivers can update membership" and with the D0 proposal.
8. `DeliverHint` does exactly what `ReplicaPut` with an empty `hint_for` does: does it need its own RPC?
9. Proto stays at `proto/dynamo/dynamo.proto`: update the SPEC layout and the TASKS ownership table.
10. `Status` gets `STATUS_UNSPECIFIED = 0`, so an unset status never reads as success (SPEC change).
11. `request_timeout_ms` (default 1500) added to the SPEC config table.
12. Settings rule: every flag defaults from its env var (`NODE_ID`, `CONFIG`, `PORT`, `DATA_DIR`); `CONFIG` default `cluster.local.yaml`; startup fails only if node id is missing from both (his `config.py` / `cluster.yaml`).
13. WAL write or fsync failure → CRITICAL + `os._exit(1)` (his storage, D1).
14. Tombstone purge after 24 h at snapshot time + `last_alive` guard at startup (his storage, D1).
15. New local RPCs `Locate` and `LocalGet` next to `ClusterInfo`; `ClusterInfo` reports tombstone counts and member state/incarnation/heartbeat.
16. `pyproject.toml` added to the allowed code paths (one-line update in `CLAUDE.md` and `TASKS.md`).
17. `reqid.py` holds the request-ID logging filter: check it does not overlap his D0-P "logging format helper".

## 1. Requirement
Freeze the request-path contracts so both halves can be built in parallel:
- Repo layout as in `.team/SPEC.md`; `requirements.txt`; `Makefile` with `proto`, `test`,
  `run NODE=node1`, `up`, `down`; `Dockerfile` / `docker-compose.yml` skeleton.
- Proto sections: `KVStore` (Put/Get/Delete/ClusterInfo + messages, `Status`), `Forward`,
  `ReplicaPut`, `ReplicaGet`; `make proto` compiles it.
- Stubs for `server.py`, `replica.py`, `peers.py`, `ring.py`, `coordinator.py`, `client/cli.py`;
  `server.py` and `replica.py` already wire every RPC to the module functions.
- A `Node` object holding `config, storage, ring, membership, coordinator, handoff`, passed to
  handlers (no globals).

## 2. Chosen approach (plain words)
Every node builds the same consistent hash ring from `cluster.yaml`, so any node can compute a key's
3 replicas without asking anyone. Writes (Put, Delete) are coordinated by the first reachable
replica, which keeps vector clocks small; reads (Get) are coordinated by any replica that receives
them. A non-replica forwards to the replicas in liveness order, and a forwarded request is never
forwarded again. Versions follow the SPEC: vector clocks, dominated versions dropped, concurrent ones
kept as siblings; the coordinator picks its clock counter under a per-key lock, and a Get always
returns a context, even for a deleted key. Expected outcomes travel in the response's `Status`; a
gRPC error only means a node could not be reached or broke. Read repair reuses `ReplicaPut` (built
in D7). Liveness comes only from push-pull gossip with one random peer per second. A request ID is
created at the client entry point, carried in gRPC metadata and a `ContextVar`, used only for logs,
and never persisted. Each node finds its own entry in `cluster.yaml` from `NODE_ID`.

### Decisions (as agreed)

**Q1. Which node coordinates a request, and how do we stop forwarding loops?**
- Put and Delete are coordinated by the first reachable node in the preference list. If the receiving node is the first replica, it coordinates. Otherwise it forwards to the first replica, even if the receiving node is replica 2 or 3. On UNAVAILABLE or DEADLINE_EXCEEDED it moves to the next replica in the list. If that next replica is the receiving node itself, it coordinates locally. If every attempt fails, it returns QUORUM_FAILED. Clock entries come only from coordinators, so coordinating writes at the first replica keeps vector clocks small. That is what Dynamo did.
- Get is coordinated by any node in the preference list. If the receiving node is a replica, it coordinates itself. Otherwise it forwards to the first replica, with the same fallback. Reads never create clock entries, and read repair re-sends existing versions, so skipping the extra hop is safe.
- "First reachable" means: try the preference-list nodes that gossip marks ALIVE first, in list order, then SUSPECT, then DEAD last. A wrong gossip view then costs only a timeout, never a failed request.
- A Forward is always coordinated by its receiver and never forwarded again.
- There is no hop count or forwarded flag in the proto. Receiving a Forward RPC is itself the signal not to forward again.
- Client-side routing, where the CLI sends requests straight to the first replica, is out of scope for Phase 0.

**Q1a. How is the first replica found?**
- A consistent hash ring is built locally on every node from `cluster.yaml`. There is no partition table and no metadata service.
- **Hash:** `hash32(x) = int(md5(x).hexdigest(), 16) % 2**32`, as in the SPEC. Any fixed 4 bytes distribute keys equally well; all that matters is that every node uses the same function. Python's `hash()` is never used.
- **Ring:** 32 vnodes per node at `hash32(f"{node_id}#{i}")` for i = 0..31, stored as (position, node_id) pairs sorted by (position, node_id).
- **Lookup:** compute `hash32(key)`, `bisect_left` into the ring, wrap to 0 at the end, then walk clockwise skipping nodes already picked until there are N=3 distinct nodes. That list is the preference list.
- `ring.py` is a pure module with no I/O.
- **Ring fingerprint:** the first 16 hex characters of SHA-256 over the sorted entries plus N. `Ring.fingerprint(n)` takes N because the ring itself does not know it. Each node’s ring fingerprint is carried in its entry of the gossip table. A node that sees a fingerprint different from its own logs it at ERROR and keeps running.

**Q2. Versions and read repair**
- Versions follow the SPEC: vector clocks. The coordinator drops dominated versions and keeps concurrent ones as siblings. Tombstones are versions like any other.
- A replica merges an incoming version with what it holds (drop dominated, keep concurrent) rather than doing "apply if newer".
- Siblings with an identical value and tombstone flag collapse into one, with their clocks merged. (A Forward retry after a partial first attempt can otherwise create two siblings with the same value.)
- `version.py` holds the comparison and merge rules and is owned by Prateek.
- Read repair reuses ReplicaPut, one call per sibling. There is no new RPC and no proto change. The merge keeps only versions that nothing else dominates, so arrival order and duplicates don't matter.
- It pushes the reconciled sibling set, exactly as stored, to every replica whose reply was missing versions or "not found", including the third replica whose reply arrives after the client got its answer.
- Repair runs in the background after the client reply. It is best-effort: failures are logged and never retried.
- Read repair is designed here and **implemented in D7**; D0 keeps only the stubs.
- Sloppy quorum (D8): the coordinator uses `ring.walk` to pick a substitute node and sets `hint_for`; the receiving replica stores the hint itself, so the coordinator does not need `handoff`.

**Q3. Where does the proto file live?**
- The proto is at `proto/dynamo/dynamo.proto`, and the generated files land in `dynamo/`.
- Generate with `python -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. proto/dynamo/dynamo.proto`.
- The generated code imports `from dynamo import dynamo_pb2`.
- Reason to keep it over the SPEC's `proto/dynamo.proto`: with that path, the generated grpc file imports `dynamo_pb2` as a top-level module and fails inside the package. The SPEC layout and the ownership table are updated to match.

**Q4. What is the error convention?**
- Expected outcomes go in the response body's Status enum, and gRPC itself returns OK. The enum is `STATUS_UNSPECIFIED = 0`, `OK`, `NOT_FOUND`, `QUORUM_FAILED`, `INVALID`: 0 is never a real answer, so a response whose status was never set cannot read as success.
- A gRPC error code means only that the node couldn't be reached or something broke.
- The coordinator counts any gRPC error from a replica, of any code, as "no ack". A replica's "not found" is a normal reply and counts toward R.
- Forward retries only on UNAVAILABLE or DEADLINE_EXCEEDED. Body statuses are passed back unchanged.
- **Time budget:** each node's budget is `min(request_timeout_ms, time left on the incoming gRPC deadline)`. If the client set no deadline (`context.time_remaining()` is None), the budget is `request_timeout_ms`. A forwarded call gets only what remains, and each replica call uses `rpc_timeout_ms`. `request_timeout_ms` is added to the SPEC config table with a default of 1500.
- One decorator wraps every handler of both servicers: it sets the request ID, turns `NotImplementedError` into `abort(UNIMPLEMENTED)` and any other `Exception` into `abort(INTERNAL)`. It catches `Exception`, never `BaseException`, so cancellations pass through. A replica with an internal error therefore never acks.
- D0 stubs raise `NotImplementedError`, so every not-yet-built RPC answers UNIMPLEMENTED.
- If a WAL write or fsync fails, the node logs CRITICAL and exits immediately with `os._exit(1)`, with no graceful shutdown.
- **CLI exit codes:** OK 0, NOT_FOUND 1, INVALID 2, any gRPC error 3, QUORUM_FAILED 4.
- STATUS_UNSPECIFIED or an unknown status: exit 3. Proto3 enums pass unknown numbers straight through, so a newer server could send one. The CLI prints a message different from the unreachable case, because the node was reached but sent a broken reply: `ERROR: bad reply from HOST:PORT (status 0), request-id <id>`. Exit 3 already means "unreachable or broken", since every gRPC error maps to it. The request ID points straight to the server logs for what is always a server bug.

**Q5. In what order is the Node built, started and stopped?**
- **Build (`build_node`):** config, storage, ring, peers, membership, coordinator, handoff, server. Constructors only wire objects together and do no I/O: nothing is bound and no file is created.
- **Start (`async start()`):** recover storage, then `add_insecure_port` (this binds the port, so it happens only after recovery) and start the gRPC server, storing the bound port (tests use port 0), then start the membership loop, then the handoff loop.
- **Stop (`async stop()`):**
  1. `server.stop(grace=2s)`.
  2. Cancel the membership and handoff loops.
  3. Await the coordinator's background task set for up to 2s, then cancel what's left.
  4. Close the peer channels.
  5. Close storage.
- `serve()` installs SIGTERM and SIGINT handlers that call `node.stop()`. Python as PID 1 ignores SIGTERM without a handler, so Docker would otherwise SIGKILL every node after 10 s.
- Crash safety comes only from the WAL; correctness never depends on stop order.

**Q5a. How do nodes know a peer is alive?**
- Membership comes only from `cluster.yaml`.
- Liveness comes only from push-pull gossip of heartbeat tables, one random peer per second. No other RPC updates liveness (`on_contact` is dropped).
- States follow the SPEC: ALIVE, SUSPECT after 3 s without heartbeat progress, DEAD after 10 s.
- `incarnation` is the node's boot time in milliseconds.
- All peers start as ALIVE, and the down timer starts at boot, so a peer never heard from goes SUSPECT after 3 s.
- Each node's entry in the gossip table carries its own `ring_fingerprint`, so every node eventually sees every other node's fingerprint, even nodes it never talks to directly.
- Peer channels set `max_reconnect_backoff_ms = 1000`.
- Gossip message fields, merge rule and restart behaviour belong to Prateek's D0-P.

**Q6. Where is the request ID created, and how does it travel?**
- **Created** only at the KVStore Put, Get and Delete handlers. The client's `x-request-id` metadata is used if present; otherwise the node generates `uuid.uuid4().hex`.
- **Between nodes**, it is carried in the gRPC metadata key `x-request-id` on Forward, ReplicaPut and ReplicaGet. It is never a proto field.
- **Inside a node**, it lives in a `ContextVar` that background tasks inherit automatically. A logging filter adds `request_id` and `node_id` to every log line.
- **Back to the client**, it is returned in trailing metadata `x-request-id` on every path, errors included, and `kv -v` prints it.
- The request ID is never persisted anywhere: not in storage, the WAL, Version or hints.
- Gossip rounds and handoff deliveries are logged with the labels `gossip` and `handoff`.
- It is used for tracing only, never for correctness.

**Q7. How does a node find its own entry in `cluster.yaml`?**
- Every flag defaults from its env var: `--node-id` / `NODE_ID`, `--config` / `CONFIG`, `--port` / `PORT`, `--data-dir` / `DATA_DIR`. Startup fails if the node id is missing from both, or is not in the config.
- `CONFIG` defaults to `cluster.local.yaml`; Docker sets `cluster.yaml`.
- A config address is only what peers use to reach the node.
- The node binds `0.0.0.0` on the port from its own entry; `--port` / `PORT` overrides it.
- The data folder is `<data-dir>/<node_id>/`, with `--data-dir` defaulting to `./data`.
- `make run NODE=nodeX` errors out if `NODE` is empty, and runs with `cluster.local.yaml`.
- `make up` runs `docker compose`, which sets `NODE_ID` per service using a YAML anchor. Inside the containers every node listens on 50051, mapped to host ports 50051–50055.
- **Startup log line:** `nodeX: advertised <addr>, listening on 0.0.0.0:<port>, data <dir>, ring <fingerprint>`.

**Q8. Tombstone purge**
- Keep the SPEC's 24 h purge, applied when a snapshot is written.
- Guard (as in Cassandra): a node that was down longer than 24 h could bring deleted keys back to life. Each node writes a `last_alive` timestamp every minute, and at startup it refuses to run if that is more than 24 h old, until its data folder is wiped.

**Q9. Local-only calls: ClusterInfo, Locate, LocalGet**
- `ClusterInfo` is answered locally and never forwarded. It returns:
  - node_id, N, R, W, vnodes and the ring fingerprint
  - every member's address, state, incarnation and heartbeat
  - local counts of live keys and of tombstones
- `Locate` and `LocalGet` are local in the same way.

**Q10. How does the coordinator choose its clock counter?**
- The new counter is `max(context[self], highest own counter among its local versions of the key) + 1`, computed under a per-key lock.
- Without this, two clients writing with the same context through the same coordinator get identical clocks for different values.
- Known limitation: see 6.1.

**Q11. Delete keeps its context**
- Get returns the non-tombstone siblings.
- The context is the merge of all sibling clocks, tombstones included.
- If every sibling is a tombstone, the result is NOT_FOUND, still with that context, and the CLI saves it.
- Without this, a put after a delete comes out concurrent with the tombstone.

**Q12. Build, layout and CLI**
- `Node`, `build_node()` and `main()` live in `server.py`. A new module `dynamo/reqid.py` (Meet) holds the request-ID `ContextVar`, the handler decorator and the logging filter, so the other modules do not import each other.
- Helper scripts live in `tools/`.
- `pyproject.toml` is added: it gives the `kv` command the README and demo use, makes `dynamo` and `client` importable in tests without path hacks, and holds the pytest settings such as `asyncio_mode`. The allowed code paths get a one-line update.
- `requirements.txt`: `grpcio>=1.60`, `grpcio-tools>=1.60`, `PyYAML>=6.0`, `pytest>=8.0`, `pytest-asyncio>=0.23`.
- `Makefile`: `proto` runs the Q3 command; `test` runs `$(MAKE) proto && pytest -q`; `run` fails with `usage: make run NODE=node1` if `NODE` is empty, else runs `NODE_ID=$(NODE) CONFIG=cluster.local.yaml python -m dynamo.server`; `up` = `docker compose up -d --build`; `down` = `docker compose down`.
- `Dockerfile`: `python:3.11-slim`; install requirements; copy the project; run the protoc command; `ENV CONFIG=cluster.yaml DATA_DIR=/data PYTHONUNBUFFERED=1` (without `DATA_DIR` the data lands in `/app/data`, outside the mount, and is lost with the container; `PYTHONUNBUFFERED` makes `docker compose logs` prompt); `CMD ["python", "-m", "dynamo.server"]`.
- `docker-compose.yml`: an `x-node` anchor shared by node1..node5, each setting its own `NODE_ID`; ports `5005X:50051`; one shared `./data:/data` mount, each node writing only `/data/<node_id>/`.
- **CLI:**
  - `-v`, `--node`, `--timeout` and `--context-file` live on a parent parser shared by every subcommand, so `put cart:42 book -v` parses.
  - `--node` defaults to `$KV_NODE` or `localhost:50051`; `--timeout` to 2.0 s.
  - Context file: `--context-file` or `$KV_CONTEXT`, default `.kv_context.json` in the current folder, keyed by key only, values base64. Written atomically (temp file, then `os.replace`). Separate files keep the local and Docker clusters from sharing contexts.
  - `get` saves the returned context (also on NOT_FOUND); `put` and `delete` send the saved context and save the new one. A `put` with no saved context sends an empty one, which per the SPEC can create a sibling.
  - Values go to stdout; with 2+ siblings each is printed as `[i] value`. The sibling hint (`N siblings: merge them and put again`) and the `-v` details (`written_to` / `read_from`, request ID) go to stderr, so scripts can parse stdout.

## 3. Alternatives we rejected

| Option | Why not |
|---|---|
| Any replica coordinates writes | Clock entries come only from coordinators; first-replica writes keep vector clocks small |
| Always forward Get to the first replica | Reads never create clock entries and read repair re-sends existing versions, so the extra hop buys nothing |
| Forward in plain list order, ignoring gossip | A replica known to be down would cost a timeout on every write; ALIVE → SUSPECT → DEAD order makes a wrong view cost at most one timeout |
| Hop count or "forwarded" flag in the proto | Receiving a Forward RPC is itself the signal not to forward again |
| Client-side routing (CLI sends straight to the first replica) | Out of scope for Phase 0 |
| Partition table / metadata service | Every node builds the same ring from `cluster.yaml`, so no extra service is needed |
| Python's built-in `hash()` | Randomised per process, so nodes would disagree on placement |
| A new RPC for read repair | `ReplicaPut` already delivers versions, and the replica merges them |
| `repeated Version` in `ReplicaPutRequest` for repair | Not needed: one call per sibling gives the same result, because merge ignores order and duplicates |
| Request ID as a proto field, or persisted anywhere | Tracing only, never needed for correctness |
| Forward retry on any gRPC error or body status | Only UNAVAILABLE and DEADLINE_EXCEEDED mean the target never answered; every other outcome is a real answer (see 3.1) |
| All-to-all Ping for liveness | About n² messages per second and no path to dynamic membership; gossip costs about n and reaches every node in about log n rounds (see 3.2) |
| `on_contact(sender)` on every RPC | The replica only sees an IP, not a node ID, and it would be a second liveness source fighting gossip |
| (timestamp_ms, writer) last-write-wins | Silently loses concurrent writes and lets clock skew pick an older write; the SPEC's API already has room for siblings (see 3.3) |
| Counter = `context[self] + 1` only | Two writes with the same context through one coordinator would get identical clocks for different values |
| Get on a deleted key returns no context | A later put would be concurrent with the tombstone instead of replacing it |
| `OK = 0` in `Status` | A response whose status was never set would read as success |
| Proto at `proto/dynamo.proto` (SPEC) | The generated grpc file would import `dynamo_pb2` as a top-level module and fail inside the package |
| Bind the port in `build_node` | Breaks "no I/O in build" and opens the port before recovery |
| `--node-id` required as a flag | `make run` and Docker pass only the `NODE_ID` env var |
| No `pyproject.toml`, `python -m client.cli` only | No `kv` command for the README and demo; tests would need path hacks |
| Purge tombstones with no startup guard | A node down for more than 24 h could bring deleted keys back to life |

### 3.1 Why Forward retries only on UNAVAILABLE or DEADLINE_EXCEEDED

Decision: the entry node moves to the next replica only when the call fails with UNAVAILABLE or DEADLINE_EXCEEDED. Every other result is passed back to the client unchanged.

Reason: these two codes are the only ones meaning the target never gave an answer. It either couldn't be reached or didn't reply in time. Every other outcome is a real answer:

- A body status like `QUORUM_FAILED` or `NOT_FOUND` is the coordinator's verdict. Asking another replica would repeat the work and hide the real result.
- `INVALID` would fail identically on every node.
- `INTERNAL` means a bug, and retrying elsewhere would likely hit the same bug and hide it.

Retrying after a timeout is safe even if the first attempt already wrote: with vector clocks the retry can at worst create a second sibling with the same value, and siblings with an identical value and tombstone flag collapse into one with their clocks merged.

### 3.2 Why gossip instead of all-to-all Ping

Decision: liveness comes from push-pull gossip of heartbeat tables, one random peer per second, with the SPEC's states: SUSPECT after 3 s and DEAD after 10 s without heartbeat progress.

Reason: the system is meant to scale. All-to-all pinging costs about n² messages per second, which is fine for 5 nodes but not for hundreds. Gossip costs about n messages per second in total, and news reaches every node in roughly log n rounds.

Gossip is also the mechanism dynamic membership needs later. Ring changes and node joins ride on the same tables, so building it now avoids a rewrite. The accepted cost is that a failure is noticed later than with direct pings, because news travels second-hand.

### 3.3 Why vector clocks (SPEC) instead of (timestamp_ms, writer)

Decision: follow the SPEC. Drop dominated versions and keep concurrent ones as siblings.

Reason: (timestamp_ms, writer) was argued for because the API seemed to have nowhere to put siblings. The SPEC already has a `context` field in Put and Get and a context file in the CLI, so that reason is gone. Last-write-wins would mean that concurrent writes silently lose one, and clock skew can let an older write win. Prateek's D3 task stays.

## 4. Interfaces and data structures

```python
# dynamo/server.py  (Meet)
@dataclass
class Node:
    config: ClusterConfig            # D0-P
    storage: Storage                 # D0-P
    ring: Ring
    peers: Peers
    membership: Membership           # D0-P
    coordinator: Coordinator
    handoff: Handoff                 # D0-P
    server: grpc.aio.Server
    port: int = 0                    # set by start() from add_insecure_port

    async def start(self) -> None    # recover storage → add_insecure_port + server.start → membership loop → handoff loop
    async def stop(self) -> None     # server.stop(2) → cancel loops → coordinator.drain(2) → peers.close() → storage.close()

def build_node(config: ClusterConfig, node_id: str, port: int, data_dir: str) -> Node   # wiring only: binds nothing, creates no files

class KVStoreServicer(dynamo_pb2_grpc.KVStoreServicer):
    def __init__(self, node: Node) -> None
    async def Put(self, request: PutRequest, context) -> PutResponse
    async def Get(self, request: GetRequest, context) -> GetResponse
    async def Delete(self, request: DeleteRequest, context) -> DeleteResponse
    async def ClusterInfo(self, request: Empty, context) -> ClusterInfoResponse      # local only
    async def Locate(self, request: LocateRequest, context) -> LocateResponse        # local only
    async def LocalGet(self, request: LocalGetRequest, context) -> LocalGetResponse  # local only

def request_budget(context, config: ClusterConfig) -> float   # min(request_timeout_ms, time_remaining); time_remaining None → request_timeout_ms; seconds
def validate(key: str, value: bytes | None, quorum: int, n: int) -> bool   # key 1–256 B, value ≤ 1 MiB, 0 ≤ quorum ≤ N
def parse_args(argv: list[str] | None = None) -> argparse.Namespace       # --node-id/NODE_ID --config/CONFIG --port/PORT --data-dir/DATA_DIR
async def serve(args: argparse.Namespace) -> None                         # build, start, SIGTERM/SIGINT → node.stop()
def main() -> None
```

```python
# dynamo/reqid.py  (Meet, new)
REQUEST_ID: ContextVar[str]                                   # default "-"
def start_request(metadata) -> str                            # client entry: x-request-id or uuid4().hex
def adopt(metadata) -> None                                   # node-to-node entry: take it from metadata
def outgoing() -> tuple[tuple[str, str], ...]                 # metadata to attach to outgoing calls
def rpc_handler(kind: Literal["client", "node"])              # decorator for both servicers:
    # client → start_request + trailing x-request-id on every path; node → adopt
    # NotImplementedError → abort(UNIMPLEMENTED); other Exception → abort(INTERNAL); BaseException passes through
class RequestIdFilter(logging.Filter)                         # adds request_id and node_id to every record
```

```python
# dynamo/ring.py  (Meet) — pure, no I/O
def hash32(text: str) -> int                                  # int(md5(text).hexdigest(), 16) % 2**32
class Ring:
    def __init__(self, node_ids: list[str], vnodes: int) -> None
    def position(self, key: str) -> int                       # for Locate
    def preference_list(self, key: str, n: int) -> list[str]
    def walk(self, key: str) -> Iterator[str]
    def fingerprint(self, n: int) -> str                      # 16 hex of SHA-256(sorted entries + n)
```

```python
# dynamo/peers.py  (Meet)
class Peers:
    def __init__(self, config: ClusterConfig, self_id: str) -> None   # no I/O; channels created lazily
    def stub(self, node_id: str) -> NodeServiceStub                    # cached grpc.aio channel, max_reconnect_backoff_ms=1000
    async def close(self) -> None
```

```python
# dynamo/coordinator.py  (Meet)
class Coordinator:
    def __init__(self, node_id: str, config: ClusterConfig, ring: Ring, storage: Storage,
                 peers: Peers, membership: Membership) -> None          # no handoff (Q2, D8)

    # client entry: route (coordinate here or Forward)
    async def put(self, req: PutRequest, budget: float) -> PutResponse
    async def get(self, req: GetRequest, budget: float) -> GetResponse
    async def delete(self, req: DeleteRequest, budget: float) -> DeleteResponse

    # Forward entry: always coordinates here, never forwards
    async def coordinate(self, req: ForwardRequest, budget: float) -> ForwardResponse

    async def drain(self, timeout: float) -> None             # await background tasks, then cancel the rest

    # shared core, used by both put/get/delete and coordinate
    async def _do_put(self, req: PutRequest, budget: float) -> PutResponse
        # per-key lock → new clock (Q10) → local write (1 ack) → ReplicaPut fan-out → wait W
    async def _do_get(self, req: GetRequest, budget: float) -> GetResponse
        # local read + ReplicaGet fan-out → wait R → reconcile → values + context (Q11) → schedule repair (D7)
    async def _do_delete(self, req: DeleteRequest, budget: float) -> DeleteResponse
        # _do_put of a tombstone
    async def _forward(self, order: list[str], req: ForwardRequest, budget: float) -> ForwardResponse | None
        # try each node in order; reaching this node itself → coordinate locally;
        # next node only on UNAVAILABLE / DEADLINE_EXCEEDED; None when every attempt failed (caller → QUORUM_FAILED)

    def _route_order(self, pref: list[str]) -> list[str]      # ALIVE, then SUSPECT, then DEAD, list order inside each
    def _key_lock(self, key: str) -> asyncio.Lock             # per-key lock for the counter rule
    def _spawn(self, coro) -> None                            # tracked background task (late replies, read repair)
```

```python
# dynamo/replica.py  (Meet) — every handler wrapped with @rpc_handler("node")
class NodeServiceServicer(dynamo_pb2_grpc.NodeServiceServicer):
    def __init__(self, node: Node) -> None
    async def Forward(self, request: ForwardRequest, context) -> ForwardResponse            # → coordinator.coordinate
    async def ReplicaPut(self, request: ReplicaPutRequest, context) -> ReplicaPutResponse   # hint_for empty → storage.apply, set → storage.put_hint
    async def ReplicaGet(self, request: ReplicaGetRequest, context) -> ReplicaGetResponse   # → storage.get (all versions, tombstones too)
    async def Gossip(self, request: GossipMessage, context) -> GossipMessage                # → membership.on_gossip (D0-P)
    async def DeliverHint(self, request: ReplicaPutRequest, context) -> ReplicaPutResponse  # → storage.apply (own RPC? ask Prateek)
```

```python
# client/cli.py  (Meet)
#   kv put KEY VALUE [--w N] | get KEY [--r N] | delete KEY [--w N] | info | locate KEY | local-get KEY
#   shared parent parser on every subcommand: [-v] [--node HOST:PORT] [--timeout S] [--context-file PATH]
def parse_args(argv: list[str] | None = None) -> argparse.Namespace   # --node $KV_NODE or localhost:50051, --timeout 2.0, --context-file $KV_CONTEXT or .kv_context.json
def load_context(path: str, key: str) -> bytes                         # b"" when none; file values are base64
def save_context(path: str, key: str, context: bytes) -> None          # temp file + os.replace
def show_value(value: bytes) -> str                                    # UTF-8, else "base64:..."
def run(args: argparse.Namespace, stub: KVStoreStub) -> int            # values → stdout, hints and -v details → stderr; returns exit code
def main(argv: list[str] | None = None) -> None
```

```python
# needed from D0-P (membership.py) — item 6 of "Needs Prateek"
def state(self, node_id: str) -> MemberState     # Coordinator._route_order
def members(self) -> list[Member]                # ClusterInfo
```

```protobuf
// proto/dynamo/dynamo.proto — this half
service KVStore {
  rpc Put(PutRequest) returns (PutResponse);
  rpc Get(GetRequest) returns (GetResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
  rpc ClusterInfo(Empty) returns (ClusterInfoResponse);
  rpc Locate(LocateRequest) returns (LocateResponse);
  rpc LocalGet(LocalGetRequest) returns (LocalGetResponse);
}
service NodeService {
  rpc Forward(ForwardRequest) returns (ForwardResponse);
  rpc ReplicaPut(ReplicaPutRequest) returns (ReplicaPutResponse);
  rpc ReplicaGet(ReplicaGetRequest) returns (ReplicaGetResponse);
  rpc Gossip(GossipMessage) returns (GossipMessage);               // D0-P
  rpc DeliverHint(ReplicaPutRequest) returns (ReplicaPutResponse); // D0-P
}
enum Status { STATUS_UNSPECIFIED = 0; OK = 1; NOT_FOUND = 2; QUORUM_FAILED = 3; INVALID = 4; }

// Put/Get/Delete, Forward, ReplicaPut (single Version), ReplicaGet: as in SPEC.md
// GetResponse.context is set on NOT_FOUND too (Q11)

message ClusterInfoResponse {        // SPEC numbers 1–4 kept, rest added
  repeated Member members = 1;       // address, state, incarnation, heartbeat (Member is D0-P)
  uint32 n = 2; uint32 r = 3; uint32 w = 4;
  string node_id = 5;
  uint32 vnodes = 6;
  string ring_fingerprint = 7;
  uint64 live_keys = 8;
  uint64 tombstones = 9;
}
message LocateRequest    { string key = 1; }
message LocateResponse   { repeated string nodes = 1; uint32 position = 2; }
message LocalGetRequest  { string key = 1; }
message LocalGetResponse { Status status = 1; repeated Version versions = 2; string node_id = 3; }
```

## 5. Flow
1. Client sends Put/Get/Delete to any node; the KVStore handler takes `x-request-id` or makes one, and computes the budget.
2. The node computes the preference list from its local ring.
3. Routing:
   - **Put / Delete:** first replica → coordinate here. Anyone else (including replica 2 or 3) →
     `_forward` to the replicas in ALIVE, then SUSPECT, then DEAD order (list order within each);
     move on after UNAVAILABLE or DEADLINE_EXCEEDED; if the next one is this node, coordinate
     locally; every attempt fails → `QUORUM_FAILED`.
   - **Get:** any replica → coordinate here. Not a replica → `_forward`, same order and fallback.
   - A Forward is always coordinated by its receiver (`coordinate`).
   - `ClusterInfo`, `Locate`, `LocalGet` are answered locally, never forwarded.
4. Write: under the key's lock the coordinator builds the new clock (Q10), writes locally (1 ack)
   and sends `ReplicaPut` to the other replicas with `rpc_timeout_ms` each; any gRPC error = no ack;
   returns after W acks, the rest finish in the background.
5. Read: local read + `ReplicaGet` to the others; after R replies, dominated versions are dropped and
   concurrent ones kept; the reply carries the non-tombstone values and the merged context (Q11).
6. Reply to the client with the body `Status` and trailing `x-request-id`.
7. After a Get reply, read repair (D7) sends one `ReplicaPut` per sibling in the background to every
   replica whose reply was missing versions or "not found", including late replies; best-effort.

## 6. Edge cases

| Case | What happens | Test |
|---|---|---|
| Put / Delete arrives at replica 2 or 3 | Forwarded to the first replica | D5 `test_write_forwarded_to_first_replica` |
| First replica down when forwarding | Move to the next replica; every attempt fails → `QUORUM_FAILED` | D5 `test_forward_fallback` |
| First replica down and the next replica is the receiving node | It coordinates locally | D5 `test_forward_reaches_self` |
| Gossip wrongly marks a replica DEAD | It is tried last; costs at most a timeout, never a failed request | D7 `test_route_order_wrong_view` |
| Get arrives at replica 2 or 3 | It coordinates itself, no extra hop | D5 `test_get_no_extra_hop` |
| Forward arrives at a node whose ring view differs | It coordinates anyway, never forwards again | D5 `test_forward_never_reforwarded` |
| A gossip table entry carries a ring fingerprint different from this node's | Logged at ERROR, node keeps running | D6 `test_fingerprint_mismatch_logged` |
| Two puts with the same context through one coordinator | Different counters (Q10) | D5 `test_same_context_different_counters` |
| Forward retry after a partial first write | Two siblings with the same value collapse into one, clocks merged | D3 `test_identical_siblings_collapse` |
| Incoming version dominated by what the replica holds | Acknowledged, nothing stored | D3 `test_dominated_version_ignored` |
| Incoming version concurrent with what the replica holds | Kept as a sibling | D3 `test_concurrent_version_kept` |
| Get on a deleted key | NOT_FOUND with the tombstone's clock as context; CLI saves it | D5 `test_notfound_returns_context` |
| Put after a delete, with the saved context | Replaces the tombstone | D5 `test_put_after_delete_replaces_tombstone` |
| Third replica replies after the client got its answer and is stale | It is still repaired | D7 `test_late_reply_repaired` |
| Repair `ReplicaPut` fails | Logged, never retried | D7 `test_repair_failure_logged` |
| Handler raises `NotImplementedError` (D0 stub) | `UNIMPLEMENTED` | D0 `test_stub_rpcs_unimplemented` |
| Handler raises any other exception | `abort(INTERNAL)`, counted as no ack; cancellation passes through | D0 `test_handler_internal_error` |
| WAL write or fsync fails | Log CRITICAL, `os._exit(1)` | D1 |
| Incoming deadline shorter than `request_timeout_ms` | Budget = time left; forwarded call gets only what remains | D5 `test_budget_from_deadline` |
| Client sets no deadline | Budget = `request_timeout_ms` | D0 `test_budget_without_deadline` |
| Peer never heard from after boot | SUSPECT after 3 s, DEAD after 10 s | D6 |
| Node's `last_alive` is more than 24 h old at startup | Refuses to run until its data folder is wiped | D1 |
| Node id missing from both flag and env var, or not in config | Startup fails | D0 `test_startup_without_node_id` |
| `make run` with empty `NODE` | Errors out | D0 (manual) |
| `build_node` called | Binds nothing, creates no files | D0 `test_build_node_no_io` |
| SIGTERM to a node | `node.stop()` runs; no SIGKILL after 10 s in Docker | D0 `test_sigterm_stops_node` |
| Client sends its own `x-request-id` | That ID is used | D0 `test_client_request_id_kept` |
| Two concurrent requests | Keep separate request IDs | D0 `test_concurrent_request_ids` |
| `ClusterInfo` / `Locate` / `LocalGet` sent to any node | Answered locally, never forwarded | D0 `test_local_calls_not_forwarded` |
| `-v` after the subcommand (`put k v -v`) | Parses | D0 `test_cli_verbose_after_subcommand` |
| Reply with STATUS_UNSPECIFIED or an unknown status number | `ERROR: bad reply from HOST:PORT (status N), request-id <id>`, exit 3 | D0 `test_cli_unknown_status` |

### 6.1 Known limitations
- Two writes with the same context through the same coordinator get counters 4 and 5, so 5 looks newer and silently replaces 4 although the writes were concurrent. Dotted version vectors fix this; out of scope.

## 7. Tests
- **Unit:**
  - `test_ring.py`: deterministic across builds; N distinct nodes; about 1/5 of keys move when a node is removed; wrap-around; a key exactly on a vnode position (`bisect_left`); fingerprint stable, and changes with N.
  - `test_reqid.py`: client ID kept; ID generated when absent; background tasks inherit it; two concurrent requests keep separate IDs; decorator maps `NotImplementedError` → UNIMPLEMENTED and other exceptions → INTERNAL.
  - `test_cli.py`: `-v` / `--node` / `--timeout` parse after the subcommand; exit-code mapping; context file saved atomically, base64, keyed by key, from `--context-file` / `$KV_CONTEXT`; sibling printing on stdout and hint on stderr; STATUS_UNSPECIFIED and an unknown status number both give the "bad reply" message with the request ID and exit 3.
  - `test_server.py`: `build_node` binds nothing and creates no files; `request_budget` with and without a deadline; flags default from env vars; startup fails without a node id.
- **In-process cluster** (Prateek's fixture): 5 nodes boot and every stub RPC answers UNIMPLEMENTED, never hangs; `Locate` gives the same answer on all nodes; `ClusterInfo` lists 5 members and is never forwarded; `x-request-id` comes back in trailing metadata, also on errors; port 0 → `node.port` is the bound port.
- **Docker:** `make up`, then `kv info` on each of ports 50051–50055 lists 5 members; `docker compose stop` stops every node cleanly within the grace period.
- **Later tasks** (from section 6): two puts with the same context through one coordinator get different counters (D5); a put after a delete replaces the tombstone (D5).

## 8. Interview questions on this part
1. Q: Why do writes go to the first replica but reads go to any replica?  A: Clock entries come only from coordinators, so one coordinator per key keeps vector clocks small. Reads create no clock entries, so the extra hop would be wasted.
2. Q: How do you stop forwarding loops without a hop count?  A: Receiving a Forward is the signal: its receiver always coordinates and never forwards again.
3. Q: Why retry only on UNAVAILABLE or DEADLINE_EXCEEDED?  A: Those are the only codes meaning the target never answered. Anything else is a real verdict, and retrying would hide it.
4. Q: Why carry the request ID in metadata and not in the proto?  A: It is for tracing only; metadata keeps it out of the data model and the WAL.
5. Q: How does the coordinator choose its clock counter?  A: `max(context[self], highest own counter among its local versions of the key) + 1`, under a per-key lock, so two writes with the same context never get identical clocks. The remaining gap (4 vs 5 looking ordered) is what dotted version vectors fix.
6. Q: Why must NOT_FOUND still return a context?  A: A deleted key is a tombstone with a clock. If the client doesn't get that clock, its next put is concurrent with the tombstone instead of replacing it.

## 9. As built
<filled after implementation: what changed from this design and why>

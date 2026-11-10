# D0-M — Request path, server wiring & build contracts

Owner: Meet · Reviewer: Prateek · Status: DESIGNING (draft) · Date: 2026-10-24

> Draft. Items marked **(proposed — confirm)** still need Meet's confirmation. The list under
> "Needs Prateek" changes the SPEC, a shared file, or a file Prateek owns; each needs his agreement
> and a `DEC-NN` entry before this doc is DESIGNED.

### Needs Prateek (agreement + DEC entry)
1. Vector clocks as in the SPEC (Meet now follows the SPEC); `version.py` added to the file list, owned by Prateek; replicas merge incoming versions; identical siblings collapse.
2. Gossip states as in the SPEC (ALIVE, SUSPECT 3 s, DEAD 10 s); `incarnation` = boot time in ms; all peers start ALIVE with the timer starting at boot (his D6 / `membership.py` / `gossip.py`).
3. Each gossip entry carries the node's `ring_fingerprint` (his half of the proto).
4. Proto stays at `proto/dynamo/dynamo.proto`: update the SPEC layout and the TASKS ownership table.
5. `request_timeout_ms` (default 1500) added to the SPEC config table.
6. `NODE_ID` required, `CONFIG` default `cluster.local.yaml` (his `config.py` / `cluster.yaml`).
7. WAL write or fsync failure → CRITICAL + `os._exit(1)` (his storage, D1).
8. Tombstone purge after 24 h at snapshot time + `last_alive` guard at startup (his storage, D1).
9. New local RPCs `Locate` and `LocalGet` next to `ClusterInfo`; `ClusterInfo` reports tombstone counts and member state/incarnation/heartbeat.

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
kept as siblings. Expected outcomes travel in the response's `Status`; a gRPC error only means a node
could not be reached or broke. Read repair reuses `ReplicaPut` (built in D7). Liveness comes from
push-pull gossip with one random peer per second. A request ID is created at the client entry point,
carried in gRPC metadata and a `ContextVar`, used only for logs, and never persisted. Each node finds
its own entry in `cluster.yaml` from a required `NODE_ID`.

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
- **Ring fingerprint:** the first 16 hex characters of SHA-256 over the sorted entries plus N. Each node’s ring fingerprint is carried in its entry of the gossip table. A node that sees a fingerprint different from its own logs it at ERROR and keeps running.

**Q2. Versions and read repair**
- Versions follow the SPEC: vector clocks. The coordinator drops dominated versions and keeps concurrent ones as siblings. Tombstones are versions like any other.
- A replica merges an incoming version with what it holds (drop dominated, keep concurrent) rather than doing "apply if newer".
- Siblings with an identical value and tombstone flag collapse into one, with their clocks merged. (A Forward retry after a partial first attempt can otherwise create two siblings with the same value.)
- `version.py` holds the comparison and merge rules and is owned by Prateek.
- Read repair reuses ReplicaPut. There is no new RPC. It pushes the reconciled sibling set, exactly as stored, to every replica whose reply was missing versions or "not found", including the third replica whose reply arrives after the client got its answer.
- Repair runs in the background after the client reply. It is best-effort: failures are logged and never retried.
- Read repair is designed here and **implemented in D7**; D0 keeps only the stubs.

**Q3. Where does the proto file live?**
- The proto is at `proto/dynamo/dynamo.proto`, and the generated files land in `dynamo/`.
- Generate with `python -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. proto/dynamo/dynamo.proto`.
- The generated code imports `from dynamo import dynamo_pb2`.
- Reason to keep it over the SPEC's `proto/dynamo.proto`: with that path, the generated grpc file imports `dynamo_pb2` as a top-level module and fails inside the package. The SPEC layout and the ownership table are updated to match.

**Q4. What is the error convention?**
- Expected outcomes go in the response body's Status enum (`OK`, `NOT_FOUND`, `INVALID`, `QUORUM_FAILED`), and gRPC itself returns OK.
- A gRPC error code means only that the node couldn't be reached or something broke.
- The coordinator counts any gRPC error from a replica, of any code, as "no ack". A replica's "not found" is a normal reply and counts toward R.
- Forward retries only on UNAVAILABLE or DEADLINE_EXCEEDED. Body statuses are passed back unchanged.
- **Time budget:** each node's budget is `min(request_timeout_ms, time left on the incoming gRPC deadline)`. A forwarded call gets only what remains, and each replica call uses `rpc_timeout_ms`. `request_timeout_ms` is added to the SPEC config table with a default of 1500.
- A replica with an internal error calls `context.abort(INTERNAL)` and never acks.
- If a WAL write or fsync fails, the node logs CRITICAL and exits immediately with `os._exit(1)`, with no graceful shutdown.
- **CLI exit codes:** OK 0, NOT_FOUND 1, INVALID 2, any gRPC error 3, QUORUM_FAILED 4.

**Q5. In what order is the Node built, started and stopped?**
- **Build:** config, storage, ring, peers, membership, coordinator, handoff, server. Constructors only wire objects together and do no I/O.
- **Start (`async start()`):** recover storage, start the gRPC server, start the membership loop, then start the handoff loop.
- **Stop (`async stop()`, triggered by SIGTERM):**
  1. `server.stop(grace=2s)`.
  2. Cancel the membership and handoff loops.
  3. Await the coordinator's background task set for up to 2s, then cancel what's left.
  4. Close the peer channels.
  5. Close storage.
- Crash safety comes only from the WAL; correctness never depends on stop order.

**Q5a. How do nodes know a peer is alive?**
- Membership comes only from `cluster.yaml`.
- Liveness comes from push-pull gossip of heartbeat tables, one random peer per second.
- States follow the SPEC: ALIVE, SUSPECT after 3 s without heartbeat progress, DEAD after 10 s.
- `incarnation` is the node's boot time in milliseconds.
- All peers start as ALIVE, and the down timer starts at boot, so a peer never heard from goes SUSPECT after 3 s.
- Each node's entry in the gossip table carries its own `ring_fingerprint`, so every node eventually sees every other node's fingerprint, even nodes it never talks to directly.
- Peer channels set `max_reconnect_backoff_ms = 1000`.
- **Still to paste:** Gossip message fields, merge rule, what happens on restart.

**Q6. Where is the request ID created, and how does it travel?**
- **Created** only at the KVStore Put, Get and Delete handlers. The client's `x-request-id` metadata is used if present; otherwise the node generates `uuid.uuid4().hex`.
- **Between nodes**, it is carried in the gRPC metadata key `x-request-id` on Forward, ReplicaPut and ReplicaGet. It is never a proto field.
- **Inside a node**, it lives in a `ContextVar` that background tasks inherit automatically. A logging filter adds `request_id` and `node_id` to every log line.
- **Back to the client**, it is returned in trailing metadata `x-request-id`, and `kv -v` prints it.
- The request ID is never persisted anywhere: not in storage, the WAL, Version or hints.
- Gossip rounds and handoff deliveries are logged with the labels `gossip` and `handoff`.
- It is used for tracing only, never for correctness.

**Q7. How does a node find its own entry in `cluster.yaml`?**
- `NODE_ID` (or `--node-id`) is required, with no default. Startup fails if it is missing or not in the config.
- `CONFIG` defaults to `cluster.local.yaml`; Docker sets `cluster.yaml`.
- A config address is only what peers use to reach the node.
- The node binds `0.0.0.0` on the port from its own entry; `--port` overrides it.
- The data folder is `<data-dir>/<node_id>/`, with `--data-dir` defaulting to `./data`.
- `make run NODE=nodeX` errors out if `NODE` is empty, and runs with `cluster.local.yaml`.
- `make up` runs `docker compose`, which sets `NODE_ID` and `CONFIG` per service using a YAML anchor. Inside the containers every node listens on 50051, mapped to host ports 50051–50055.
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
| Request ID as a proto field, or persisted anywhere | Tracing only, never needed for correctness |
| Forward retry on any gRPC error or body status | Only UNAVAILABLE and DEADLINE_EXCEEDED mean the target never answered; every other outcome is a real answer (see 3.1) |
| All-to-all Ping for liveness | About n² messages per second and no path to dynamic membership; gossip costs about n and reaches every node in about log n rounds (see 3.2) |
| (timestamp_ms, writer) last-write-wins | Silently loses concurrent writes and lets clock skew pick an older write; the SPEC's API already has room for siblings (see 3.3) |
| Proto at `proto/dynamo.proto` (SPEC) | The generated grpc file would import `dynamo_pb2` as a top-level module and fail inside the package |
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
# ring.py — pure, no I/O
def hash32(x: str) -> int                     # int(md5(x).hexdigest(), 16) % 2**32
class Ring:
    def __init__(self, node_ids: list[str], vnodes: int): ...   # (position, node_id) sorted
    def preference_list(self, key: str, n: int) -> list[str]    # bisect_left, wrap, skip repeats
    def walk(self, key: str) -> Iterator[str]
    def fingerprint(self) -> str              # first 16 hex of SHA-256(sorted entries + N)  (name proposed — confirm)

# Node
class Node:  # holds config, storage, ring, peers, membership, coordinator, handoff, server
    async def start(self) -> None   # recover storage → gRPC server → membership loop → handoff loop
    async def stop(self) -> None    # server.stop(2s) → cancel loops → await bg tasks ≤2s → close peers → close storage

# gRPC metadata
x-request-id                        # in: client (optional); between nodes: Forward/ReplicaPut/ReplicaGet; out: trailing metadata
```
Proto in this half (exact messages still to write): `Status` (`OK`, `NOT_FOUND`, `QUORUM_FAILED`,
`INVALID`); `KVStore` Put/Get/Delete/ClusterInfo plus local `Locate` and `LocalGet`; `NodeService`
Forward/ReplicaPut/ReplicaGet. Signatures for `peers.py`, `coordinator.py`, `replica.py`,
`server.py`, `client/cli.py`, and the `ClusterInfo` / `Locate` / `LocalGet` messages: **to be
written** before DESIGNED. Gossip message fields belong to D0-P.

## 5. Flow
1. Client sends Put/Get/Delete to any node; the KVStore handler takes `x-request-id` or makes one.
2. The node computes the preference list from its local ring.
3. Routing:
   - **Put / Delete:** first replica → coordinate here. Anyone else (including replica 2 or 3) →
     Forward to the replicas in ALIVE, then SUSPECT, then DEAD order (list order within each); move
     on after UNAVAILABLE or DEADLINE_EXCEEDED; if the next one is this node, coordinate locally;
     every attempt fails → `QUORUM_FAILED`.
   - **Get:** any replica → coordinate here. Not a replica → Forward, same order and fallback.
   - A Forward is always coordinated by its receiver.
   - `ClusterInfo`, `Locate`, `LocalGet` are answered locally, never forwarded.
4. The coordinator calls the replicas with `rpc_timeout_ms` each, inside the request budget
   `min(request_timeout_ms, time left on the incoming deadline)`; any gRPC error = no ack.
5. Replies are reconciled: dominated versions dropped, concurrent ones kept as siblings.
6. Reply to the client with the body `Status` and trailing `x-request-id`.
7. After a Get reply, read repair (D7) pushes the sibling set in the background to every replica
   whose reply was missing versions or "not found", including late replies; best-effort, no retry.

## 6. Edge cases
**(proposed — confirm, and fill the Test column)**

| Case | What happens | Test |
|---|---|---|
| Put / Delete arrives at replica 2 or 3 | Forwarded to the first replica | |
| First replica down when forwarding | Move to the next replica; every attempt fails → `QUORUM_FAILED` | |
| First replica down and the next replica is the receiving node | It coordinates locally | |
| Gossip wrongly marks a replica DEAD | It is tried last; costs at most a timeout, never a failed request | |
| Get arrives at replica 2 or 3 | It coordinates itself, no extra hop | |
| Forward arrives at a node whose ring view differs | It coordinates anyway, never forwards again | |
| A gossip table entry carries a ring fingerprint different from this node's | Logged at ERROR, node keeps running | |
| Forward retry after a partial first write | Two siblings with the same value collapse into one, clocks merged | |
| Incoming version dominated by what the replica holds | Acknowledged, nothing stored | |
| Incoming version concurrent with what the replica holds | Kept as a sibling | |
| Third replica replies after the client got its answer and is stale | It is still repaired (D7) | |
| Repair `ReplicaPut` fails | Logged, never retried | |
| Replica internal error | `context.abort(INTERNAL)`, counted as no ack | |
| WAL write or fsync fails | Log CRITICAL, `os._exit(1)` | |
| Incoming deadline shorter than `request_timeout_ms` | Budget = time left; forwarded call gets only what remains | |
| Peer never heard from after boot | SUSPECT after 3 s, DEAD after 10 s | |
| Node's `last_alive` is more than 24 h old at startup | Refuses to run until its data folder is wiped | |
| `NODE_ID` missing or not in config | Startup fails | |
| `make run` with empty `NODE` | Errors out | |
| Client sends its own `x-request-id` | That ID is used | |
| `ClusterInfo` / `Locate` / `LocalGet` sent to any node | Answered locally, never forwarded | |

## 7. Tests
**To be written** (unit, in-process cluster, Docker).

## 8. Interview questions on this part
**To be written.**

## 9. As built
<filled after implementation: what changed from this design and why>

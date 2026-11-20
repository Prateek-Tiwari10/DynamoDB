# D4 — Ring + static cluster + Docker

Owner: Meet · Reviewer: Prateek · Status: REVIEW · Date: 2026-11-21

> Confirmed by Meet on 2026-11-21 as written. No shared file changes.
> Most of the code already exists from D0-M (`ring.py`, `Dockerfile`, `docker-compose.yml`, the
> `Locate` / `ClusterInfo` calls). D4 fixes the open design questions, adds a local 5-node runner
> and a cluster test, and writes down the numbers.

## 1. Requirement
From TASKS.md: MD5 ring, 32 vnodes per node, `preference_list(key, n)` returning n distinct
physical nodes, `walk(key)` for sloppy quorum later. Five-node Docker Compose with storage per node
so data survives container restarts.
**Done when:** ring tests (deterministic, distinct, ~1/5 of keys move when a node is removed);
`docker compose up` → `kv info` against any node lists 5 members.

## 2. Chosen approach (plain words)
The ring is built once at startup from `cluster.yaml` and **never changes while the cluster runs**.
A dead node keeps its place on the ring; being DEAD only changes *where requests are sent* (D7
routing order) and triggers sloppy quorum (D8). Every node builds the same ring, so any node can
answer "who stores this key" by itself, and the ring fingerprint lets us check that they agree.

The cluster runs either in Docker (`make up`, 5 containers, one image) or as 5 local processes
(`tools/cluster_local.sh`, for laptops without Docker). Both use the same code; only the config
file differs (`cluster.yaml` with service names vs `cluster.local.yaml` with localhost ports).

### Decisions (confirmed)

**Q1. Does the ring change when a node is DEAD?**
- Decision: **no.** Ownership is fixed by `cluster.yaml`. DEAD changes only routing (D7) and the
  choice of a stand-in node (D8, `ring.walk`).
- Why: if the ring moved on every failure, a node that flaps (dies for 5 s, comes back) would move
  about 1/5 of all keys twice, and nodes with different gossip views would disagree on owners
  during the change. Dynamo also keeps the preference list fixed and uses hinted handoff instead.

**Q2. Lookup and equal positions**
- Sorted list of `(position, node_id)`, `bisect_left` on the positions, wrap to index 0 after the
  end (already built). A key exactly on a vnode belongs to that vnode.
- Two vnodes on the same position: practically never (160 points among 2³² ≈ 4.3 billion), and the
  sort breaks the tie by `node_id`, so every node still builds the same order. Nothing else needed.

**Q3. `n` larger than the number of nodes**
- `preference_list` returns every node once (the walk stops when all nodes are seen). No error:
  `load_config` already rejects `n > number of nodes` at startup, so this only matters in tests.

**Q4. Vnode count: measured**
10,000 keys (`key0` … `key9999`), 5 nodes. "First owner" = keys whose replica 1 is that node;
"replica copies" = keys stored on that node with N = 3 (perfect = 2,000 and 6,000).

| vnodes | first owner per node | std dev | replica copies per node | std dev | keys moved when node5 is removed |
|---|---|---|---|---|---|
| 1 | 206 – 5,074 | 1,755 | 3,793 – 8,661 | 1,832 | 1,133 (only node5's) |
| 8 | 1,312 – 2,542 | 448 | 4,524 – 8,184 | 1,235 | 1,312 |
| **32** | **1,811 – 2,311** | **181** | **5,753 – 6,323** | **239** | **2,311** |
| 128 | 1,661 – 2,399 | 257 | 5,540 – 6,726 | 410 | 2,107 |

- Decision: keep **32**. One vnode per node is very uneven (one node owns half the keys). 8 is
  still uneven. 32 is within about ±15 % of the ideal. 128 was not better on this key set:
  positions are random, so more vnodes only make the spread better *on average*; it also makes the
  ring 4× bigger. In every row the moved keys are exactly the removed node's own keys.

**Q5. Docker**
- Addresses by compose service name (`node1:50051` … `node5:50051`, from `cluster.yaml`). Every
  container listens on 50051 inside; on the host they are 50051–50055, so the CLI on the host uses
  `kv --node localhost:5005X …`.
- Storage per node: one bind mount `./data:/data`; each node writes only `/data/<node_id>/`. Data
  survives `docker compose stop/start` and `down`/`up`; `rm -rf data/` wipes the cluster.
- `docker compose stop nodeX` = SIGTERM = clean stop (D0-M signal handler). `docker compose kill
  nodeX` = SIGKILL = crash. The demos use `kill` to show crash safety and `stop` for maintenance.
- `docker compose start nodeX` brings it back with the same data and a new incarnation.

**Q6. Running 5 nodes without Docker**
- `tools/cluster_local.sh start|stop|status`: starts node1..node5 as background processes with
  `cluster.local.yaml`, data in `./data/<node_id>/`, logs in `./data/<node_id>.log`, PIDs in
  `./data/<node_id>.pid`. `stop` sends SIGTERM; `kill NODE` sends SIGKILL to one node.
- Why: Meet's laptop has no Docker; the same checks (`kv info` on every port) then run locally.

**Q7. Check that the cluster agrees**
- `tools/check_cluster.sh [host]`: runs `kv info` on ports 50051–50055 and `kv locate cart:42` on
  each; passes if every node lists 5 members, all fingerprints are equal and all `locate` answers
  are equal. Prints `CLUSTER OK`.

## 3. Alternatives we rejected
| Option | Why not |
|---|---|
| Rebuild the ring without DEAD nodes | A flapping node moves ~1/5 of all keys twice; nodes with different gossip views disagree on owners |
| `hash(key) % 5` | Changing the node count moves almost every key |
| 128 or more vnodes | No better spread on our keys, 4× more ring entries |
| Named Docker volumes, one per node | Same safety; a bind mount is easier to look at and wipe during a demo |
| One container port per node (50051..50055 inside) | Every node can use the same config entry pattern `nodeX:50051`; only the host mapping differs |
| Docker only | Meet's laptop has no Docker; a local script runs the same cluster |

## 4. Interfaces and data structures
```python
# dynamo/ring.py (Meet) — unchanged from D0-M
def hash32(text: str) -> int
class Ring:
    def __init__(self, node_ids: list[str], vnodes: int) -> None
    def position(self, key: str) -> int
    def walk(self, key: str) -> Iterator[str]
    def preference_list(self, key: str, n: int) -> list[str]
    def fingerprint(self, n: int) -> str
```
```
tools/cluster_local.sh start | stop | status | kill NODE     # 5 local processes, cluster.local.yaml
tools/check_cluster.sh [HOST]                               # kv info + kv locate on 50051-50055
```

## 5. Flow
1. `make up` (or `tools/cluster_local.sh start`): 5 nodes start, each builds the same ring from the
   same config and logs its fingerprint.
2. `kv --node localhost:5005X info` on each node: 5 members, same fingerprint.
3. `kv --node localhost:5005X locate cart:42` on each node: same position, same 3 replicas.
4. `docker compose kill node3` / `docker compose start node3`: node3 comes back with its data.

## 6. Edge cases
| Case | What happens | Test |
|---|---|---|
| Node DEAD | Ring unchanged; only routing changes (D7/D8) | design rule; D7 tests |
| Key past the last vnode | Wraps to the first vnode | `test_key_after_the_last_vnode_wraps_to_the_first` |
| Key exactly on a vnode | Belongs to that vnode | `test_key_exactly_on_a_vnode_belongs_to_that_vnode` |
| `n` > nodes | All nodes once; config rejects it at startup anyway | `test_n_larger_than_cluster_returns_every_node` |
| Node removed from the config | Only its keys move (~1/5) | `test_removing_a_node_moves_only_its_keys_about_a_fifth` |
| Config listed in a different order on another node | Same ring (node ids are sorted) | `test_same_input_gives_same_ring` |
| Vnode count changed on one node | Different fingerprint → mismatch visible in `kv info` | `test_fingerprint_changes_with_n_nodes_and_vnodes` |
| 5 nodes started together | Every node lists 5 members, same fingerprint, same `Locate` | `test_cluster_agrees` |
| One node killed and restarted | Its data is back | `test_cluster_restart_keeps_node_data` |
| Container killed vs stopped | kill = crash (no clean stop), stop = SIGTERM clean stop | manual, Docker |

## 7. Tests
- Unit: existing `tests/test_ring.py` (10 tests) covers Q2/Q3 and the "1/5 moves" rule.
- In-process cluster (`tests/test_static_cluster.py`): 5 real nodes on free localhost ports in one
  test process (a small local fixture until Prateek's `conftest.py` fixture exists): every node
  lists 5 members, all fingerprints equal, `Locate` equal on all nodes, a stopped and restarted node
  keeps data written with `ReplicaPut`.
- Local processes: `tools/cluster_local.sh start` + `tools/check_cluster.sh` → `CLUSTER OK`.
- Docker (not possible on Meet's laptop, no Docker installed): `make up` + `tools/check_cluster.sh`.

## 8. Interview questions on this part
1. Q: Why does the ring not change when a node dies?  A: Ownership stays stable; a short outage
   should not move a fifth of the data twice. DEAD only changes routing and uses a stand-in node
   with a hint.
2. Q: Why 32 vnodes?  A: Measured: with 1 vnode one node owned half the keys; with 32 every node is
   within about ±15 % of 2,000 keys; 128 was not better and makes the ring 4× bigger.
3. Q: How does the CLI on your laptop reach a node inside Docker?  A: Each container's 50051 is
   mapped to a host port 50051–50055; inside the network nodes use service names.
4. Q: How do you prove all nodes agree on placement?  A: Same fingerprint in `kv info` on every
   node, and the same `kv locate` answer from every node.

## 9. As built
_Done 2026-11-21, built as designed._
- `ring.py`, `Dockerfile`, `docker-compose.yml` unchanged from D0-M (they already matched Q1–Q5).
- `tools/cluster_local.sh start|stop|kill|status [NODE...]` and `tools/check_cluster.sh [HOST] [KEY]` added. Checked on Meet's laptop: 5 local processes → `CLUSTER OK` (all nodes: 5 members, ring `e99736f3c5dfcdd1`, `cart:42` → node1, node4, node3); after `kill node3` the check reports node3 unreachable and `CLUSTER NOT OK` while the other nodes still list node3 on the ring (Q1); after `start node3` → `CLUSTER OK` again.
- `tests/test_static_cluster.py`: 5 in-process nodes agree on members, fingerprint and `Locate`; a restarted node keeps its data. It has its own small fixture; switch to Prateek's `conftest.py` fixture when it exists.
- Not yet checked: `make up` in Docker, because Docker is not installed on Meet's laptop. Same check: `make up && tools/check_cluster.sh`.
- Suite: 216 passed, 1 skipped (with Prateek's D3 code).

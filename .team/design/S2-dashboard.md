# S2 — Live dashboard

Owner: Meet · Reviewer: Prateek · Status: REVIEW · Date: 2026-11-22

## 1. Requirement
TASKS S2 / SPEC: a page showing the ring, node states and key counts per node. Asked for now:
watch the Azure cluster live (every node's state) and see which key is stored where.

Files: `tools/dashboard.py`, `tools/dashboard.html`, `tests/test_dashboard.py`.

## 2. Chosen approach (plain words)
A small web server, `python -m tools.dashboard`, runs **next to the cluster** (on the Azure VM, or
on the laptop). Once a second it asks every node for `ClusterInfo` (its member table, key and
tombstone counts, ring fingerprint) over the normal client API, and keeps the latest answers.
The page polls `/api/status` once a second and redraws.

The server listens on **127.0.0.1 only**. From the laptop you open it through the ssh tunnel you
already use: `ssh -L 8080:localhost:8080 azureuser@<vm>`, then http://localhost:8080. No new port
is opened on Azure, so there is nothing for strangers to reach.

The page shows:
1. **Node cards**: one per node: state (ALIVE / SUSPECT / DEAD / no answer) as a colour, live
   keys, tombstones, heartbeat, incarnation (boot time, so a restart is visible).
2. **"Who sees whom" grid**: 5 × 5, row = the node asked, column = its opinion of each node. During
   a kill you watch the column of the killed node turn SUSPECT then DEAD, node by node, as gossip
   spreads.
3. **The ring**: a circle with every vnode (5 × 32 = 160 ticks) coloured by its node, drawn with
   the same `Ring` class the nodes use, so it is exactly their placement.
4. **Key lookup**: type a key → its ring position (a marker on the circle, arrows to its N
   replicas, from `Locate`) and what **each of the 5 nodes** really holds for it (`LocalGet`):
   values, vector clocks, tombstones, "owner" or "not an owner". A stale replica or siblings are
   visible at once. Keys looked up are kept in a "recent keys" list that refreshes too.

5. **Put / Get / Delete box** (Meet's choice, 2026-11-22): key, value, coordinator node
   (default: the first node that answers), optional R / W (0 = cluster default). The reply shows
   status, values (siblings listed), `written_to` / `read_from`. The server keeps the last context
   of each key from every Get / Put reply, like the CLI's context file, so normal writes replace the
   old value. A **"write without context"** checkbox sends an empty context on purpose: through a
   different coordinator that makes a sibling, for the siblings demo. The key is added to "recent
   keys" so its placement is shown at once.

No kill / start buttons: those stay in the terminal (`docker compose kill nodeX`).

## 3. Alternatives we rejected
| Option | Why not |
|---|---|
| FastAPI (named in SPEC) | Needs `fastapi` + `uvicorn` in `requirements.txt` (shared, DEC) and in the Docker image; the stdlib `http.server` does the same for 2 JSON routes and one page. |
| Page hosted outside the VM (e.g. a public web page) | It would need the nodes' gRPC ports open to the internet; our gRPC has no auth. |
| Open port 8080 on Azure | Same reason; the ssh tunnel gives the same page without exposing anything. |
| List every key per node | There is no RPC that lists keys; a new one is a shared proto change (Prateek). Lookup by key + recent keys covers "which key is where". Could be a later DEC. |
| Show hints held by stand-ins | Same: hints are only inside `storage`; no RPC returns them. |
| Push updates (websocket / server-sent events) | Polling once a second is enough for a 5-node demo and much simpler. |
| ~~Buttons to kill / start nodes~~ | Rejected first (risk); **taken after all** on Meet's request, see §10. |
| Contexts kept in the browser | The server already has them from the replies; one place, and the same behaviour as the CLI. |
| Read-only page | Asked for put / get / delete; writes go through the normal coordinator path, so nothing new on the node side. |

## 4. Interfaces and data structures
Uses only existing client RPCs: `ClusterInfo`, `Locate`, `LocalGet`, and `dynamo.ring.Ring`,
`dynamo.config.load_config`. No change to nodes, proto or shared files.

```python
# tools/dashboard.py
def node_view(info: ClusterInfoResponse) -> dict          # proto -> JSON-ready dict
def ring_json(config: ClusterConfig) -> dict               # vnode positions per node
def key_json(key: str, locate, local_gets: dict) -> dict   # position, replicas, per-node versions
class Poller:                                              # background thread, ClusterInfo every 1 s
    def snapshot(self) -> dict
class Actions:                                             # put / get / delete with kept contexts
    def put(self, key, value, node, w, blind) -> dict
    def get(self, key, node, r) -> dict
    def delete(self, key, node, w) -> dict
class Handler(BaseHTTPRequestHandler):                     # GET /, /api/status, /api/ring, /api/key?k=
                                                           # POST /api/put, /api/get, /api/delete (JSON body)
def main(argv=None) -> None   # --config cluster.local.yaml --port 8080 --interval 1.0
```
JSON `/api/status`: `{"time": ..., "nodes": {"node1": {"answered": true, "live_keys": 10, "tombstones": 0,
"ring": "e997…", "members": {"node2": {"state": "ALIVE", "heartbeat": 812, "incarnation": ...}}}}}`

## 5. Flow
1. Start: load config (default `cluster.local.yaml`: node ids, `localhost:5005x`, vnodes, N; on the
   VM those are the host ports Docker maps), build the `Ring`, start the poller thread, serve.
2. Poller: every interval, `ClusterInfo` on each node with a 0.5 s timeout; a failure = "no answer".
3. Page: load `/api/ring` once, then `/api/status` (and `/api/key` for each recent key) every second.

## 6. Edge cases
| Case | What happens | Test |
|---|---|---|
| Node down | Card "no answer" (grey), its grid row grey; other rows show its SUSPECT/DEAD | unit + cluster |
| Nodes disagree on the ring | Fingerprint shown on each card; a mismatch is shown in red | unit |
| Key not stored anywhere | Lookup shows the replicas and "not found" on each node | cluster |
| Siblings / stale replica | Each node's versions listed with their clocks; differing ones highlighted | unit |
| Tombstone | Shown as "deleted" with its clock | unit |
| Binary or long values | Shown as UTF-8 with replacement, cut at 60 characters | unit |
| Config's nodes not reachable at all | Page loads, every card "no answer" | manual |
| Port 8080 taken | `--port` option; clear error | manual |
| Another web site in the same browser POSTs to localhost:8080 | Writes need the header `X-Dashboard: 1` and a JSON body; a cross-site page cannot send a custom header without a CORS preflight, which the server never allows | unit |
| Chosen coordinator is down | Reply "node3 does not answer" (UNAVAILABLE / DEADLINE), nothing else tried | cluster |
| QUORUM_FAILED / NOT_FOUND / INVALID | Shown as the status, in colour | unit |
| Value empty / very long | Empty allowed; values over 1 MiB rejected by the node (INVALID) and shown | unit |

## 7. Tests
- Unit: `node_view`, `ring_json` (160 ticks, same positions as `Ring`), `key_json` (owners
  marked, siblings, tombstone, value cut), HTTP routes with a fake poller.
- In-process cluster: poller against the `cluster` fixture → 5 answered nodes; kill one → "no answer"
  and others report SUSPECT/DEAD; `/api/key` after a put shows the value on its 3 owners.
- Actions: put then get returns the value; second put uses the kept context (no sibling); blind
  put through another coordinator makes 2 siblings; delete then get = NOT_FOUND; POST without the
  header → 403.
- Manual: on the Azure VM through the ssh tunnel, kill a container and watch.

## 8. Interview questions on this part
1. Q: Why is the grid 5 × 5 and not one state per node?  A: There is no central view in Dynamo;
   each node has its own opinion from gossip, and they disagree for a few seconds after a failure.
2. Q: How does the page know where a key lives?  A: The same `Ring` code (MD5 of the key, 32
   vnodes per node), and `LocalGet` on every node shows what each one really stores.
3. Q: Why not expose it on the internet?  A: The cluster has no authentication; the ssh tunnel
   gives access only to whoever has the ssh key.

## 9. As built
Built as designed (`tools/dashboard.py`, `tools/dashboard.html`, `tests/test_dashboard.py` 16 tests).
Changes:
1. A key's placement comes from the dashboard's own `Ring` (same code and config as the nodes)
   instead of the `Locate` RPC, so it still works when nodes are down; `LocalGet` still asks every
   node what it really stores. The page also lists the stand-in order after the N replicas.
2. "Coordinator" became "Send to node": a Put sent to any node is coordinated by the key's first
   ALIVE replica (D5 routing), so a blind put makes a sibling only when replica 1 is down. Found by
   the sibling test; the test now kills replica 1 first, and the page says so.
3. `?key=KEY` in the URL opens that key directly.
4. Ring markers 1/2/3 are spread along the circle (replica vnodes can be a few degrees apart).
5. On the Azure VM it runs as a systemd service `dynamo-dashboard` (127.0.0.1:8080, restarts on
   failure, starts at boot); reach it with `ssh -L 8080:localhost:8080 azureuser@<vm>`.

## 10. Change 2026-11-22: node controls (Meet's decision)
Asked: stop a node and start it again from the page, make one SUSPECT, and watch everything.

- **Buttons per node card:** Kill (SIGKILL, a crash), Stop (SIGTERM, clean), Pause (freeze the
  process: it stays up but answers nothing, like a hung or cut-off node: SUSPECT, then DEAD),
  Resume (after Pause: same incarnation, not a restart), Start (after Kill / Stop).
  Pause + Resume within 10 s shows SUSPECT → ALIVE without DEAD.
- **Off by default.** `--control docker` runs `docker compose <kill|stop|start|pause|unpause> nodeX`
  in the repo folder; `--control local` uses `tools/cluster_local.sh kill|stop|start` and
  SIGSTOP / SIGCONT on the pid in `data/<node>.pid` for pause / resume. Without the flag the
  buttons are hidden and `POST /api/node` answers 404.
- **Safety:** only the config's node ids and these five actions are accepted; the command is a fixed
  argument list (no shell); the same `X-Dashboard: 1` header guard as put/get/delete; the server
  still listens on 127.0.0.1 only. On the VM the service user is in the `docker` group.
- **Process state on the cards:** with control on, each round also reads the container / process
  state (running, paused, exited), so a card says "paused" or "exited" next to what gossip says.

Rejected: a separate control daemon (one more process for the same thing); sudo in the service
(the docker group is enough); network partitions with iptables (needs root, harder to undo).

### As built (§10)
Built as in §10 (`Control` in `tools/dashboard.py`, 11 more tests with a fake command runner, 27
dashboard tests in all). Cards are built once and updated in place; the buttons are rebuilt only
when the process state changes, so a click is not lost to the 1 s refresh. Which buttons show
depends on the process state (running: Pause / Stop / Kill; paused: Resume / Kill; exited: Start).
On the VM the service runs with `--control docker`; checked live: Pause node5 → node1 sees it
SUSPECT after 7 s → Resume → ALIVE, never DEAD.

## 11. Change 2026-11-22: hints and everything a node stores (Meet's request)
Asked: show hinted writes ("hint for whom") and any value on any node.

- **Read from the node's files, not over gRPC.** No RPC returns hints or lists keys, and a new
  one is a shared proto change. The dashboard runs on the same machine as the nodes, so with
  `--data-dir DIR` (default `data`, where Docker and `cluster_local.sh` keep `DIR/<node>/`) it
  **copies** the node's `snapshot.json`, `hints.json` and `wal.log` to a temp folder and loads the
  copy with Prateek's own `Storage` class (`load`, `keys`, `get`, `hints`). The live files are only
  read; recovery steps of `load` (removing tmp files, skipping a torn last line) run on the copy.
  Cached for 1 s per node.
- **Page:** (1) a new panel "What each node stores": one tab per node with its own keys (values,
  clocks; at most 200 shown) and the hints it holds, each with the owner it is for; (2) the key
  lookup table also shows, on a stand-in's row, "hint for nodeX: value".
- Off when the data folder is missing (panel says so); `/api/disk?node=X` answers 404 then.

Rejected: a `ListKeys` / `LocalHints` RPC (shared proto, DEC with Prateek; worth it if the
dashboard must work on another machine); loading the live folder directly (`load` may delete
leftover tmp files and opens the log for appending); parsing the log in the dashboard (would
copy Prateek's replay rules instead of using them).
Limits: a view can lag ~1 s, and during a snapshot (rename, then log cleared) one read can miss
lines; the next read is right. On another machine than the nodes this panel is off.

### As built (§11)
Built as in §11 (`DiskReader`, `disk_json`, `/api/disk`, hints in `/api/key`; 4 more tests, 31
dashboard tests). `DiskReader.read` must run in a thread with no event loop (it uses
`asyncio.run(storage.close())` on the copy); the HTTP handler threads are such threads. Checked
live on the VM: kill node3 → put `demo:hint` → node1 (stand-in) shows "hint for node3", node3
stores nothing → start node3 → ~12 s later the hint is gone and node3 stores the value.

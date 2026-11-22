# Dynamo-style distributed key-value store

A key-value store in the style of Amazon's Dynamo: consistent hashing with virtual nodes,
N/R/W quorums, vector clocks with siblings, gossip failure detection, sloppy quorum with hinted
handoff, read repair and Merkle-tree anti-entropy. Python 3.11, asyncio, gRPC, Docker Compose.

## Live demo

**https://dynamo-kv-demo.indiasouthcentral.cloudapp.azure.com**

A 5-node cluster (N=3, R=2, W=2) running on an Azure VM, with a live dashboard. Nothing to
install and no login: open the link.

What you can do on the page:

- **Watch the nodes**: each node's state, key count and heartbeat, refreshed every second.
- **Who sees whom**: every node's own opinion of every other node, from gossip.
- **Fail a node**: *Pause* it and watch the others mark it SUSPECT (~5 s); *Kill* it and watch it
  turn DEAD (~10 s); *Start* / *Resume* brings it back.
- **Write and read**: *Put*, *Get* and *Delete* a key through any node, and see which 3 nodes hold
  it on the ring, with the vector clock of every stored version.
- **Hinted handoff**: kill one of a key's replicas, put the key, and a stand-in node shows a
  hint for the dead owner; start the owner again and the hint is handed over.
- **Siblings**: kill a key's first replica, put with "write without context", start it again and
  get: both values come back.

The demo is open to everyone, so it protects itself: a node stopped from the page comes back by
itself after 2 minutes, at most 2 of the 5 nodes can be down at once, and requests are rate
limited.

How it runs: one Azure VM (Standard_B1ms, Ubuntu 24.04) runs the 5 nodes with Docker Compose;
the dashboard (`tools/dashboard.py`) talks to them over the normal client API and is served over
HTTPS by Caddy. The nodes' gRPC ports are not reachable from the internet.

Run the dashboard yourself next to a local cluster:

```
tools/cluster_local.sh start
python -m tools.dashboard --control local      # then open http://localhost:8080
```

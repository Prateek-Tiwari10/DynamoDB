#!/usr/bin/env bash
# Phase 1 demo: an acknowledged write survives kill -9.
#   put demo:1 hello -> kill -9 the node -> start it again -> get demo:1 must print hello
# Uses cluster.single.yaml (one node on localhost:50051) and a fresh temp data folder.
set -euo pipefail
cd "$(dirname "$0")/.."

# The project's .venv if there is one (the system python usually lacks grpc), else python.
if [ -x .venv/bin/python ]; then PY=${PYTHON:-.venv/bin/python}; else PY=${PYTHON:-python}; fi
"$PY" -c "import grpc, yaml" 2>/dev/null ||
    { echo "$PY cannot import grpc/yaml: run 'pip install -r requirements.txt' or set PYTHON=..." >&2; exit 3; }
WORK=$(mktemp -d)
export NODE_ID=node1 CONFIG=cluster.single.yaml DATA_DIR="$WORK/data" KV_CONTEXT="$WORK/ctx.json"
PID=""

cleanup() { if [ -n "$PID" ]; then kill "$PID" 2>/dev/null || true; fi; }
trap cleanup EXIT

start_node() {
    "$PY" -m dynamo.server 2>>"$WORK/node.log" &
    PID=$!
    for _ in $(seq 50); do
        if "$PY" -m client.cli info >/dev/null 2>&1; then return; fi
        sleep 0.1
    done
    echo "node did not start, log: $WORK/node.log"
    exit 1
}

echo "1. start node1";            start_node
echo "2. kv put demo:1 hello";    "$PY" -m client.cli put demo:1 hello
echo "3. kill -9 node1";          kill -9 "$PID"; wait "$PID" 2>/dev/null || true; PID=""
echo "4. start node1 again";      start_node
echo "5. kv get demo:1";          GOT=$("$PY" -m client.cli get demo:1)
echo "   -> $GOT"

if [ "$GOT" = "hello" ]; then
    echo "PHASE 1 DEMO PASSED"
else
    echo "PHASE 1 DEMO FAILED: expected 'hello', got '$GOT'"
    exit 1
fi

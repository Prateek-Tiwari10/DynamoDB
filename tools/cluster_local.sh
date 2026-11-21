#!/usr/bin/env bash
# Run the 5-node cluster as local processes, without Docker.
# Uses cluster.local.yaml (node1..node5 on localhost:50051..50055).
#
#   tools/cluster_local.sh start [NODE...]   start all nodes, or only the ones named
#   tools/cluster_local.sh stop  [NODE...]   clean stop (SIGTERM)
#   tools/cluster_local.sh kill  NODE...     crash (SIGKILL)
#   tools/cluster_local.sh status
#
# Data in $DATA_DIR/<node_id>/ (default ./data), log in $DATA_DIR/<node_id>.log,
# process id in $DATA_DIR/<node_id>.pid.
set -euo pipefail
cd "$(dirname "$0")/.."

# The project's .venv if there is one (the system python usually lacks grpc), else python.
if [ -x .venv/bin/python ]; then PY=${PYTHON:-.venv/bin/python}; else PY=${PYTHON:-python}; fi
"$PY" -c "import grpc, yaml" 2>/dev/null ||
    { echo "$PY cannot import grpc/yaml: run 'pip install -r requirements.txt' or set PYTHON=..." >&2; exit 3; }
DATA=${DATA_DIR:-./data}
ALL_NODES="node1 node2 node3 node4 node5"
mkdir -p "$DATA"

pid_of() { cat "$DATA/$1.pid" 2>/dev/null || true; }

running() {
    local pid
    pid=$(pid_of "$1")
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

start_node() {
    if running "$1"; then
        echo "$1 already running (pid $(pid_of "$1"))"
        return
    fi
    NODE_ID="$1" CONFIG=cluster.local.yaml DATA_DIR="$DATA" PYTHONUNBUFFERED=1 \
        "$PY" -m dynamo.server >>"$DATA/$1.log" 2>&1 &
    echo $! >"$DATA/$1.pid"
    echo "$1 started (pid $!, log $DATA/$1.log)"
}

signal_node() {  # signal_node NODE TERM|KILL
    if ! running "$1"; then
        echo "$1 not running"
        rm -f "$DATA/$1.pid"
        return
    fi
    kill -"$2" "$(pid_of "$1")"
    for _ in $(seq 50); do  # wait up to 5 s for the process to end
        running "$1" || break
        sleep 0.1
    done
    rm -f "$DATA/$1.pid"
    if [ "$2" = KILL ]; then echo "$1 killed"; else echo "$1 stopped"; fi
}

command=${1:-}
shift || true
nodes=${*:-$ALL_NODES}

case "$command" in
    start)  for n in $nodes; do start_node "$n"; done ;;
    stop)   for n in $nodes; do signal_node "$n" TERM; done ;;
    kill)   [ $# -gt 0 ] || { echo "usage: $0 kill NODE..."; exit 1; }
            for n in $nodes; do signal_node "$n" KILL; done ;;
    status) for n in $ALL_NODES; do
                if running "$n"; then echo "$n running (pid $(pid_of "$n"))"; else echo "$n stopped"; fi
            done ;;
    *)      echo "usage: $0 start|stop|kill|status [NODE...]"; exit 1 ;;
esac

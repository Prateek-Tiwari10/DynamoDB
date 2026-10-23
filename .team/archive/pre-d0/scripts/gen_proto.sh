#!/usr/bin/env bash
set -euo pipefail
python -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. proto/dynamo/dynamo.proto

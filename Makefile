PYTHON ?= python
CONFIG ?= cluster.local.yaml

.PHONY: proto test run up down cluster-test chaos

proto:
	$(PYTHON) -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. proto/dynamo/dynamo.proto

test:
	$(MAKE) proto && $(PYTHON) -m pytest -q

run:
	@if [ -z "$(NODE)" ]; then echo "usage: make run NODE=node1"; exit 1; fi
	NODE_ID=$(NODE) CONFIG=$(CONFIG) $(PYTHON) -m dynamo.server

up:
	docker compose up -d --build

down:
	docker compose down

# Tests against the real 5-node cluster (local processes; CLUSTER_TARGET=docker for Docker).
cluster-test:
	RUN_CLUSTER=1 $(PYTHON) -m pytest -q tests/cluster

# 5-minute chaos run on the real cluster; report in results/.
chaos:
	$(PYTHON) -m tools.chaos --seconds $${CHAOS_SECONDS:-300}

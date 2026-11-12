PYTHON ?= python

.PHONY: proto test run up down

proto:
	$(PYTHON) -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. proto/dynamo/dynamo.proto

test:
	$(MAKE) proto && $(PYTHON) -m pytest -q

run:
	@if [ -z "$(NODE)" ]; then echo "usage: make run NODE=node1"; exit 1; fi
	NODE_ID=$(NODE) CONFIG=cluster.local.yaml $(PYTHON) -m dynamo.server

up:
	docker compose up -d --build

down:
	docker compose down

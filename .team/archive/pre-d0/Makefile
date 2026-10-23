.PHONY: install proto test run up down demo

install:
	pip install -r requirements.txt && pip install -e .

proto:
	bash scripts/gen_proto.sh

test:
	$(MAKE) proto && pytest -v tests/

run:
	python -m dynamo.server --node-id node1 --port 50051 --data-dir ./data

up:
	docker compose up -d --build

down:
	docker compose down

demo:
	bash scripts/demo_phase1.sh

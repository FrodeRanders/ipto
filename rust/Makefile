SHELL := /bin/sh

export IPTO_PG_HOST ?= localhost
export IPTO_PG_PORT ?= 5432
export IPTO_PG_USER ?= repo
export IPTO_PG_PASSWORD ?= repo
export IPTO_PG_DATABASE ?= repo
export IPTO_NEO4J_HTTP_PORT ?= 7474
export IPTO_NEO4J_BOLT_PORT ?= 7687
export IPTO_NEO4J_URL ?= http://localhost:$(IPTO_NEO4J_HTTP_PORT)
export IPTO_NEO4J_DATABASE ?= neo4j
export IPTO_NEO4J_USER ?= neo4j
export IPTO_NEO4J_PASSWORD ?= neo4jtest
export IPTO_NEO4J_AUTH ?= neo4j/neo4jtest

.PHONY: test pg-up pg-down pg-logs pg-test pg-test-integration neo4j-test integration-all py-dev py-install-dev py-test full-check real-tests real-tests-down py-bench

test:
	cargo test

pg-up:
	docker compose up -d

pg-down:
	docker compose down -v

pg-logs:
	docker compose logs -f postgres

pg-test:
	IPTO_PG_INTEGRATION=1 cargo test postgres_mvp_crud_and_links -- --nocapture

pg-test-integration: pg-up pg-test

neo4j-test:
	IPTO_NEO4J_INTEGRATION=1 cargo test neo4j_mvp_metadata_and_health -- --nocapture

integration-all:
	IPTO_PG_INTEGRATION=1 IPTO_NEO4J_INTEGRATION=1 cargo test -- --nocapture

py-dev:
	maturin develop

py-install-dev:
	python3 -m pip install -r requirements-dev.txt

py-test: py-install-dev py-dev
	python3 -m pytest -q python_tests/test_ipto.py

full-check:
	@set -e; \
	rc=0; \
	docker compose up -d; \
	IPTO_PG_INTEGRATION=1 IPTO_NEO4J_INTEGRATION=1 cargo test -- --nocapture || rc=$$?; \
	if [ $$rc -eq 0 ]; then python3 -m pip install -r requirements-dev.txt || rc=$$?; fi; \
	if [ $$rc -eq 0 ]; then maturin develop || rc=$$?; fi; \
	if [ $$rc -eq 0 ]; then IPTO_PG_INTEGRATION=1 IPTO_NEO4J_INTEGRATION=1 python3 -m pytest -q python_tests/test_ipto.py || rc=$$?; fi; \
	docker compose down -v; \
	exit $$rc

real-tests:
	./scripts/run_real_backend_tests.sh

real-tests-down:
	./scripts/run_real_backend_tests.sh --down

py-bench:
	python3 scripts/python_backend_benchmark.py

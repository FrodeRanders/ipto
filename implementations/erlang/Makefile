.PHONY: test compile clean deps pg-up pg-down neo4j-up neo4j-down up down \
        test-pg test-neo4j test-all test-graphql full-check

compile:
	rebar3 compile

deps:
	rebar3 get-deps

clean:
	rebar3 clean

test: compile
	rebar3 as test eunit

test-graphql:
	rebar3 as graphql,test eunit

pg-up:
	docker compose up -d postgres

pg-down:
	docker compose down -v

neo4j-up:
	docker compose up -d neo4j

neo4j-down:
	docker compose down -v

up:
	docker compose up -d

down:
	docker compose down -v

test-pg: pg-up
	IPTO_PG_INTEGRATION=1 rebar3 as pg,test eunit --module=ipto_pg_integration_tests
	$(MAKE) pg-down

test-neo4j: neo4j-up
	IPTO_NEO4J_INTEGRATION=1 rebar3 as test eunit --module=ipto_neo4j_integration_tests
	$(MAKE) neo4j-down

test-all: up
	IPTO_PG_INTEGRATION=1 IPTO_NEO4J_INTEGRATION=1 rebar3 as pg,test eunit
	$(MAKE) down

full-check:
	@echo "=== Unit tests ==="
	$(MAKE) test
	@echo "=== GraphQL tests ==="
	$(MAKE) test-graphql
	@echo "=== Integration tests ==="
	$(MAKE) up
	IPTO_PG_INTEGRATION=1 IPTO_NEO4J_INTEGRATION=1 rebar3 as pg,test eunit \
		--module=ipto_pg_integration_tests \
		--module=ipto_neo4j_integration_tests
	$(MAKE) down
	@echo "=== All checks passed ==="

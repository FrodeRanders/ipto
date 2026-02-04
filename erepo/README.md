# erepo

`erepo` is an OTP scaffold intended to mirror the core behavior of `repo`.

## Current status

- Project structure and module boundaries are in place.
- Public facade API exists in `src/erepo.erl`.
- In-memory persistence exists in `src/erepo_db.erl` via `erepo_cache`.
- PostgreSQL mode is wired as a best-effort implementation through `epgsql` (if available at runtime).
- Search parser supports a minimal query-string syntax for PostgreSQL search.
- Unit write/read in PostgreSQL mode now prefers stored procedures and falls back to direct SQL if procedure calls fail.

## Intended migration path

1. Expand PostgreSQL search coverage (attributes/relations) to match Java parity.
2. Add full JSON attribute persistence for unit writes.
3. Expand integration tests for status/version/attribute semantics.

## Module map

- `erepo` facade API
- `erepo_repo` orchestration
- `erepo_unit` unit aggregate helpers
- `erepo_attr` attribute lifecycle hook
- `erepo_search` / `erepo_search_parser` search entry points
- `erepo_rel`, `erepo_assoc`, `erepo_lock` relationship primitives
- `erepo_db` persistence boundary
- `erepo_backend` backend behavior contract
- `erepo_db_memory` in-memory backend adapter
- `erepo_db_pg` PostgreSQL backend adapter
- `erepo_db_neo4j` Neo4j backend adapter
- `erepo_cache` latest-unit cache placeholder
- `erepo_event` event fan-out placeholder
- `erepo_graphql` GraphQL facade
- `erepo_graphql_adapter` `graphql_erl` adapter and API discovery
- `erepo_graphql_schema` SDL definition for repo primitives
- `erepo_graphql_resolvers` resolver mapping to `erepo` operations

## Running tests without rebar3

```sh
mkdir -p ebin
erlc -W -I include -o ebin src/*.erl test/*.erl
erl -noshell -pa ebin -eval 'eunit:test(erepo_smoke_tests, [verbose]), halt().'
```

## Running with rebar3

```sh
rebar3 compile
rebar3 eunit
```

GraphQL profile (fetches `graphql-erlang` from GitHub and runs graphql-enabled tests):

```sh
rebar3 as graphql eunit
```

HTTP profile (fetches Cowboy):

```sh
rebar3 as http compile
```

Combined GraphQL + HTTP test run:

```sh
rebar3 as graphql,http eunit
```

Dedicated combined profile (same intent, one profile name):

```sh
rebar3 as graphql_http eunit
```

## Enabling PostgreSQL backend

Set app env and provide PostgreSQL connection env vars:

- `backend = pg` in app env (`application:set_env(erepo, backend, pg)`).
- `EREPO_PG_HOST`, `EREPO_PG_PORT`, `EREPO_PG_USER`, `EREPO_PG_PASSWORD`, `EREPO_PG_DATABASE`.

Also ensure `epgsql` is present in the Erlang code path at runtime.

Backend options currently recognized:

- `memory` (default)
- `pg` / `postgres`
- `neo4j` (implemented backend surface via Neo4j HTTP Cypher endpoint)

Neo4j connection env vars:

- `EREPO_NEO4J_URL` (default `http://localhost:7474`)
- `EREPO_NEO4J_DATABASE` (default `neo4j`)
- `EREPO_NEO4J_USER` (default `neo4j`)
- `EREPO_NEO4J_PASSWORD` (default `neo4j`)
- `EREPO_NEO4J_TIMEOUT_MS` (default `5000`)
- `EREPO_NEO4J_CONNECT_TIMEOUT_MS` (default `2000`)
- `EREPO_NEO4J_RETRIES` (default `2`, meaning up to 3 attempts total)
- `EREPO_NEO4J_RETRY_BASE_MS` (default `100`, exponential backoff base)
- `EREPO_NEO4J_BOOTSTRAP` (default `true`, creates constraints/indexes with `IF NOT EXISTS`)
- `EREPO_NEO4J_SSL_VERIFY` (default `false`; set `true` to enforce TLS cert verification)

For GraphQL support, ensure `graphql_erl` is present in the Erlang code path.

SDL configuration/initialization options:

- App env `graphql_schema_sdl` (inline SDL binary/string)
- App env `graphql_schema_file` (path to SDL file)
- Env var `EREPO_GRAPHQL_SCHEMA_FILE` (fallback path)

At runtime you can reload schema/config after changing overrides:

```erlang
ok = erepo_graphql:reload_schema().
```

## Runtime startup defaults

`erepo` now supports startup defaults through app env (see `src/erepo.app.src`):

- `http_enabled` (default `false`)
- `http_port` (default `8080`)

You can override with env vars:

- `EREPO_HTTP_ENABLED=true|false|1|0|yes|no`
- `EREPO_HTTP_PORT=8080`

When `http_enabled` is true, `erepo_app` starts Cowboy and exposes:

- `POST /graphql`
- `GET /health`

HTTP lifecycle is managed by supervised worker `erepo_http_server`, which makes upgrade behavior predictable.
You can force listener reconciliation with current env/app config:

```erlang
ok = erepo_http_server:refresh().
```

## Hot upgrade / release handling

See `doc/HOT_UPGRADE.md` for the full release-based upgrade and rollback procedure (`.appup` / `.relup`).
For a full runtime release package (PG + GraphQL + HTTP), use:

```sh
rebar3 as runtime release
```

Backend module contract and Neo4j scaffold notes are documented in `doc/BACKEND_CONTRACT.md`.

Example GraphQL execution from Erlang:

```erlang
{ok, Result} =
    erepo_graphql:execute(
        "mutation { createUnit(tenantid: 1, name: \"demo\") { tenantid unitid unitver } }",
        #{}
    ).
```

You can fetch it through the dedicated profile:

```sh
rebar3 as pg compile
```

Run integration tests against a live PostgreSQL instance:

```sh
EREPO_PG_INTEGRATION=1 \
EREPO_PG_HOST=localhost \
EREPO_PG_PORT=5432 \
EREPO_PG_USER=repo \
EREPO_PG_PASSWORD=repo \
EREPO_PG_DATABASE=repo \
rebar3 as pg eunit
```

Run Neo4j backend integration smoke test:

```sh
EREPO_NEO4J_INTEGRATION=1 \
EREPO_NEO4J_URL=http://localhost:7474 \
EREPO_NEO4J_DATABASE=neo4j \
EREPO_NEO4J_USER=neo4j \
EREPO_NEO4J_PASSWORD=neo4j \
rebar3 eunit
```

## Current PostgreSQL search expression format

`erepo:search_units/3` accepts:

- a map expression in `pg` mode, or
- a query string parsed by `erepo_search_parser`.

Query string fields/operators:

- `tenantid=...`
- `unitid=...`
- `status=...`
- `name=...`
- `name~...` (ILIKE pattern)
- `created>=...`
- `created<...`

Equivalent map fields:

- `tenantid` (integer)
- `unitid` (integer)
- `status` (integer)
- `name` (exact unit name)
- `name_ilike` (pattern, e.g. `\"%foo%\"`)
- `created_after`
- `created_before`

Example:

```erlang
erepo:search_units(
    "tenantid=1 and status=30 and name~\"%case%\"",
    {created, desc},
    #{limit => 50, offset => 0}
).
```

Run a local GraphQL HTTP endpoint:

```erlang
{ok, _} = erepo_http:start(#{port => 8080}).
```

Endpoints:

- `POST /graphql` with JSON body: `{\"query\": \"...\", \"variables\": {...}}`
- `GET /health`

## Setting up Neo4j as backend

```terminaloutput
➜  docker pull neo4j
Using default tag: latest
latest: Pulling from library/neo4j
0bab5b9a037a: Pull complete
6c6e74c6e4d2: Pull complete
809c4a6247c8: Pull complete
da7560e339a5: Pull complete
de7499ca303e: Pull complete
4f4fb700ef54: Pull complete
Digest: sha256:c64d8750884c95ae57441a103d64d08fdaf55265acc3af687aa8ec25aa77d0c3
Status: Downloaded newer image for neo4j:latest
docker.io/library/neo4j:latest
```
```terminaloutput
➜  mkdir data
```

```terminaloutput
➜  docker run -d --name neo4j -p7474:7474 -p7687:7687 -e NEO4J_AUTH=neo4j/neo4j-repo -v ./data:/data neo4j
3abbdf87d0137d9761e741bbf19ea6539967d65b65a4e62ad9611e9ce49db12a
```

```terminaloutput
➜  curl -i -u neo4j:neo4j-repo \
-H 'Content-Type: application/json' \
-d '{"statements":[{"statement":"RETURN 1"}]}' \
http://127.0.0.1:7474/db/neo4j/tx/commit

HTTP/1.1 200 OK
Date: Wed, 04 Feb 2026 11:57:00 GMT
Access-Control-Allow-Origin: *
Content-Type: application/json
Content-Length: 130

{"results":[{"columns":["1"],"data":[{"row":[1],"meta":[null]}]}],"errors":[],"lastBookmarks":["FB:kcwQqSBvcCitShaLaDe6hsHTwQOQ"]}
```

So running the tests with these parameters works:
```
EREPO_NEO4J_URL=http://127.0.0.1:7474
EREPO_NEO4J_DATABASE=neo4j
EREPO_NEO4J_USER=neo4j
EREPO_NEO4J_PASSWORD=neo4j-repo
EREPO_NEO4J_INTEGRATION=1
```

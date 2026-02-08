# ipto

`ipto` is an OTP scaffold intended to mirror the core behavior of `repo`.

## Current status

- Project structure and module boundaries are in place.
- Public facade API exists in `src/ipto.erl`.
- In-memory persistence exists in `src/ipto_db.erl` via `ipto_cache`.
- PostgreSQL mode is wired as a best-effort implementation through `epgsql` (if available at runtime).
- Search parser supports a minimal query-string syntax for PostgreSQL search.
- Unit write/read in PostgreSQL mode now prefers stored procedures and falls back to direct SQL if procedure calls fail.

## Intended migration path

1. Expand PostgreSQL search coverage (attributes/relations) to match Java parity.
2. Add full JSON attribute persistence for unit writes.
3. Expand integration tests for status/version/attribute semantics.

## Module map

- `ipto` facade API
- `ipto_repo` orchestration
- `ipto_unit` unit aggregate helpers
- `ipto_attr` attribute lifecycle hook
- `ipto_search` / `ipto_search_parser` search entry points
- `ipto_rel`, `ipto_assoc`, `ipto_lock` relationship primitives
- `ipto_db` persistence boundary
- `ipto_backend` backend behavior contract
- `ipto_db_memory` in-memory backend adapter
- `ipto_db_pg` PostgreSQL backend adapter
- `ipto_db_neo4j` Neo4j backend adapter
- `ipto_cache` latest-unit cache placeholder
- `ipto_event` event fan-out placeholder
- `ipto_graphql` GraphQL facade
- `ipto_graphql_adapter` `graphql_erl` adapter and API discovery
- `ipto_graphql_schema` SDL definition for repo primitives
- `ipto_graphql_resolvers` resolver mapping to `ipto` operations

## Running tests without rebar3

```sh
mkdir -p ebin
erlc -W -I include -o ebin src/*.erl test/*.erl
erl -noshell -pa ebin -eval 'eunit:test(ipto_smoke_tests, [verbose]), halt().'
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

- `backend = pg` in app env (`application:set_env(ipto, backend, pg)`).
- `IPTO_PG_HOST`, `IPTO_PG_PORT`, `IPTO_PG_USER`, `IPTO_PG_PASSWORD`, `IPTO_PG_DATABASE`.
- `IPTO_PG_POOL_SIZE` (optional; defaults to number of online schedulers, minimum 2)

Pool telemetry is available at runtime:

```erlang
ipto_pg_pool:get_stats().
```

Also ensure `epgsql` is present in the Erlang code path at runtime.

Backend options currently recognized:

- `memory` (default)
- `pg` / `postgres`
- `neo4j` (implemented backend surface via Neo4j HTTP Cypher endpoint)

Neo4j connection env vars:

- `IPTO_NEO4J_URL` (default `http://localhost:7474`)
- `IPTO_NEO4J_DATABASE` (default `neo4j`)
- `IPTO_NEO4J_USER` (default `neo4j`)
- `IPTO_NEO4J_PASSWORD` (default `neo4j`)
- `IPTO_NEO4J_TIMEOUT_MS` (default `5000`)
- `IPTO_NEO4J_CONNECT_TIMEOUT_MS` (default `2000`)
- `IPTO_NEO4J_RETRIES` (default `2`, meaning up to 3 attempts total)
- `IPTO_NEO4J_RETRY_BASE_MS` (default `100`, exponential backoff base)
- `IPTO_NEO4J_BOOTSTRAP` (default `true`, creates constraints/indexes with `IF NOT EXISTS`)
- `IPTO_NEO4J_SSL_VERIFY` (default `false`; set `true` to enforce TLS cert verification)

For GraphQL support, ensure `graphql_erl` is present in the Erlang code path.

SDL configuration/initialization options:

- App env `graphql_schema_sdl` (inline SDL binary/string)
- App env `graphql_schema_file` (path to SDL file)
- Env var `IPTO_GRAPHQL_SCHEMA_FILE` (fallback path)

At runtime you can reload schema/config after changing overrides:

```erlang
ok = ipto_graphql:reload_schema().
```

## Runtime startup defaults

`ipto` now supports startup defaults through app env (see `src/ipto.app.src`):

- `http_enabled` (default `false`)
- `http_port` (default `8080`)
- `log_level` (default `info`)
- `log_file` (default `undefined`, disabled)
- `log_file_level` (default `info`)

You can override with env vars:

- `IPTO_HTTP_ENABLED=true|false|1|0|yes|no`
- `IPTO_HTTP_PORT=8080`
- `IPTO_LOG_LEVEL=debug|info|notice|warning|error`
- `IPTO_LOG_FILE=/path/to/ipto.log`
- `IPTO_LOG_FILE_LEVEL=debug|info|notice|warning|error`

When `http_enabled` is true, `ipto_app` starts Cowboy and exposes:

- `POST /graphql`
- `GET /health`

HTTP lifecycle is managed by supervised worker `ipto_http_server`, which makes upgrade behavior predictable.
You can force listener reconciliation with current env/app config:

```erlang
ok = ipto_http_server:refresh().
```

Logging uses OTP `logger` via `ipto_log`. Problems are logged as warnings/errors and major lifecycle events are logged at notice level.

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
    ipto_graphql:execute(
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
IPTO_PG_INTEGRATION=1 \
IPTO_PG_HOST=localhost \
IPTO_PG_PORT=5432 \
IPTO_PG_USER=repo \
IPTO_PG_PASSWORD=repo \
IPTO_PG_DATABASE=repo \
rebar3 as pg eunit
```

Run Neo4j backend integration smoke test:

```sh
IPTO_NEO4J_INTEGRATION=1 \
IPTO_NEO4J_URL=http://localhost:7474 \
IPTO_NEO4J_DATABASE=neo4j \
IPTO_NEO4J_USER=neo4j \
IPTO_NEO4J_PASSWORD=neo4j \
rebar3 eunit
```

## Current PostgreSQL search expression format

`ipto:search_units/3` accepts:

- a map expression in `pg` mode, or
- a query string parsed by `ipto_search_parser`.

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
ipto:search_units(
    "tenantid=1 and status=30 and name~\"%case%\"",
    {created, desc},
    #{limit => 50, offset => 0}
).
```

Run a local GraphQL HTTP endpoint:

```erlang
{ok, _} = ipto_http:start(#{port => 8080}).
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
IPTO_NEO4J_URL=http://127.0.0.1:7474
IPTO_NEO4J_DATABASE=neo4j
IPTO_NEO4J_USER=neo4j
IPTO_NEO4J_PASSWORD=neo4j-repo
IPTO_NEO4J_INTEGRATION=1
```

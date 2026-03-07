# ipto

`ipto` is an OTP scaffold intended to mirror the core behavior of `repo`.

## Current status

- Project structure and module boundaries are in place.
- Public facade API exists in `src/ipto.erl`.
- In-memory persistence exists in `src/ipto_db.erl` via `ipto_cache`.
- PostgreSQL mode is wired as a best-effort implementation through `epgsql` (if available at runtime).
- Search parser supports a minimal query-string syntax for PostgreSQL search.
- Unit write/read in PostgreSQL mode now prefers stored procedures and falls back to direct SQL if procedure calls fail.

Detailed parity scope and roadmap are tracked in `FEATURE_COMPLETENESS.md`.

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
- `ipto_graphql_sdl` SDL inspection/validation parser for setup configuration
- `ipto_graphql_operations` operation registration metadata and allowlist filtering
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
- App env `graphql_operation_allowlist` (optional list of allowed operation names for registration metadata)

At runtime you can reload schema/config after changing overrides:

```erlang
ok = ipto_graphql:reload_schema().
```

SDL inspection/setup APIs (current first step):

```erlang
Catalog = ipto:inspect_graphql_sdl(SdlBin),
{ok, Summary} = ipto:configure_graphql_sdl(SdlBin).

%% file convenience
{ok, Summary2} = ipto:configure_graphql_sdl_file("/path/to/schema.graphqls").
```

GraphQL setup operations are also exposed:

- Query: `inspectGraphqlSdl(sdl: String!): SdlCatalog!`
- Query: `registeredOperations: RegisteredOperations!` (`queries/mutations` as operation-name entries)
- Query: `unitByCorrid(corrid: String!, tenantid: Int): Unit`
- Query: `unitExists(tenantid: Int!, unitid: Int!): Boolean!`
- Query: `unitLocked(tenantid: Int!, unitid: Int!): Boolean!`
- Mutation: `configureGraphqlSdl(sdl: String!): SdlConfigureResult!`
- Mutation: `configureGraphqlSdlFile(path: String!): SdlConfigureResult!`
- Query: `units(query: String, orderField: String, orderDir: String, limit: Int, offset: Int): SearchResult!`
- Query: `unitsByExpression(tenantid: Int, unitid: Int, status: Int, name: String, nameLike: String, corrid: String, orderField: String, orderDir: String, limit: Int, offset: Int): SearchResult!`
- Query: `rightRelation(tenantid: Int!, unitid: Int!, reltype: Int!): Relation`
- Query: `rightRelations(tenantid: Int!, unitid: Int!, reltype: Int!): [Relation!]!`
- Query: `leftRelations(tenantid: Int!, unitid: Int!, reltype: Int!): [Relation!]!`
- Query: `countRightRelations(tenantid: Int!, unitid: Int!, reltype: Int!): Int!`
- Query: `countLeftRelations(tenantid: Int!, unitid: Int!, reltype: Int!): Int!`
- Query: `rightAssociation(tenantid: Int!, unitid: Int!, assoctype: Int!): Association`
- Query: `rightAssociations(tenantid: Int!, unitid: Int!, assoctype: Int!): [Association!]!`
- Query: `leftAssociations(assoctype: Int!, assocstring: String!): [Association!]!`
- Query: `countRightAssociations(tenantid: Int!, unitid: Int!, assoctype: Int!): Int!`
- Query: `countLeftAssociations(assoctype: Int!, assocstring: String!): Int!`
- Query: `tenantInfo(id: Int, name: String): TenantInfo`
- Query: `attributeInfo(id: Int, name: String): AttributeInfo`
- Mutation: `lockUnit(tenantid: Int!, unitid: Int!, locktype: Int!, purpose: String!): Boolean!`
- Mutation: `unlockUnit(tenantid: Int!, unitid: Int!): Boolean!`
- Mutation: `requestStatusTransition(tenantid: Int!, unitid: Int!, status: Int!): StatusTransitionResult!`
- Mutation: `transitionUnitStatus(tenantid: Int!, unitid: Int!, status: Int!): StatusTransitionResult!`
- Mutation: `addRelation(tenantid: Int!, unitid: Int!, reltype: Int!, reltenantid: Int!, relunitid: Int!): Boolean!`
- Mutation: `removeRelation(tenantid: Int!, unitid: Int!, reltype: Int!, reltenantid: Int!, relunitid: Int!): Boolean!`
- Mutation: `addAssociation(tenantid: Int!, unitid: Int!, assoctype: Int!, assocstring: String!): Boolean!`
- Mutation: `removeAssociation(tenantid: Int!, unitid: Int!, assoctype: Int!, assocstring: String!): Boolean!`

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
IPTO_NEO4J_PASSWORD=neo4j-repo \
rebar3 eunit
```

Run backend parity search corpus (same query corpus totals across PG and Neo4j):

```sh
IPTO_BACKEND_PARITY=1 \
IPTO_PG_INTEGRATION=1 \
IPTO_NEO4J_INTEGRATION=1 \
IPTO_PG_HOST=localhost \
IPTO_PG_PORT=5432 \
IPTO_PG_USER=repo \
IPTO_PG_PASSWORD=repo \
IPTO_PG_DATABASE=repo \
IPTO_NEO4J_URL=http://localhost:7474 \
IPTO_NEO4J_DATABASE=neo4j \
IPTO_NEO4J_USER=neo4j \
IPTO_NEO4J_PASSWORD=neo4j-repo \
rebar3 as pg eunit
```

## Current search expression format

`ipto:search_units/3` accepts:

- a map expression in `pg` mode, or
- a query string parsed by `ipto_search_parser`.

Query string supports:

- Boolean composition: `and`, `or`, `not`, parenthesis grouping.
- Comparison: `=`, `!=`, `<>`, `>`, `>=`, `<`, `<=`.
- Pattern: `~`, `like`.
- Set/range: `in (...)`, `not in (...)`, `between ... and ...`.
- Field aliases: `tenant_id`, `unit_id`, `corr_id|correlationid`, `unitname|unit_name|name`.
- Status literals: `PENDING_DISPOSITION`, `PENDING_DELETION`, `OBLITERATED`, `EFFECTIVE`, `ARCHIVED`.

Equivalent map fields include (non-exhaustive):

- `tenantid` (integer)
- `unitid` (integer)
- `status` (integer)
- `name` (exact unit name)
- `name_ne`
- `name_ilike` (pattern, e.g. `\"%foo%\"`)
- `corrid`
- `corrid_ne`
- `corrid_ilike`
- `tenantid_ne|gt|gte|lt|lte`
- `unitid_ne|gt|gte|lt|lte`
- `status_ne|gt|gte|lt|lte`
- `created`, `created_ne|gt|gte|lt|lte`
- `created_after`
- `created_before`
- `modified`, `modified_ne|gt|gte|lt|lte`
- `modified_after`
- `modified_before`

Example:

```erlang
ipto:search_units(
    "tenantid=1 and (status in (30, 40)) and not name=\"obsolete\" and unitid between 1 and 999999",
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

Use the helper script (start, wait-for-ready, stop, logs):

```sh
./scripts/setup-neo4j-for-test.sh start
./scripts/setup-neo4j-for-test.sh status
./scripts/setup-neo4j-for-test.sh logs
./scripts/setup-neo4j-for-test.sh stop
```

For local Docker runs, use this integration env:

```sh
IPTO_NEO4J_URL=http://127.0.0.1:7474
IPTO_NEO4J_DATABASE=neo4j
IPTO_NEO4J_USER=neo4j
IPTO_NEO4J_PASSWORD=neo4j-repo
IPTO_NEO4J_INTEGRATION=1
```

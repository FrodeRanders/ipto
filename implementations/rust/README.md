# Rust IPTO scaffold (`./implementations/rust`)

This directory is a `maturin`-ready Rust baseline for implementing IPTO `repo` and parts of `graphql`, with backend adapters for PostgreSQL and Neo4j.

## Feasibility assessment

Short answer: yes, this is realistic, but full Java parity is a substantial project.

What is straightforward:
- Mirror Erlang's backend contract as a Rust trait and keep orchestration backend-agnostic.
- Expose a stable Python API via `pyo3` + `maturin`.
- Implement PostgreSQL persistence first, then Neo4j.
- Reuse existing GraphQL SDL conventions from `Specification.graphql` and `implementations/java/graphql/`.

What is harder:
- Reaching feature parity with Java search/query semantics and dynamic GraphQL runtime wiring.
- Matching all edge cases around versioning, locking, relation/association cardinality, and status transitions.
- Cross-backend consistency (PostgreSQL vs Neo4j) for query behavior and paging.

## Recommended phased plan

1. `repo-core` parity subset:
- Unit create/get/store
- version selector (`latest` / explicit version)
- status updates
- lock/unlock

2. PostgreSQL backend MVP:
- map unit JSON model to existing SQL schema and procedures in `shared/db/postgresql`
- implement search for current Erlang subset first

3. Neo4j backend MVP:
- implement same backend trait against Cypher
- preserve the same external API shape

4. Python API stabilization:
- freeze exception mapping and return shapes
- add integration tests from Python for both backends

5. GraphQL layer:
- start with a minimal runtime surface for unit CRUD/search
- optionally introduce dynamic SDL-driven wiring after core parity is stable

## Package layout

- `src/lib.rs`: `pyo3` module entry point and Python-visible facade.
- `src/model.rs`: shared domain model types.
- `src/backend.rs`: backend trait and shared types.
- `src/repo.rs`: backend-agnostic orchestration service.
- `src/backends/postgres.rs`: PostgreSQL adapter scaffold.
- `src/backends/neo4j.rs`: Neo4j adapter scaffold.
- `src/graphql.rs`: GraphQL runtime bridge (MVP supports `searchUnits` query execution).

## Build (local)

```sh
cd implementations/rust
maturin develop
```

or

```sh
cd implementations/rust
maturin build
```

## Note

This scaffold intentionally does not claim parity yet. It defines the API and extension points so implementation can proceed iteratively while preserving a stable Python module shape.

Rust-only test/build (without Python linking):

```sh
cd implementations/rust
cargo test
```

## PostgreSQL MVP status

Implemented in `PostgresBackend`:
- `health`
- `unit_exists`
- `get_unit_json` via `CALL repo.extract_unit_json(...)`
- `store_unit_json` via `CALL repo.ingest_new_unit_json(...)` and `CALL repo.ingest_new_version_json(...)`
- `search_units` (current unit-version header search subset), including:
  - `tenantid`, `unitid`, `status`, `corrid`
  - `name`, `name_ilike`
  - `created_after`/`created_before`, `modified_after`/`modified_before`
  - relation constraints: `relation_type`, `related_tenantid`, `related_unitid`
  - association constraints: `association_type`, `association_reference`
  - attribute equality constraint: `attribute_eq` with selector (`name_or_id`/`attrid`/`name`) and `value`
  - typed attribute comparison constraint: `attribute_cmp` with selector (`name_or_id`/`attrid`/`name`), `op` (`eq|gt|gte|lt|lte`), `value`, and optional `value_type` (`string|number|boolean|time`)
  - string attribute comparisons are normalized to lower-case (Java-style case-insensitive behavior)
  - string equality supports wildcard patterns (`*` normalized to `%`, `%`/`_` pattern matching)
  - set-operation composition in `RepoService`: recursive `and`/`or`/`not` over leaf expressions
  - `not` is currently tenant-scoped (requires direct tenant leaf or tenant derivable from child expression)
  - timestamp filters accept RFC3339 strings and epoch-millis
- `set_status`
- relation API: add/remove/get/count (left and right)
- association API: add/remove/get/count (left and right)
- lock API: lock/unlock
- metadata API: `create_attribute`, `get_attribute_info`, `get_tenant_info`

Environment variables (same naming convention as Erlang):
- `IPTO_PG_HOST` (default `localhost`)
- `IPTO_PG_PORT` (default `5432`)
- `IPTO_PG_USER` (default `repo`)
- `IPTO_PG_PASSWORD` (default `repo`)
- `IPTO_PG_DATABASE` (default `repo`)

Neo4j environment variables:
- `IPTO_NEO4J_URL` (default `http://localhost:7474`)
- `IPTO_NEO4J_DATABASE` (default `neo4j`)
- `IPTO_NEO4J_USER` (default `neo4j`)
- `IPTO_NEO4J_PASSWORD` (default `neo4j`)
- `IPTO_NEO4J_BOOTSTRAP` (default `true`; set `false` to skip automatic constraints/indexes bootstrap)

Current Python methods on `PyIpto`:
- `health() -> str` (JSON string)
- `graphql_execute(query, variables_json=None) -> str` (GraphQL JSON envelope as string)
- `graphql_execute_allowlist(query, allowlist, variables_json=None) -> str` (GraphQL execute with explicit operation allowlist)
- `reset_statistics() -> None` (clear per-operation runtime stats for this client instance)
- `get_statistics() -> dict` (per-operation count/error/latency summary as Python dictionary)
- `get_statistics_json() -> str` (per-operation count/error/latency summary as JSON)
- `flush_cache() -> None` (cache-control parity surface; currently a no-op for stateless backends)
- `unit_exists(tenant_id, unit_id) -> bool`
- `get_unit_json(tenant_id, unit_id, version=None) -> Optional[str]`
- `get_unit_by_corrid_json(corrid) -> Optional[str]`
- `store_unit_json(unit_json: str) -> str`
- `search_units(expression_json, order_field, descending, limit, offset) -> str`
- `set_status(tenant_id, unit_id, status) -> None`
- `add_relation(tenant_id, unit_id, relation_type, other_tenant_id, other_unit_id) -> None`
- `remove_relation(tenant_id, unit_id, relation_type, other_tenant_id, other_unit_id) -> None`
- `add_association(tenant_id, unit_id, association_type, reference) -> None`
- `remove_association(tenant_id, unit_id, association_type, reference) -> None`
- `lock_unit(tenant_id, unit_id, lock_type, purpose) -> None`
- `unlock_unit(tenant_id, unit_id) -> None`
- `is_unit_locked(tenant_id, unit_id) -> bool`
- `activate_unit(tenant_id, unit_id) -> None`
- `inactivate_unit(tenant_id, unit_id) -> None`
- `request_status_transition(tenant_id, unit_id, requested_status) -> int`
- `get_right_relation(tenant_id, unit_id, relation_type) -> Optional[str]`
- `get_right_relations(tenant_id, unit_id, relation_type) -> str`
- `get_left_relations(tenant_id, unit_id, relation_type) -> str`
- `count_right_relations(tenant_id, unit_id, relation_type) -> int`
- `count_left_relations(tenant_id, unit_id, relation_type) -> int`
- `get_right_association(tenant_id, unit_id, association_type) -> Optional[str]`
- `get_right_associations(tenant_id, unit_id, association_type) -> str`
- `get_left_associations(association_type, reference) -> str`
- `count_right_associations(tenant_id, unit_id, association_type) -> int`
- `count_left_associations(association_type, reference) -> int`
- `create_attribute(alias, name, qualname, attribute_type, is_array=False) -> str`
- `instantiate_attribute(name_or_id) -> Optional[str]`
- `can_change_attribute(name_or_id) -> bool`
- `get_attribute_info(name_or_id) -> Optional[str]`
- `get_tenant_info(name_or_id) -> Optional[str]`
- `inspect_graphql_sdl(sdl) -> str` (parse SDL; returns attribute/record/template catalog + counts)
- `configure_graphql_sdl(sdl) -> str` (apply attribute setup from SDL, validate record/template references, and persist template shapes where backend supports it)
- `configure_graphql_sdl_file(path) -> str` (same as above, reading SDL from file)
- `attribute_name_to_id(attribute_name) -> Optional[int]`
- `attribute_id_to_name(attribute_id) -> Optional[str]`
- `tenant_name_to_id(tenant_name) -> Optional[int]`
- `tenant_id_to_name(tenant_id) -> Optional[str]`

Note: `can_change_attribute` is usage-aware in both backends.
For Neo4j, usage is tracked from stored unit payload `attributes` entries (`attrid`/`attrname`) via `(:UnitVersion)-[:USES_ATTRIBUTE]->(:Attribute)`.

Status semantics policy:
- `request_status_transition` uses a strict transition matrix (Java `requestStatusTransition` semantics).
- `activate_unit` and `inactivate_unit` are lifecycle helpers and may apply direct transitions by design.

Integration tests are opt-in and require a running PostgreSQL with schema/procedures loaded:

```sh
cd implementations/rust
IPTO_PG_INTEGRATION=1 cargo test postgres_mvp_crud_and_links -- --nocapture
```

Quick local stack and commands:

```sh
cd implementations/rust
make pg-up
make pg-test
make pg-down
```

Note: Rust Docker PostgreSQL initialization mounts SQL directly from
`shared/db/postgresql/schema.sql` and `shared/db/postgresql/procedures.sql`.
There are no Rust-local schema copies to keep in sync.

Neo4j MVP currently implemented:
- `health`, `unit_exists`, `set_status`
- unit API: `store_unit_json`, `get_unit_json`, `search_units`
- relation API: add/remove/get/count (left and right)
- association API: add/remove/get/count (left and right)
- lock API: lock/unlock
- metadata API: `create_attribute`, `get_attribute_info`, `get_tenant_info`

Neo4j integration test:

```sh
cd implementations/rust
make neo4j-test
```

GraphQL runtime MVP:
- `GraphqlRuntime::execute(query, variables)` currently supports read-only:
  - `registeredOperations`
  - `searchUnits`
  - `unitExists`
  - `isUnitLocked` (by `tenantid` + `unitid`)
  - `getUnit` (by `tenantid` + `unitid`, optional `version`)
  - `getUnitByCorrid` (by `tenantid` + `corrid`)
  - `getAttributeInfo` (by `nameOrId`)
  - `instantiateAttribute` (by `nameOrId`)
  - `getTenantInfo` (by `nameOrId`)
  - mapping reads: `attributeNameToId`, `attributeIdToName`, `tenantNameToId`, `tenantIdToName`
  - relation reads: `getRightRelation`, `getRightRelations`, `getLeftRelations`, `countRightRelations`, `countLeftRelations`
  - association reads: `getRightAssociation`, `getRightAssociations`, `getLeftAssociations`, `countRightAssociations`, `countLeftAssociations`
  - `health`
- It maps to `RepoService::search_units` and returns:
  - `{"data": {"searchUnits": {"totalHits": ..., "results": [...]}}}`
- `getUnit` returns: `{"data": {"getUnit": <unit-or-null>}}`
- `getUnitByCorrid` returns: `{"data": {"getUnitByCorrid": <unit-or-null>}}`
- `getAttributeInfo` returns: `{"data": {"getAttributeInfo": <attribute-or-null>}}`
- `getTenantInfo` returns: `{"data": {"getTenantInfo": <tenant-or-null>}}`
- Also supports scoped mutations:
  - `setStatus` with vars `tenantid`, `unitid`, `status`
  - `storeUnit` with var `unit` (object payload)
  - `createAttribute` with vars `alias`, `name`, `qualname`, `attributeType`, optional `isArray`
  - `flushCache` (no vars)
  - `activateUnit` / `inactivateUnit` with vars `tenantid`, `unitid`
  - `requestStatusTransition` with vars `tenantid`, `unitid`, `requestedStatus`
  - `lockUnit` with vars `tenantid`, `unitid`, `lockType`, `purpose`
  - `unlockUnit` with vars `tenantid`, `unitid`
  - `addRelation` / `removeRelation` with vars `tenantid`, `unitid`, `relationType`, `otherTenantId`, `otherUnitId`
  - `addAssociation` / `removeAssociation` with vars `tenantid`, `unitid`, `associationType`, `reference`
  - `canChangeAttribute` query with var `nameOrId`
- Error handling uses a GraphQL-style envelope:
  - `{"data": null, "errors": [{"message": "...", "extensions": {"code": "..."} }]}`
  - current `extensions.code` values include:
    - `INVALID_INPUT`, `UNSUPPORTED`, `NOT_FOUND`, `ALREADY_LOCKED`, `BACKEND_ERROR`
- Dispatch uses the query root field name (exact operation matching) to avoid substring collisions.
- Operations are registered through an explicit allowlist/registry:
  - default: all currently supported operations are enabled
  - optional: `GraphqlRuntime::with_operation_allowlist(repo, &[...])` constrains enabled operations
  - introspection helper: `registered_operations()` returns enabled query/mutation operation names
  - unsupported operation responses include strict metadata:
    - `extensions.operationType`, `extensions.operation`, `extensions.supportedOperations`

Run all Rust integration tests (both backends enabled):

```sh
cd implementations/rust
make integration-all
```

Use the dedicated real-backend runner script (handles env defaults, startup, readiness checks, and test selection):

```sh
cd implementations/rust
./scripts/run_real_backend_tests.sh
```

Common variants:

```sh
# Postgres only integration
./scripts/run_real_backend_tests.sh --backend postgres --scope integration

# Neo4j only parity check, keep existing containers
./scripts/run_real_backend_tests.sh --backend neo4j --scope parity --no-up

# Run everything and stop containers afterward
./scripts/run_real_backend_tests.sh --down
```

Run PyO3-backed backend soak benchmark with running statistics (store throughput + latency summary):

```sh
cd implementations/rust
python3 scripts/python_backend_benchmark.py --backend all --up --down
```

Default long-run targets:
- PostgreSQL: `10000` unit stores (use `--pg-units 500000` for a full long soak)
- Neo4j: `1000` unit stores

Useful variants:

```sh
# PostgreSQL-only long run
python3 scripts/python_backend_benchmark.py --backend postgres --pg-units 500000 --up --down

# PostgreSQL long run with 4 parallel worker processes
python3 scripts/python_backend_benchmark.py --backend postgres --pg-units 500000 --workers 4 --up --down

# Neo4j-only long run
python3 scripts/python_backend_benchmark.py --backend neo4j --neo4j-units 1000 --up --down

# Neo4j with parallel workers and transient retry handling
python3 scripts/python_backend_benchmark.py --backend neo4j --neo4j-units 1000 --workers 2 --max-retries 5 --up --down

# Short smoke run
python3 scripts/python_backend_benchmark.py --backend all --pg-units 200 --neo4j-units 50 --up --down
```

The benchmark uses the Python `ipto_rust.PyIpto` bridge (`store_unit_json`, `search_units`) so it exercises the full PyO3 path, not just direct Rust tests.
Use `--workers N` to run store operations in parallel worker processes per backend, and `--max-retries` for transient contention errors.

Run full local validation pipeline (containers + Rust integration + Python integration + cleanup):

```sh
cd implementations/rust
make full-check
```

Python integration test (after `maturin develop`):

```sh
cd implementations/rust
IPTO_PG_INTEGRATION=1 pytest -q python_tests/test_ipto.py
```

Install Python test dependencies:

```sh
cd implementations/rust
python3 -m pip install -r requirements-dev.txt
```

Run Python integration tests (module form, no PATH dependency):

```sh
cd implementations/rust
python3 -m pytest -q python_tests/test_ipto.py
```

Convenience Make targets:

```sh
cd implementations/rust
make py-install-dev
make py-test
make py-bench
```

Python GraphQL examples:

```python
import json
import ipto_rust

client = ipto_rust.PyIpto("postgres")

# 1) storeUnit mutation
store_resp = json.loads(
    client.graphql_execute(
        "mutation StoreUnit { storeUnit { tenantid unitid unitver status unitname } }",
        json.dumps(
            {
                "unit": {
                    "tenantid": 1,
                    "corrid": "00000000-0000-0000-0000-000000000001",
                    "status": 30,
                    "unitname": "example-unit",
                    "attributes": [],
                }
            }
        ),
    )
)

# 2) unitExists and isUnitLocked queries
exists_resp = json.loads(
    client.graphql_execute(
        "query UnitExists { unitExists }",
        json.dumps({"tenantid": 1, "unitid": 1}),
    )
)

locked_resp = json.loads(
    client.graphql_execute(
        "query IsUnitLocked { isUnitLocked }",
        json.dumps({"tenantid": 1, "unitid": 1}),
    )
)

# 3) createAttribute mutation
attr_resp = json.loads(
    client.graphql_execute(
        "mutation CreateAttribute { createAttribute { id alias name qualname } }",
        json.dumps(
            {
                "alias": "example_attr_alias",
                "name": "example_attr_name",
                "qualname": "example.attr.name",
                "attributeType": "string",
                "isArray": False,
            }
        ),
    )
)

# 4) allowlist-gated execution (only health allowed)
ok = json.loads(
    client.graphql_execute_allowlist(
        "query Health { health { status } }",
        ["health"],
    )
)

blocked = json.loads(
    client.graphql_execute_allowlist(
        "query UnitExists { unitExists }",
        ["health"],
        json.dumps({"tenantid": 1, "unitid": 1}),
    )
)
assert blocked["errors"][0]["extensions"]["code"] == "UNSUPPORTED"
```

Feature parity tracker:
- `FEATURE_COMPLETENESS.md` (living status against Java `repo`)

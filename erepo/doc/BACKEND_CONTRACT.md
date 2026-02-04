# erepo Backend Contract

`erepo` now routes persistence through backend modules implementing `erepo_backend`.

## Behavior module

See `src/erepo_backend.erl`.

Required callbacks:

- `get_unit_json/3`
- `unit_exists/2`
- `store_unit_json/1`
- `search_units/3`
- `add_relation/3`
- `remove_relation/3`
- `add_association/3`
- `remove_association/3`
- `lock_unit/3`
- `unlock_unit/1`
- `set_status/2`
- `create_attribute/5`
- `get_attribute_info/1`
- `get_tenant_info/1`

## Implementations

- `src/erepo_db_memory.erl`: delegates to current memory implementation.
- `src/erepo_db_pg.erl`: delegates to current PostgreSQL implementation.
- `src/erepo_db_neo4j.erl`: implements the full backend callback surface.

## Backend selection

`src/erepo_db.erl` dispatches by app env `backend`:

- `memory` -> `erepo_db_memory`
- `pg` / `postgres` -> `erepo_db_pg`
- `neo4j` -> `erepo_db_neo4j`

Example:

```erlang
application:set_env(erepo, backend, neo4j).
```

## Neo4j next steps

1. Decide if status updates should also version payload nodes (currently kernel-only, aligned with PG path).
2. Validate and tune bootstrap-created constraints/indexes for your production workload.
3. Add dedicated Neo4j integration tests for paging/order edge cases, retries, and concurrency.
4. Preserve return shapes used by upper layers (`erepo_repo`, GraphQL resources) when hardening further.

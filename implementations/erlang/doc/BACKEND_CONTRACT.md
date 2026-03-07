# ipto Backend Contract

`ipto` now routes persistence through backend modules implementing `ipto_backend`.

## Behavior module

See `src/ipto_backend.erl`.

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

- `src/ipto_db_memory.erl`: delegates to current memory implementation.
- `src/ipto_db_pg.erl`: delegates to current PostgreSQL implementation.
- `src/ipto_db_neo4j.erl`: implements the full backend callback surface.

## Backend selection

`src/ipto_db.erl` dispatches by app env `backend`:

- `memory` -> `ipto_db_memory`
- `pg` / `postgres` -> `ipto_db_pg`
- `neo4j` -> `ipto_db_neo4j`

Example:

```erlang
application:set_env(ipto, backend, neo4j).
```

# Erlang Feature Completeness Tracker

Last updated: 2026-06-11

Legend: `DONE`, `PARTIAL`, `MISSING`

## Scope and intent

This tracks Erlang parity against Java core behavior, while allowing intentional divergence where useful.

## Current status (high level)

- Repository core lifecycle (`create/get/store/version/status/lock`): `DONE`
- Relations and associations (PG + Neo4j + memory): `DONE`
- Attribute and tenant metadata lookup/create: `DONE`
- GraphQL runtime/API breadth: `DONE`
- SDL parser and setup/apply path: `DONE` (memory + PG; Neo4j template persistence unsupported by design)
- Search language/parser depth: `DONE` (full AST, DSL parser, SET_OPS/EXISTS SQL compiler, Cypher compiler)
- Integration test depth and parity matrix: `PARTIAL` (env-gated; CI file exists but not running remotely)
- CI/CD infrastructure: `PARTIAL` (Makefile, docker-compose, GH Actions workflow exist; not yet activated on remote)

## Track A: SDL parser and setup (top priority)

### A1. SDL inspection API

Status: `DONE`

- `ipto_graphql_sdl` parses SDL into normalized catalog maps.
- Uses `graphql:parse/1` from graphql-erlang for AST-based extraction of:
  - `@attributeRegistry` enums with `@attribute(datatype: ...)` values
  - `@record(attribute: ...)` type directives
  - `@template(name: ...)` type directives
  - `@use(attribute: ...)` field directives
- Validates reference integrity between records/templates and declared attributes.
- Falls back gracefully when graphql-erlang is not loaded.
- graphql-erlang pinned to `v0.17.0` (was `master` branch).

### A2. SDL apply/configure API

Status: `DONE` (memory + PostgreSQL; Neo4j returns unsupported)

- `ipto:inspect_graphql_sdl/1`, `ipto:configure_graphql_sdl/1`, `ipto:configure_graphql_sdl_file/1`
- PG implementation persists record/unit templates to shared schema tables.
- Idempotent re-apply behavior.
- Env-gated PG integration tests verify template persistence.

### A3. GraphQL exposure for setup

Status: `DONE`

- GraphQL query `inspectGraphqlSdl(sdl: String!)`.
- GraphQL mutations `configureGraphqlSdl(sdl: String!)`, `configureGraphqlSdlFile(path: String!)`.
- Typed response shapes for all SDL operations.

## Track B: Search language depth (second priority)

### B1. Parser grammar expansion

Status: `DONE`

- Boolean composition: `and`, `or`, `not`, parenthesis grouping with correct precedence (AND binds tighter than OR).
- Operators: `=`, `!=`, `<>`, `>`, `>=`, `<`, `<=`, `~`, `like`, `in`, `not in`, `between ... and ...`.
- Field aliases: `tenantid|tenant_id`, `unitid|unit_id`, `corrid|corr_id|correlationid`, `unitname|unit_name|name`.
- Status literals: `PENDING_DISPOSITION`, `PENDING_DELETION`, `OBLITERATED`, `EFFECTIVE`, `ARCHIVED`.
- Wildcard patterns: `*` → `%`, `_` → SQL wildcard (consistent with Java reference).
- **Attribute-qualified field names**: `prefix:name = value` → attribute search items.
- **Relation specs**: `relation:left:TYPE = tenantId.unitId`, `rel_right:TYPE = ref`.
- **Association specs**: `association:left:TYPE = "ref"`, `assoc_right:TYPE = "ref"`.
- **No regex in the parser** — relation/association field classification uses `string:split` and pattern matching.
- Robust property tests (558 randomized tests) covering valid/invalid parse, roundtrip stability, boolean precedence.

### B2. Search AST and backend compilation

Status: `DONE`

- **`ipto_search_ast.erl`**: proper typed AST matching Java's `SearchExpression` hierarchy:
  - `{'$and', ...}`, `{'$or', ...}`, `{'$not', ...}`, `{leaf, ...}`, `{'$between', ...}`
  - `search_item()` tuples: `{unit, Col, Op, Val}`, `{attr, Name, AttrId, Type, Op, Val}`, `{rel, ...}`, `{assoc, ...}`
- **`ipto_search_sql.erl`**: SQL compiler walking AST with two strategies:
  - **EXISTS**: for unit-only queries (direct WHERE clauses)
  - **SET_OPS**: for attribute-constrained queries (WITH/CTE + INTERSECT/UNION)
  - Type-aware vector table selection for CTE joins
  - Sequential `$N` parameter numbering across CTEs and outer query
  - PostgreSQL `LIMIT ... OFFSET ...` paging
- **Neo4j AST→Cypher compiler** (in `ipto_db_neo4j.erl`):
  - Unit items → Cypher WHERE predicates on `k`/`v` nodes
  - Attribute items → `OPTIONAL MATCH` on `AttributeValue` nodes
  - Relation items → `EXISTS { MATCH ... }` on `RELATED_TO` edges
  - Association items → `EXISTS { MATCH ... }` on `HAS_ASSOC` edges
- **Memory backend** walks AST directly with `memory_match_ast/2` and `memory_match_item/2`, including attribute matching against the unit's `attributes` list.
- **Unified pipeline**: all expressions (text, map, AST tuple) go through the same AST path.
- **Legacy WHERE compiler removed** from `ipto_db_pg.erl` (saved ~210 lines).
- Pipeline tests (24 tests) verify full text→AST→SQL for every search item type.

### B3. Search API/GraphQL alignment

Status: `DONE`

- Text-query search via GraphQL `units(query: ..., limit/offset/orderField/orderDir)`.
- Expression-argument search via GraphQL `unitsByExpression(...)`.
- `map_to_ast/1` converts legacy flat-map expressions to proper AST for unified dispatch.

## Track C: GraphQL breadth

Status: `DONE`

- 22 queries + 12 mutations registered.
- All read ops: unit (by id, corridorid, exists, locked), relation/association reads and counts, metadata (attribute/tenant info).
- All mutation ops: createUnit, lock/unlock, status transitions, relation/association add/remove, SDL configure.
- Operation allowlist/registration via `registeredOperations` query.

## Track D: Test matrix and CI confidence

Status: `PARTIAL`

### Test coverage

- **Unit tests**: 1,104 tests across base profile covering parser, search, CRUD, GraphQL, memory backend.
- **GraphQL profile**: 1,107 tests including graphql-erlang execution tests.
- **Property tests**: 558 randomized search parser tests + 239 status transition tests.
- **Pipeline tests**: 24 full-path tests from text query through AST to SQL/Cypher compilation.
- **Integration tests**: env-gated PG and Neo4j tests (`IPTO_PG_INTEGRATION=1`, `IPTO_NEO4J_INTEGRATION=1`).

### CI infrastructure

- `Makefile` with targets: `test`, `test-graphql`, `test-pg`, `test-neo4j`, `test-all`, `full-check`, `up`/`down`.
- `docker-compose.yml` for Postgres 16 + Neo4j 5 with health checks and schema auto-loading.
- `.github/workflows/erlang-ci.yml` workflow (unit + PG integration, not yet activated on remote).

## Milestone plan

### M1 (foundation) — `DONE`

- SDL inspection API with graphql-erlang AST parsing.
- Parser expansion with all operators, aliases, boolean grouping, relation/association specs.
- Minimal integration tests.

### M2 (usable setup) — `DONE`

- SDL configure/apply for PostgreSQL.
- AST-based search with SET_OPS/EXISTS SQL compilation.
- GraphQL setup/search operations.

### M3 (cross-backend + API breadth) — `DONE`

- Neo4j AST→Cypher compiler.
- Memory backend AST-based matching with attribute support.
- GraphQL surface covering 22 queries + 12 mutations.
- Unified search pipeline (maps→AST→compiler, no legacy path).

## Architecture highlights

### Module structure

| Module | Lines | Role |
|--------|-------|------|
| `ipto_search_ast.erl` | ~190 | Typed AST nodes and search items |
| `ipto_search_parser.erl` | ~680 | Tokenizer + recursive-descent parser for DSL |
| `ipto_search_sql.erl` | ~340 | SQL compiler (SET_OPS + EXISTS strategies) |
| `ipto_db.erl` | ~230 | Public API + backend dispatch |
| `ipto_db_pg.erl` | ~1,090 | PostgreSQL backend (AST-aware search, CRUD) |
| `ipto_db_memory.erl` | ~680 | Memory backend (AST matching, search) |
| `ipto_db_neo4j.erl` | ~2,080 | Neo4j backend (AST→Cypher, CRUD) |
| `ipto_db_utils.erl` | ~220 | Shared helpers (normalize, like_match, etc.) |

### Search pipeline

```
Map expression  → map_to_ast/1  →  AST  ─┐
Text query      → parse_ast/1  →  AST  ─┤
AST tuple       → direct       →  AST  ─┘
                                           ↓
                              PG: ipto_search_sql:compile/3
                              Memory: memory_match_ast/2
                              Neo4j: search_units_ast_cypher/3
```

## Intentional differences to keep

- Rust-specific Python integration remains Rust-only.
- Erlang keeps simpler runtime wiring than Java dynamic runtime internals where API behavior is compatible.
- GraphQL schema is hardcoded as a binary fallback (overridable via `graphql_schema_file` env var or `graphql_schema_sdl` app env).

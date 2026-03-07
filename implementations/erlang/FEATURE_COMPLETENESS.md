# Erlang Feature Completeness Tracker

Last updated: 2026-03-07

Legend: `DONE`, `PARTIAL`, `MISSING`

## Scope and intent

This tracks Erlang parity against Java core behavior, while allowing intentional divergence where useful (for example Rust/Python integration does not need Erlang parity).

Priority for Erlang usefulness:

1. SDL-driven configuration/setup of metadata and templates.
2. Search language depth beyond the current minimal subset.
3. Broader GraphQL operation coverage aligned with repo operations.

## Current status (high level)

- Repository core lifecycle (`create/get/store/version/status/lock`): `PARTIAL`
- Relations and associations (PG + Neo4j + memory): `DONE` on core operations
- Attribute and tenant metadata lookup/create: `PARTIAL`
- GraphQL runtime/API breadth: `PARTIAL`
- SDL parser and setup/apply path: `DONE` (memory + PG; Neo4j template persistence unsupported by design)
- Search language/parser depth: `PARTIAL` (minimal current fields/operators only)
- Integration test depth and parity matrix: `PARTIAL`

## Track A: SDL parser and setup (top priority)

Goal: configure persistence from GraphQL SDL for attributes/records/templates, so Erlang can bootstrap database metadata without manual SQL metadata prep.

### A1. SDL inspection API

- Status: `DONE`
- Deliverables:
  - `ipto_graphql_sdl` module that parses SDL and returns normalized catalog maps.
  - Recognize at least:
    - `@attributeRegistry`
    - `@record`
    - `@template`
    - `@use(attribute: ...)`
  - Validation errors include field/type/attribute reference context.
- Acceptance:
  - Unit tests for valid SDL, unresolved attribute references, duplicate keys, and unsupported directive forms.

### A2. SDL apply/configure API

- Status: `DONE` (for memory + PostgreSQL; Neo4j explicitly reports unsupported template persistence)
- Deliverables:
  - `ipto:inspect_graphql_sdl/1`
  - `ipto:configure_graphql_sdl/1`
  - Optional file variant: `ipto:configure_graphql_sdl_file/1`
  - Backend callback additions in `ipto_backend` for template persistence (record/unit templates).
  - PG implementation persists metadata to shared schema tables.
  - Neo4j implementation returns explicit unsupported or partial support metadata (not silent no-op).
- Acceptance:
  - End-to-end integration test:
    - configure from SDL
    - verify attributes exist
    - verify record/unit templates persisted (PG)
  - Idempotent re-apply behavior.

Current progress:

- `DONE` `ipto:inspect_graphql_sdl/1` and `ipto:configure_graphql_sdl/1`.
- `DONE` `ipto:configure_graphql_sdl_file/1`.
- `DONE` SDL validation errors for missing registry, duplicates, and unresolved `@record/@use` references.
- `DONE` idempotent attribute setup from SDL in current backend (`create missing`, `reuse existing`).
- `DONE` explicit backend capability reporting for record/template persistence.
- `DONE` PostgreSQL persistence wiring for record/unit templates, including env-gated PG integration verification of `repo_record_template*` and `repo_unit_template*` rows after configure.

### A3. GraphQL exposure for setup

- Status: `DONE`
- Deliverables:
  - GraphQL operations for SDL inspection/configure (or strict admin-only subset).
  - Clear error envelope for parse/validation/backend capability failures.
- Acceptance:
  - GraphQL tests for happy path and validation failure path.

Current progress:

- `DONE` GraphQL query operation `inspectGraphqlSdl(sdl: String!)`.
- `DONE` GraphQL mutation operations `configureGraphqlSdl(sdl: String!)` and `configureGraphqlSdlFile(path: String!)`.
- `DONE` resource-level tests for setup operation dispatch and error handling.
- `DONE` typed GraphQL response shapes for setup operations (`SdlCatalog`, `SdlConfigureResult` and related types).
- `DONE` end-to-end `graphql_erl` execution coverage for setup operations (`inspectGraphqlSdl` + `configureGraphqlSdl`) in graphql profile tests.

## Track B: Search language depth (second priority)

Goal: move from minimal parser to practical parity with Java/Rust query ergonomics where it matters operationally.

### B1. Parser grammar expansion

- Status: `DONE`
- Deliverables:
  - Boolean composition: `and`, `or`, `not`, parenthesis grouping.
  - Operators:
    - equality/inequality (`=`, `!=`, `<>`)
    - comparison (`>`, `>=`, `<`, `<=`)
    - wildcard/pattern (`like`, `~`)
    - list membership (`in`, `not in`)
    - range (`between ... and ...`)
  - Field aliases:
    - `tenantid|tenant_id`
    - `unitid|unit_id`
    - `corrid|corr_id|correlationid`
    - `unitname|unit_name|name`
  - Status literal support (for example `EFFECTIVE`) in addition to numeric.
- Acceptance:
  - Parser unit tests for precedence, invalid syntax, and alias normalization.

Current progress:

- `DONE` field aliases: `tenant_id`, `unit_id`, `corr_id`, `correlationid`, `unitname`, `unit_name`.
- `DONE` status literal parsing (`PENDING_DISPOSITION`, `PENDING_DELETION`, `OBLITERATED`, `EFFECTIVE`, `ARCHIVED`) in addition to numeric.
- `DONE` operator coverage for scalar predicates: `=`, `!=`, `<>`, `>`, `>=`, `<`, `<=`, `~`, `like`.
- `DONE` boolean composition and grouping: `and`, `or`, `not`, parenthesized expressions.
- `DONE` set/range syntax: `in`, `not in`, `between ... and ...`.
- `DONE` parser tests for aliases, literals, extended operators, precedence, and grouping.

### B2. Backend search semantics

- Status: `DONE`
- Deliverables:
  - PG and Neo4j backends execute expanded expression model consistently.
  - Shared normalization layer for wildcard handling and type coercion.
  - Document explicitly where backend semantics diverge.
- Acceptance:
  - Backend parity integration tests with same query corpus against PG and Neo4j.

Current progress:

- `DONE` memory backend evaluation for composed boolean expressions (`and`/`or`/`not`) with grouped predicates.
- `DONE` PostgreSQL SQL compilation for composed boolean expressions (`and`/`or`/`not`) with grouped predicates.
- `DONE` env-gated PostgreSQL integration search corpus covering `or`, `not`, `in`, `not in`, and `between`.
- `DONE` Neo4j backend query compilation for composed boolean expressions and expanded scalar predicates used by the parser output.
- `DONE` env-gated Neo4j integration search corpus covering `or`, `not`, `in`, `not in`, and `between`.
- `DONE` env-gated cross-backend parity harness (`IPTO_BACKEND_PARITY=1`) asserting same search corpus totals across PostgreSQL and Neo4j.

### B3. Search API/GraphQL alignment

- Status: `PARTIAL`
- Deliverables:
  - GraphQL query operations for expression-based and text-query-based search.
  - Paging + ordering model consistent with core `search_units/3`.
- Acceptance:
  - GraphQL test coverage for query variants and validation failures.

Current progress:

- `DONE` text-query search via GraphQL `units(query: ..., limit/offset/orderField/orderDir)`.
- `DONE` expression-argument search via GraphQL `unitsByExpression(...)` with paging and ordering.
- `DONE` graphql profile execution coverage for both text-query and expression-argument search operations.
- `PARTIAL` richer GraphQL expression shape (boolean tree input object) is still pending.

## Track C: GraphQL breadth

Goal: increase operation surface beyond current `unit/units/createUnit/inactivateUnit/activateUnit`.

- Status: `PARTIAL`
- Deliverables:
  - Read operations:
    - unit by corrid
    - existence/lock state
    - relation/association read + counts
    - metadata fetches (`getAttributeInfo`, `getTenantInfo`)
  - Mutations:
    - lock/unlock
    - relation/association add/remove
    - status transition request
  - Operation allowlist/registration metadata (optional but recommended).
- Acceptance:
  - Resolver tests per operation and integration smoke coverage against active backend.

Current progress:

- `DONE` query: `unitByCorrid(corrid, tenantid?)`.
- `DONE` query: `unitExists(tenantid, unitid)`.
- `DONE` query: explicit lock-state read (`unitLocked(tenantid, unitid)`).
- `DONE` query: relation/association reads and counts (`rightRelation(s)`, `leftRelations`, `count*Relations`, `rightAssociation(s)`, `leftAssociations`, `count*Associations`).
- `DONE` query: `tenantInfo(id|name)` and `attributeInfo(id|name)`.
- `DONE` mutations: `lockUnit`, `unlockUnit`.
- `DONE` mutations: status transition request (`requestStatusTransition`, `transitionUnitStatus` alias) using transition policy.
- `DONE` mutations: `addRelation`, `removeRelation`, `addAssociation`, `removeAssociation`.
- `DONE` operation registration metadata query: `registeredOperations` with optional app-env allowlist filtering (`graphql_operation_allowlist`).
- `DONE` resource-level and graphql profile execution tests for expanded operation surface.

## Track D: Test matrix and CI confidence

- Status: `PARTIAL`
- Deliverables:
  - Explicit test matrix for backends (`memory`, `pg`, `neo4j`) and profiles (`core`, `graphql`, `http`).
  - Contract tests for unsupported capability reporting (not implicit fallback).
  - CI targets:
    - fast unit/core
    - backend integrations (opt-in/env-gated)
- Acceptance:
  - Documented commands and expected gates for each track milestone.

## Milestone plan

### M1 (foundation)

- A1 SDL inspection API
- B1 parser expansion (core operators + aliases)
- Minimal integration tests for parser and inspection

Exit criteria:

- SDL can be parsed and validated from Erlang API.
- Search parser supports grouped boolean expressions and richer operators.

### M2 (usable setup)

- A2 SDL configure/apply for PostgreSQL
- B2 PG backend execution of expanded search
- C initial GraphQL setup/search operations

Exit criteria:

- Fresh PG test database can be configured from SDL through Erlang API.
- Search use cases for tenant/unit/name/corrid/status/relations/associations are covered.

### M3 (cross-backend + API breadth)

- A2 Neo4j capability handling + explicit support reporting
- B2 Neo4j execution parity for supported expression set
- C expanded GraphQL read/mutation surface
- D matrix hardening

Exit criteria:

- Clear, documented supported/unsupported matrix across memory/PG/Neo4j.
- GraphQL surface is practically usable for administration and query workflows.

## Intentional differences to keep

- Rust-specific Python integration remains Rust-only and is not a parity target for Erlang.
- Erlang may keep simpler runtime wiring than Java dynamic runtime internals if API behavior is compatible.

## Near-term implementation order

1. Build `ipto_graphql_sdl` parser + validation (`A1`).
2. Add `inspect/configure` API and PG persistence path (`A2`).
3. Expand parser and PG search compiler first (`B1` + PG part of `B2`).
4. Expose SDL/setup and search operations via GraphQL (`A3` + `B3` + `C` baseline).
5. Bring Neo4j search support and explicit capability reporting to parity envelope.

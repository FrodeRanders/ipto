# Repository Guidelines

## Project Structure & Module Organization
- `implementations/java/repo/` contains the core Java repository implementation (`src/main/java`) and unit tests (`src/test/java`).
- `implementations/java/graphql/` hosts the GraphQL runtime/configuration layer and its tests.
- `implementations/java/it/` contains integration tests (`*IT.java`) and shared test fixtures.
- `shared/db/` holds database schema, procedures, and setup scripts (PostgreSQL and DB2).
- `implementations/erlang/` contains the Erlang implementation.
- `implementations/rust/` contains the Rust implementation.
- `apps/admin-web/` contains the web-administration GUI.
- `Specification.graphql` and `doc/` document the SDL configuration and usage guides.

## Build, Test, and Development Commands
- `mvn -f implementations/java/pom.xml -q test` runs unit tests across Java modules (JUnit Jupiter).
- `mvn -f implementations/java/pom.xml -pl repo test` or `mvn -f implementations/java/pom.xml -pl graphql test` scopes tests to a Java module.
- `mvn -f implementations/java/pom.xml -pl it verify` runs integration tests via Failsafe (`*IT.java`).
- `python shared/db/postgresql/setup-for-test.py` or `python shared/db/db2/setup-for-test.py` provisions a local Docker DB with schema.

## Coding Style & Naming Conventions
- Java 21 source level; use 4-space indentation and standard Java formatting.
- Package names are lowercase (`org.gautelis.ipto.*`); classes use `PascalCase`; methods/fields use `camelCase`.
- Test classes use `*Test` (unit) and `*IT` (integration) suffixes.
- No enforced formatter is configured; align with surrounding code and existing import ordering.

## Testing Guidelines
- Framework: JUnit Jupiter (`org.junit.jupiter`).
- Unit tests live under each module’s `src/test/java` and follow `*Test.java` naming.
- Integration tests live in `implementations/java/it/src/test/java` and are named `*IT.java`; they expect a running DB and SDL fixtures.
- Prefer running DB setup scripts before `mvn -f implementations/java/pom.xml -pl it verify`.

## Commit & Pull Request Guidelines
- Recent commits use short, imperative summaries; some use a `fix:`-style prefix. Keep messages concise and descriptive.
- PRs should include: a brief summary, test command(s) run, and any DB or SDL changes (`Specification.graphql`).
- If behavior changes touch persistence or GraphQL, add/adjust tests in `implementations/java/repo/` or `implementations/java/it/` accordingly.

## Configuration & Database Notes
- DB configuration lives in `implementations/java/repo/src/main/resources/org/gautelis/ipto/repo/` (e.g., `configuration_pg.xml`).
- SQL statements and schema live under `shared/db/postgresql/` and `shared/db/db2/`; keep DB-specific changes in both where applicable.

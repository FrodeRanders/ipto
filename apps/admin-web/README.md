# IPTO Admin (SvelteKit)

Minimal SvelteKit UI for administration, tree browsing, and search.

Requires Node.js 22.17 or newer; CI uses Node.js 24.

## Structure
- `src/routes/+page.svelte`: overview
- `src/routes/admin/+page.svelte`: attributes/records/templates admin
- `src/routes/trees/+page.svelte`: tenant-scoped unit tree + detail
- `src/routes/search/+page.svelte`: search filters + results + detail
- `src/lib/components/`: reusable UI widgets

## Local run

Start PostgreSQL and the Quarkus app on port 8080 first. For example, from the repository root:

```bash
python3 shared/db/postgresql/setup-for-test.py
mvn -f implementations/java/pom.xml -pl quarkus-app -am -DskipTests package
java -jar implementations/java/quarkus-app/target/quarkus-app/quarkus-run.jar
```

In another terminal, from `apps/admin-web`:

```bash
npm ci
npm run dev
```

Vite forwards `/api` to Quarkus, so reads and writes use the same browser origin.
To use another backend, set `IPTO_API_URL`, for example
`IPTO_API_URL=http://localhost:8082 npm run dev`.
The Quarkus CORS configuration also permits `PUT` from the default development origin.

## Dependencies and configuration
- SvelteKit 3 configuration lives in `vite.config.js`; library imports use the `#lib/*` package alias.
- SvelteKit manages its Cookie 2 dependency directly; the former Cookie 0.7 override has been removed.
- `deepmerge@4.3.1`, used by the Svelte build plugin, has an unresolved [low-severity advisory](https://github.com/advisories/GHSA-gcqx-w48m-w2h4). npm also reports its dependent packages. No patched release is published; do not apply npm's suggested downgrade to pre-1.0 SvelteKit packages.
- CI rejects moderate-or-higher npm advisories before building the UI.

## Production packaging (Quarkus static)
This app can be built into the Quarkus static resources folder so it is served by the same HTTP server as the REST API.

```bash
cd apps/admin-web
npm ci
npm run build
```

The build output is written to `../../implementations/java/quarkus-app/src/main/resources/META-INF/resources`, which Quarkus serves at `/`.

## Integration points
- `src/lib/api.js` calls the Quarkus `/api` endpoints for tenants, units, search, attributes, records, and templates.
- The role store is a UI placeholder. Authentication and authorization belong in the deployment/API layer.
- Java CI builds this app before packaging and testing Quarkus, including when only UI files change.

## Data content visualization
- `DataPreview.svelte` renders JSON safely when `mimetype=application/json`.
- For other MIME types, show metadata and a safe fallback message.
- When introducing downloads, ensure per-tenant authorization and content-type checks.

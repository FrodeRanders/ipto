# IPTO Admin (SvelteKit)

Minimal SvelteKit UI for administration, tree browsing, and search.

## Structure
- `src/routes/+page.svelte`: overview
- `src/routes/admin/+page.svelte`: attributes/records/templates admin
- `src/routes/trees/+page.svelte`: tenant-scoped unit tree + detail
- `src/routes/search/+page.svelte`: search filters + results + detail
- `src/lib/components/`: reusable UI widgets

## Local run
```bash
npm ci
npm run dev
```

## Dependency note
- `cookie` is overridden to `^0.7.0` in `package.json` to address a security advisory while `@sveltejs/kit` still declares `^0.6.0`.

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

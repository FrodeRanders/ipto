# IPTO Admin (SvelteKit)

Minimal SvelteKit UI for administration, tree browsing, and search. Uses mock data and in-memory state.

## Structure
- `src/routes/+page.svelte`: overview
- `src/routes/admin/+page.svelte`: attributes/records/templates admin
- `src/routes/trees/+page.svelte`: tenant-scoped unit tree + detail
- `src/routes/search/+page.svelte`: search filters + results + detail
- `src/lib/components/`: reusable UI widgets

## Local run
```bash
npm install
npm run dev
```

## Dependency note
- `cookie` is overridden to `^0.7.0` in `package.json` to address a security advisory while `@sveltejs/kit` still declares `^0.6.0`.

## Production packaging (Quarkus static)
This app can be built into the Quarkus static resources folder so it is served by the same HTTP server as the REST API.

```bash
cd admin-web
npm install
npm run build
```

The build output is written to `quarkus-app/src/main/resources/META-INF/resources`, which Quarkus serves at `/`.

## Integration points
- Replace mock data with server calls in SvelteKit `load` functions.
- Enforce server-side filtering by role and tenant in the API layer.
- Use the role store as a placeholder for auth state, wired to real auth later.
- Example REST call: `GET http://localhost:8080/api/units/example` (wired into search page).

## Data content visualization
- `DataPreview.svelte` renders JSON safely when `mimetype=application/json`.
- For other MIME types, show metadata and a safe fallback message.
- When introducing downloads, ensure per-tenant authorization and content-type checks.

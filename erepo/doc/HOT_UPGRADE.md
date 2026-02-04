# erepo Hot Upgrade Guide

This document describes a practical procedure for upgrading a running `erepo` node with OTP release handling.

## 1) What is in place

- `rebar.config` now contains a `relx` release definition for `erepo` version `0.2.0`.
- `src/erepo.appup` contains upgrade/downgrade instructions between `0.1.0` and `0.2.0`.
- HTTP lifecycle is handled by supervised worker `erepo_http_server` (child of `erepo_sup`), which is safer during upgrade than starting listeners directly in `application:start/2`.

## 2) Release model and expectations

- You upgrade **release versions**, not ad-hoc beam files.
- Each new release version should include:
  - updated `vsn` in `src/erepo.app.src`
  - updated `relx` release version in `rebar.config`
  - updated `src/erepo.appup` entries for previous version(s)
- `release_handler` uses `.appup` and generated `.relup` to run upgrade instructions.

## 3) Build base and target releases

Example: upgrade from `0.1.0` to `0.2.0`.

Use the runtime profile so optional backends (PostgreSQL + GraphQL + HTTP) are included in the release.

1. Build and archive old release (from old code checkout/tag):

```sh
rebar3 as runtime release
```

2. Build new release (this code):

```sh
rebar3 as runtime release
```

3. Generate relup in the new release build (paths may vary by your build layout):

```sh
rebar3 relup --upfrom 0.1.0 --output-dir _build/default/rel/erepo
```

4. Create tarball for distribution:

```sh
rebar3 as runtime tar
```

Notes:
- If your environment uses profiles, apply the same profile to both old/new release steps.
- `relup` generation requires the old release metadata to be available to `relx`.
- Building plain `rebar3 release` is still possible, but intended for minimal/in-memory runtime and may show missing-call warnings for optional dependencies.

## 4) Upgrade a running node

On the running node shell:

```erlang
{ok, Vsn} = release_handler:unpack_release("erepo-0.2.0").
{ok, OldVsn, _Desc} = release_handler:install_release(Vsn).
ok = release_handler:make_permanent(Vsn).
```

Recommended immediate checks:

```erlang
application:which_applications().
whereis(erepo_sup).
whereis(erepo_http_server).
```

If GraphQL SDL overrides are used and you need deterministic refresh:

```erlang
ok = erepo_graphql:reload_schema().
```

## 5) Rollback procedure

If post-upgrade validation fails:

```erlang
{ok, _Cur, _Desc} = release_handler:install_release("0.1.0").
ok = release_handler:make_permanent("0.1.0").
```

`src/erepo.appup` includes downgrade instructions from `0.2.0` to `0.1.0`.

## 6) HTTP behavior during upgrade

`erepo_http_server` ensures HTTP listener state converges to config both at startup and code change:

- Enabled + stopped -> starts listener.
- Enabled + port changed -> restarts listener on new port.
- Disabled + running -> stops listener.

Runtime controls:

- App env: `http_enabled`, `http_port`
- Env vars: `EREPO_HTTP_ENABLED`, `EREPO_HTTP_PORT`

To force runtime reconciliation after changing env/app config:

```erlang
ok = erepo_http_server:refresh().
```

## 7) Appup maintenance checklist for future releases

For each new version `X.Y.Z`:

1. Copy previous `src/erepo.appup` and change top version string to `X.Y.Z`.
2. Add/adjust upgrade instructions from prior version(s):
   - `load_module` for normal modules.
   - `update` (advanced) for stateful processes requiring `code_change/3`.
   - `add_module`/`delete_module` for added/removed modules.
   - `update ... supervisor` when child specs change.
3. Add matching downgrade entries.
4. Validate with a staging node before production.

## 8) Operational advice

- Keep release upgrades small and frequent.
- Avoid state shape changes without `code_change/3`.
- Prefer supervised long-running resources (HTTP listeners, DB pools, etc.).
- Keep your runtime config source-of-truth stable during upgrade windows.

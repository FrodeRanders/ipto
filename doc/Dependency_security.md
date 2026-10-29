# Dependency security review — 2026-10-29

The remaining GitHub alerts were reviewed against their affected versions and
patched upstream releases. Updates stay within the existing Jackson major lines
and Rust dependency compatibility ranges.

| Surface | Updated dependency |
| --- | --- |
| Java repository / GraphQL | Jackson Databind 3.2.3 |
| Quarkus REST / Vert.x | Jackson 2.22.3 BOM, imported before the Quarkus BOM |
| Optional DB2 helper | Jackson 2.22.3, retaining the Java 8 compiler target |
| Rust PostgreSQL | tokio-postgres 0.7.18 and Tokio 1.54.1 |
| Rust TLS | Rustls 0.23.46, rustls-webpki 0.103.16, webpki-roots 1.0.11 |
| Other Rust transitive dependencies | ctutils 0.4.7, anyhow 1.0.104, chacha20 0.10.2 |

Quarkus's REST stack uses Jackson 2 independently of the repository's Jackson 3
dependency. Updating the parent repository property alone left REST Databind at
2.22.0; the application now manages that dependency family explicitly.

The DB2 helper was built separately. Every class in its shaded JAR has Java 8
bytecode (major version 52). Execution inside DB2 remains a platform-specific check.

`cargo audit` reports no vulnerabilities, unsoundness warnings, or yanked packages
for the updated lockfile, including the additional
[ctutils](https://rustsec.org/advisories/RUSTSEC-2026-0385.html) and
[webpki-roots](https://rustsec.org/advisories/RUSTSEC-2026-0381.html) findings.

## Outstanding upstream finding

`deepmerge@4.3.1` has an unresolved
[low-severity advisory](https://github.com/advisories/GHSA-gcqx-w48m-w2h4).
It is used by the Svelte/Vite build plugin to merge configuration, rather than by
the Quarkus server at runtime. npm reports the affected dependency chain as four
low-severity package findings. Keep the advisory open and monitor upstream;
do not apply npm's suggested downgrade to obsolete pre-1.0 SvelteKit packages.

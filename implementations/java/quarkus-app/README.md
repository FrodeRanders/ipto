# IPTO Quarkus application

The application serves the repository REST API, GraphQL, and the optional admin UI.
Run commands below from the repository root.

## Build and run

```sh
python3 shared/db/postgresql/setup-for-test.py
./build.sh
java -jar implementations/java/quarkus-app/target/quarkus-app/quarkus-run.jar
```

`./build.sh` runs a clean Maven package with the `web` profile. It installs locked
npm dependencies, builds the UI, and includes the generated resources in the
application. Requires Node.js 22.17+, JDK 21+, Maven, and the local vopn dependency
specified in the [root README](../../../README.md). CI uses Node 24 and JDK 25.
Use `./build.sh -DskipTests` to package without a running test database.
The default build excludes the Java 8 DB2 helper.

The fast-JAR application needs the entire `target/quarkus-app` directory, including
its `lib` and `app` directories. The runner JAR alone is insufficient.

## Accounts and API access

Configure bcrypt hashes in the environment to enable local administrator and
optional reader accounts. There are no default passwords. See
[local accounts](../../../doc/Local_accounts.md) for interactive hash generation,
sign-in, HTTPS deployment, and authorization tests.

Ordinary REST reads, searches, and GraphQL queries remain public. Administrator
access is required for attribute and record writes and for GraphQL mutations.
The server decides roles; the browser displays the authenticated identity.

## Development and verification

For an API-only build:

```sh
mvn -f implementations/java/pom.xml -pl quarkus-app -am clean package
```

For all Java tests and production UI packaging:

```sh
mvn -f implementations/java/pom.xml -pl it,quarkus-app -am -Pweb clean verify
```

For UI development with Vite, see the [admin UI README](../../../apps/admin-web/README.md).
For persistent PostgreSQL data, use [schema migrations](../../../shared/db/postgresql/migrations/README.md)
instead of the disposable database setup command.

Known UI routes, including editor URLs, support direct navigation and reloads through
the packaged SvelteKit shell. Unknown API and asset paths remain 404 responses.

# PostgreSQL schema migrations

`../migrate.py` applies ordered `VNNN__description.sql` files to an existing
database using Python's standard library and `psql`. It never starts or removes
containers, drops databases, or re-runs installed SQL. All pending migrations and
their history records run in one transaction under a PostgreSQL advisory lock.
SQL failures roll back that entire invocation. A concurrent invocation waits for
the lock, then checks history again; the lock timeout is 30 seconds.

The database and its `repo` login role must already exist. For an empty database:

```sh
PGPASSWORD=repo python3 shared/db/postgresql/migrate.py
# Or use psql inside an existing container (no host psql installation needed):
PGPASSWORD=repo python3 shared/db/postgresql/migrate.py --container repo-postgres
```

Connection options are `--host`, `--port`, `--user`, and `--database`, with defaults
from `PGHOST`, `PGPORT`, `PGUSER`, and `PGDATABASE`. Use `PGPASSWORD` or a PostgreSQL
password file for authentication. With `--container`, connection options describe
the database inside that container. There is no default password in this command.

## Adopt an existing database

Back up the database and review that it matches the canonical V1 schema,
procedures, and seed data before adopting it. The existing disposable setup script
loads those canonical files without migration history. To adopt such a database:

```sh
PGPASSWORD=repo python3 shared/db/postgresql/migrate.py --container repo-postgres --baseline-existing
```

Adoption records V1 without executing its DDL or seed inserts, preserving units,
attributes, catalog changes, and identity sequences. The command checks that the
baseline tables exist in the `repo` schema; that check does not certify every
column, index, constraint, function, or seed row. The flag is an explicit operator
assertion that the database is compatible with V1. Partial schemas and repository
tables in `public` are rejected. After adoption, use the command without the flag.

## Add an upgrade

V001 is a frozen snapshot of the original `schema.sql`, `procedures.sql`, and
`boot.sql`. Never edit an installed migration. Add a higher version such as
`V002__add_example_column.sql`, using data-preserving SQL. The history table
`repo.ipto_schema_history` records SHA-256 checksums, names, installer, timestamps,
and whether V1 was adopted. Modified or missing installed migrations and newly
introduced older versions are rejected. Keep canonical disposable-setup SQL in
sync with the resulting schema when adding upgrades, while leaving V001 frozen.

Migrations must contain transactional PostgreSQL SQL. Do not include explicit
transaction control, psql commands, `CREATE DATABASE`, `VACUUM`, or concurrent index
creation; those need a separately reviewed operational procedure. There is no
automatic downgrade. Take and verify backups before upgrading persistent data.

## Tests

```sh
IPTO_PG_MIGRATION_TEST_CONTAINER=repo-postgres python3 -m unittest discover -s shared/db/postgresql -p 'test_*.py'
```

The integration tests create uniquely named databases inside that disposable
container and remove only those databases. They exercise fresh installation,
explicit adoption, a V2 upgrade of populated units, replay, failed-upgrade rollback,
checksum validation, missing/out-of-order versions, and concurrent invocations.
The account used by these tests must be able to create and drop their databases.

#!/usr/bin/env python3
"""Apply checked, transactional SQL migrations without recreating a database."""

import argparse
from hashlib import sha256
import os
from pathlib import Path
import re
import subprocess
import sys

MIGRATIONS = Path(__file__).resolve().parent / "migrations"
NAME = re.compile(r"V([0-9]+)__([a-z0-9_]+)\.sql")
BASELINE_TABLES = (
    "repo_tenant", "repo_unit_kernel", "repo_unit_version", "repo_namespace",
    "repo_attribute", "repo_attribute_description", "repo_attribute_value",
    "repo_unit_template", "repo_unit_template_elements", "repo_record_template",
    "repo_record_template_elements", "repo_string_vector", "repo_time_vector",
    "repo_integer_vector", "repo_long_vector", "repo_double_vector",
    "repo_boolean_vector", "repo_data_vector", "repo_record_vector",
    "repo_log", "repo_lock", "repo_internal_relation", "repo_external_assoc",
)


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def migration_sql(directory=MIGRATIONS, baseline_existing=False):
    migrations = []
    for path in sorted(directory.glob("*.sql")):
        match = NAME.fullmatch(path.name)
        if not match:
            raise ValueError(f"Invalid migration filename: {path.name}")
        data = path.read_bytes()
        migrations.append((int(match[1]), path.name, sha256(data).hexdigest(), data.decode("utf-8")))
    migrations.sort()
    versions = [item[0] for item in migrations]
    if not versions or versions[0] != 1 or len(versions) != len(set(versions)):
        raise ValueError("Migrations must have unique positive versions, starting at V001")
    expected = ",\n".join(f"({version}, {literal(name)}, {literal(checksum)})"
                          for version, name, checksum, _ in migrations)
    required_tables = ",".join(literal("repo." + table) for table in BASELINE_TABLES)
    sql = [rf"""\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout = '30s';
SELECT pg_advisory_xact_lock(730617, 1);
CREATE SCHEMA IF NOT EXISTS repo AUTHORIZATION repo;
SET LOCAL search_path = repo, public;
SELECT to_regclass('repo.ipto_schema_history') IS NULL
       AND EXISTS (SELECT FROM pg_tables WHERE schemaname = 'repo' AND left(tablename, 5) = 'repo_')
       AS legacy \gset
DO $migration$
DECLARE required_table text;
BEGIN
    IF to_regclass('repo.ipto_schema_history') IS NULL
       AND EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND left(tablename, 5) = 'repo_') THEN
        RAISE EXCEPTION 'Found legacy repository tables in public; expected the repo schema';
    END IF;
    IF to_regclass('repo.ipto_schema_history') IS NULL
       AND EXISTS (SELECT FROM pg_tables WHERE schemaname = 'repo' AND left(tablename, 5) = 'repo_') THEN
        IF NOT {str(baseline_existing).lower()} THEN
            RAISE EXCEPTION 'Existing database has no migration history; review it and use --baseline-existing';
        END IF;
        FOREACH required_table IN ARRAY ARRAY[{required_tables}] LOOP
            IF to_regclass(required_table) IS NULL THEN
                RAISE EXCEPTION 'Incomplete legacy baseline: missing %', required_table;
            END IF;
        END LOOP;
    END IF;
END
$migration$;
CREATE TABLE IF NOT EXISTS repo.ipto_schema_history (
    version integer PRIMARY KEY CHECK (version > 0),
    name text NOT NULL,
    checksum text NOT NULL,
    installed_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    installed_by text NOT NULL DEFAULT CURRENT_USER,
    baselined boolean NOT NULL DEFAULT false
);
CREATE TEMP TABLE ipto_expected_migrations (version integer, name text, checksum text) ON COMMIT DROP;
INSERT INTO ipto_expected_migrations VALUES {expected};
DO $migration$
BEGIN
    IF EXISTS (
        SELECT FROM repo.ipto_schema_history h
        LEFT JOIN ipto_expected_migrations e USING (version)
        WHERE e.version IS NULL OR h.name <> e.name OR h.checksum <> e.checksum
    ) THEN
        RAISE EXCEPTION 'Migration history mismatch: missing or modified migration; restore the original files';
    END IF;
    IF EXISTS (
        SELECT FROM ipto_expected_migrations e
        WHERE e.version < (SELECT max(version) FROM repo.ipto_schema_history)
          AND NOT EXISTS (SELECT FROM repo.ipto_schema_history h WHERE h.version = e.version)
    ) THEN
        RAISE EXCEPTION 'Out-of-order migration: new migrations must follow the installed version';
    END IF;
END
$migration$;
"""]
    for version, name, checksum, body in migrations:
        sql.append(f"SELECT NOT EXISTS (SELECT FROM repo.ipto_schema_history WHERE version = {version}) AS apply \\gset\n\\if :apply\n")
        if version == 1:
            sql.append("\\if :legacy\n\\echo Adopting existing V1 schema without changing repository data\n\\else\n")
        sql.append(f"\\echo Applying {name}\n{body}\n")
        if version == 1:
            sql.append("\\endif\n")
        sql.append(f"INSERT INTO repo.ipto_schema_history (version, name, checksum, baselined) "
                   f"VALUES ({version}, {literal(name)}, {literal(checksum)}, "
                   f"{'true' if version == 1 and baseline_existing else 'false'} AND :'legacy'::boolean);\n\\endif\n")
    sql.append("COMMIT;\n\\echo Migrations completed.\n")
    return "".join(sql)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--container", help="Run psql inside this existing Docker container")
    parser.add_argument("--host", default=os.environ.get("PGHOST", "localhost"))
    parser.add_argument("--port", default=os.environ.get("PGPORT", "5432"))
    parser.add_argument("--user", default=os.environ.get("PGUSER", "repo"))
    parser.add_argument("--database", default=os.environ.get("PGDATABASE", "repo"))
    parser.add_argument("--baseline-existing", action="store_true",
                        help="Explicitly adopt a reviewed database matching the V1 baseline")
    args = parser.parse_args(argv)
    command = ["psql", "-X", "-w", "-v", "ON_ERROR_STOP=1", "-h", args.host,
               "-p", args.port, "-U", args.user, "-d", args.database]
    if args.container:
        command = ["docker", "exec", "-i", "-e", "PGPASSWORD", args.container, *command]
    try:
        result = subprocess.run(command, input=migration_sql(baseline_existing=args.baseline_existing),
                                text=True, check=True)
        return result.returncode
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"Migration failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

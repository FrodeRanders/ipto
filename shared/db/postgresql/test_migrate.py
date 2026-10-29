"""Migration regressions; set IPTO_PG_MIGRATION_TEST_CONTAINER for real DB tests."""

import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
import uuid

import migrate


class MigrationPlanTest(unittest.TestCase):
    def test_duplicate_versions_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            (directory / "V001__first.sql").write_text("SELECT 1;")
            (directory / "V1__duplicate.sql").write_text("SELECT 1;")
            with self.assertRaisesRegex(ValueError, "unique"):
                migrate.migration_sql(directory)

    def test_invalid_filenames_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            (directory / "unversioned.sql").write_text("SELECT 1;")
            with self.assertRaisesRegex(ValueError, "filename"):
                migrate.migration_sql(directory)


@unittest.skipUnless(os.environ.get("IPTO_PG_MIGRATION_TEST_CONTAINER"),
                     "Set IPTO_PG_MIGRATION_TEST_CONTAINER to a disposable PostgreSQL container")
class MigrationIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.container = os.environ["IPTO_PG_MIGRATION_TEST_CONTAINER"]
        self.database = "ipto_migration_" + uuid.uuid4().hex
        self.psql(f'CREATE DATABASE "{self.database}" OWNER repo;', database="postgres")
        self.directory = tempfile.TemporaryDirectory(prefix="ipto-migrations-")
        self.addCleanup(self.directory.cleanup)
        self.migrations = Path(self.directory.name)
        shutil.copy(migrate.MIGRATIONS / "V001__baseline.sql", self.migrations)

    def tearDown(self):
        self.psql(f'DROP DATABASE "{self.database}" WITH (FORCE);', database="postgres")

    def command(self, database=None):
        return ["docker", "exec", "-i", self.container,
                "psql", "-X", "-w", "-v", "ON_ERROR_STOP=1", "-At",
                "-U", "repo", "-d", database or self.database]

    def psql(self, sql, *, database=None, success=True):
        result = subprocess.run(self.command(database), input=sql, text=True,
                                capture_output=True, timeout=30)
        if success:
            self.assertEqual(0, result.returncode, result.stderr)
        else:
            self.assertNotEqual(0, result.returncode, result.stdout)
        return result

    def apply(self, *, baseline=False, success=True):
        return self.psql(migrate.migration_sql(self.migrations, baseline), success=success)

    def seed(self):
        self.psql("INSERT INTO repo.repo_tenant (tenantid,name,description) "
                  "VALUES (987,'migration-check','must survive'); "
                  "INSERT INTO repo.repo_unit_kernel (tenantid,unitid,corrid) "
                  "VALUES (987,987,'fe6ac8ba-5006-4197-8b57-d42238b651d6'); "
                  "INSERT INTO repo.repo_unit_version (tenantid,unitid,unitver,unitname) "
                  "VALUES (987,987,1,'Preserved unit');")

    def assert_data_preserved(self):
        self.assertEqual("migration-check|must survive|Preserved unit", self.psql(
            "SELECT t.name,t.description,v.unitname FROM repo.repo_tenant t "
            "JOIN repo.repo_unit_version v USING (tenantid) WHERE t.tenantid=987;").stdout.strip())

    def test_fresh_install_upgrade_and_replay_preserve_data(self):
        self.apply()
        self.seed()
        (self.migrations / "V002__tenant_note.sql").write_text(
            "ALTER TABLE repo.repo_tenant ADD COLUMN migration_note text;\n"
            "UPDATE repo.repo_tenant SET migration_note='upgraded' WHERE tenantid=987;\n")
        self.apply()
        self.apply()
        self.assert_data_preserved()
        self.assertEqual("upgraded", self.psql(
            "SELECT migration_note FROM repo.repo_tenant WHERE tenantid=987;").stdout.strip())
        self.assertEqual("2", self.psql("SELECT count(*) FROM repo.ipto_schema_history;").stdout.strip())

    def test_legacy_requires_explicit_adoption_and_preserves_data(self):
        self.psql("SET search_path=repo,public;\n" +
                  "\n".join((migrate.MIGRATIONS.parent / name).read_text()
                            for name in ("schema.sql", "procedures.sql", "boot.sql")))
        self.seed()
        denied = self.apply(success=False)
        self.assertIn("--baseline-existing", denied.stderr)
        self.assert_data_preserved()
        self.apply(baseline=True)
        self.apply()
        self.assert_data_preserved()
        self.assertEqual("t", self.psql(
            "SELECT baselined FROM repo.ipto_schema_history WHERE version=1;").stdout.strip())

    def test_failed_upgrade_rolls_back_ddl_data_and_history(self):
        self.apply()
        self.seed()
        (self.migrations / "V002__failure.sql").write_text(
            "ALTER TABLE repo.repo_tenant ADD COLUMN should_rollback text;\n"
            "UPDATE repo.repo_tenant SET description='lost';\nSELECT 1/0;\n")
        self.apply(success=False)
        self.assert_data_preserved()
        self.assertEqual("0", self.psql(
            "SELECT count(*) FROM information_schema.columns WHERE table_schema='repo' "
            "AND table_name='repo_tenant' AND column_name='should_rollback';").stdout.strip())
        self.assertEqual("1", self.psql("SELECT count(*) FROM repo.ipto_schema_history;").stdout.strip())

    def test_modified_or_missing_history_and_out_of_order_migrations_fail(self):
        self.apply()
        baseline = self.migrations / "V001__baseline.sql"
        baseline.write_text(baseline.read_text() + "\n-- modified\n")
        self.assertIn("history mismatch", self.apply(success=False).stderr)
        shutil.copy(migrate.MIGRATIONS / "V001__baseline.sql", baseline)
        (self.migrations / "V003__third.sql").write_text("SELECT 3;\n")
        self.apply()
        (self.migrations / "V002__late.sql").write_text("SELECT 2;\n")
        self.assertIn("Out-of-order", self.apply(success=False).stderr)
        (self.migrations / "V002__late.sql").unlink()
        (self.migrations / "V003__third.sql").unlink()
        self.assertIn("history mismatch", self.apply(success=False).stderr)

    def test_concurrent_upgrades_serialize(self):
        self.apply()
        self.seed()
        (self.migrations / "V002__slow.sql").write_text(
            "SELECT pg_sleep(0.3);\nALTER TABLE repo.repo_tenant ADD COLUMN concurrent_note text;\n")
        sql = migrate.migration_sql(self.migrations)
        first = subprocess.Popen(self.command(), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, text=True)
        first.stdin.write(sql)
        first.stdin.close()
        second = self.apply()
        first.wait(timeout=30)
        self.assertEqual(0, first.returncode, first.stderr.read())
        first.stdout.close()
        first.stderr.close()
        self.assertIn("Migrations completed.", second.stdout)
        self.assert_data_preserved()

    def test_incomplete_legacy_database_is_rejected(self):
        self.psql("CREATE SCHEMA repo; CREATE TABLE repo.repo_tenant (tenantid int);")
        self.assertIn("Incomplete legacy baseline", self.apply(baseline=True, success=False).stderr)

    def test_cli_reports_sql_failure_with_nonzero_exit(self):
        # Exercise argument parsing, Docker's TCP connection, and failure propagation.
        root = self.migrations.parent / ("ipto-cli-" + uuid.uuid4().hex)
        root.mkdir()
        self.addCleanup(shutil.rmtree, root)
        shutil.copy(Path(migrate.__file__), root / "migrate.py")
        shutil.copytree(self.migrations, root / "migrations")
        command = [sys.executable, str(root / "migrate.py"), "--container", self.container,
                   "--database", self.database]
        env = dict(os.environ, PGPASSWORD="repo", PGHOST="localhost", PGPORT="5432", PGUSER="repo")
        result = subprocess.run(command, env=env, text=True, capture_output=True, timeout=30)
        self.assertEqual(0, result.returncode, result.stderr)
        (root / "migrations/V002__failure.sql").write_text("SELECT 1/0;\n")
        result = subprocess.run(command, env=env, text=True, capture_output=True, timeout=30)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("Migration failed", result.stderr)
        self.assertNotIn("Migrations completed.", result.stdout)


if __name__ == "__main__":
    unittest.main()

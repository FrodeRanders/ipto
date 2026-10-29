"""CLI regressions for setup failure reporting; no Docker daemon is required."""

import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import unittest


FAKE_DOCKER = r'''#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
with open(os.environ['DOCKER_TEST_LOG'], 'a') as log:
    log.write(json.dumps(args) + '\n')
mode = os.environ.get('DOCKER_TEST_MODE', '')
if args[0] == 'pull' and mode == 'pull_failure':
    print('image pull failed', file=sys.stderr)
    sys.exit(1)
if args[0] == 'inspect':
    print('running')
elif 'pg_isready' in args:
    sys.exit(1 if mode == 'not_ready' else 0)
elif 'psql' in args and '-f' in args and mode == 'sql_failure':
    print('ERROR: invalid schema SQL', file=sys.stderr)
    sys.exit(3)
elif args[0] == 'run':
    print('test-container-id')
'''


class SetupCliTest(unittest.TestCase):
    def run_setup(self, mode="", *extra):
        with tempfile.TemporaryDirectory(prefix="ipto-setup-test-") as directory:
            directory = Path(directory)
            executable = directory / "docker"
            executable.write_text(FAKE_DOCKER)
            executable.chmod(0o755)
            log = directory / "docker.jsonl"
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            environment = dict(os.environ)
            environment.update(PATH=str(directory) + os.pathsep + environment["PATH"],
                               DOCKER_TEST_LOG=str(log), DOCKER_TEST_MODE=mode)
            script = Path(__file__).with_name("setup-for-test.py")
            result = subprocess.run(
                [sys.executable, str(script), "--container-name", "ipto-script-test",
                 "--host-port", str(port), *extra],
                env=environment, capture_output=True, text=True, timeout=10,
            )
            commands = [json.loads(line) for line in log.read_text().splitlines()]
            return result, commands

    def test_selected_image_and_sql_error_stopping(self):
        result, commands = self.run_setup("", "--image", "postgres:18")
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("Setup completed.", result.stdout)
        self.assertIn(["pull", "postgres:18"], commands)
        run = next(command for command in commands if command[0] == "run")
        self.assertIn("postgres:18", run)
        self.assertTrue(any(argument.endswith(":/ipto-sql:ro") for argument in run))
        psql = [command for command in commands if "psql" in command]
        self.assertTrue(psql)
        self.assertTrue(all("ON_ERROR_STOP=1" in command for command in psql))

    def test_sql_failure_does_not_report_success(self):
        result, _ = self.run_setup("sql_failure", "--skip-pull")
        self.assertNotEqual(0, result.returncode)
        self.assertIn("invalid schema SQL", result.stderr)
        self.assertNotIn("Setup completed.", result.stdout)

    def test_readiness_timeout_does_not_run_sql(self):
        result, commands = self.run_setup("not_ready", "--startup-timeout", "0.3", "--skip-pull")
        self.assertNotEqual(0, result.returncode)
        self.assertNotIn("Setup completed.", result.stdout)
        self.assertFalse(any("psql" in command for command in commands))

    def test_failed_image_pull_does_not_start_database(self):
        result, commands = self.run_setup("pull_failure")
        self.assertNotEqual(0, result.returncode)
        self.assertIn("image pull failed", result.stderr)
        self.assertFalse(any(command[0] == "run" for command in commands))


if __name__ == "__main__":
    unittest.main()

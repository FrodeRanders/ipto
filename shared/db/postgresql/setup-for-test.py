#!/usr/bin/env python3

# ---------------------------------------------------------------
# -- Copyright (C) 2025-2026 Frode Randers
# -- All rights reserved
# --
# -- Licensed under the Apache License, Version 2.0 (the "License");
# -- you may not use this file except in compliance with the License.
# -- You may obtain a copy of the License at
# --
# --    http://www.apache.org/licenses/LICENSE-2.0
# --
# -- Unless required by applicable law or agreed to in writing, software
# -- distributed under the License is distributed on an "AS IS" BASIS,
# -- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# -- See the License for the specific language governing permissions and
# -- limitations under the License.
# ---------------------------------------------------------------

"""Prepare a disposable PostgreSQL database using the canonical SQL files."""

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time

SQL_FILES = ("schema.sql", "procedures.sql", "boot.sql")


def docker(*args, **kwargs):
    result = subprocess.run(
        ["docker", *args], check=True, capture_output=True, text=True, **kwargs
    )
    return result.stdout.strip()


def is_port_available(port, host="127.0.0.1"):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, port))
            return True
        except OSError:
            return False


def wait_for_container_startup(container_name, wait_seconds):
    print("Waiting for PostgreSQL TCP readiness...")
    deadline = time.monotonic() + wait_seconds
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        status = docker("inspect", "--format", "{{.State.Status}}", container_name,
                        timeout=max(0.01, min(5, remaining)))
        if status != "running":
            logs = docker("logs", "--tail", "30", container_name)
            raise RuntimeError(f"Container {container_name} is {status}: {logs}")
        ready = subprocess.run(
            ["docker", "exec", container_name, "pg_isready", "-h", "127.0.0.1",
             "-U", "postgres", "-d", "postgres"],
            capture_output=True, text=True, timeout=max(0.01, min(5, remaining))
        )
        if ready.returncode == 0:
            return
        time.sleep(max(0, min(0.25, deadline - time.monotonic())))
    raise TimeoutError(f"PostgreSQL did not become ready within {wait_seconds:g} seconds")


def run_psql(container_name, password, user, database, *, commands=None, files=()):
    args = ["exec", "-i", "-e", f"PGPASSWORD={password}", container_name,
            "psql", "-X", "-w", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1",
            "-U", user, "-d", database]
    for filename in files:
        args.extend(["-f", f"/ipto-sql/{filename}"])
    # stdin lets CREATE DATABASE run outside an implicit multi-command transaction.
    docker(*args, input=commands)


def prepare_database(args):
    script_dir = Path(__file__).resolve().parent
    for filename in SQL_FILES:
        if not (script_dir / filename).is_file():
            raise RuntimeError(f"Missing SQL file: {script_dir / filename}")

    names = docker("container", "ls", "--all", "--format", "{{.Names}}").splitlines()
    existing = args.container_name in names
    own_port = False
    if existing:
        bindings = json.loads(docker("inspect", "--format",
                                     "{{json .HostConfig.PortBindings}}", args.container_name))
        own_port = any(binding.get("HostPort") == str(args.host_port)
                       for binding in (bindings or {}).get("5432/tcp", []) or [])
    if not is_port_available(args.host_port) and not own_port:
        raise RuntimeError(f"Host port {args.host_port} is in use; select another --host-port")

    if not args.skip_pull:
        print(f"Pulling {args.image}...")
        docker("pull", args.image)
    if existing:
        print(f"Replacing test container {args.container_name}...")
        docker("rm", "-f", args.container_name)

    admin_password = os.environ.get("IPTO_PG_ADMIN_PASSWORD", "H0nd@666")
    print(f"Starting {args.image} as {args.container_name} on localhost:{args.host_port}...")
    docker("run", "--pull=never", "-m", "8g", "--name", args.container_name,
           "-e", f"POSTGRES_PASSWORD={admin_password}",
           "-p", f"127.0.0.1:{args.host_port}:5432",
           "-v", f"{script_dir}:/ipto-sql:ro", "-d", args.image,
           "-c", "shared_preload_libraries=pg_stat_statements",
           "-c", "pg_stat_statements.track=all")
    wait_for_container_startup(args.container_name, args.startup_timeout)
    run_psql(args.container_name, admin_password, "postgres", "postgres", commands=(
        "CREATE USER repo WITH SUPERUSER PASSWORD 'repo';\n"
        "CREATE DATABASE repo OWNER repo;\n"
    ))
    run_psql(args.container_name, admin_password, "postgres", "repo",
             commands="CREATE EXTENSION IF NOT EXISTS pg_stat_statements;\n")
    run_psql(args.container_name, "repo", "repo", "repo", files=SQL_FILES)
    print("Setup completed.")
    print(f"PostgreSQL ({args.image}) is available on localhost:{args.host_port}")


def positive_seconds(value):
    result = float(value)
    if not 0 < result < float("inf"):
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return result


def port_number(value):
    result = int(value)
    if not 1 <= result <= 65535:
        raise argparse.ArgumentTypeError("must be between 1 and 65535")
    return result


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--container-name", default=os.environ.get("IPTO_PG_CONTAINER_NAME", "repo-postgres"),
                        help="Test container to recreate (default: repo-postgres, or IPTO_PG_CONTAINER_NAME)")
    parser.add_argument("--host-port", type=port_number, default=os.environ.get("IPTO_PG_PORT", "5432"),
                        help="Local port (default: 5432, or IPTO_PG_PORT)")
    parser.add_argument("--image", default=os.environ.get("IPTO_PG_IMAGE", "postgres:16"),
                        help="PostgreSQL image (default: postgres:16, or IPTO_PG_IMAGE)")
    parser.add_argument("--startup-timeout", type=positive_seconds, default=60,
                        help="Readiness timeout in seconds (default: 60)")
    parser.add_argument("--skip-pull", action="store_true", help="Use an already cached image without pulling")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        prepare_database(args)
    except subprocess.CalledProcessError as error:
        detail = (error.stderr or error.stdout or f"Docker exited with status {error.returncode}").strip()
        print(f"Setup failed: {detail}", file=sys.stderr)
        return 1
    except (OSError, RuntimeError, TimeoutError, subprocess.TimeoutExpired) as error:
        print(f"Setup failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

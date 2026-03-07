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

"""Set up a local PostgreSQL instance in Docker, initialise
it with schema and initial data.
"""

import subprocess
import time
import pexpect
import sys
import argparse
import socket

import os

def pull_postgres_image():
    """
    Pull the latest postgres image from Docker Hub.
    """
    print("Pulling the latest 'postgres' image from Docker Hub...")
    result = subprocess.run(["docker", "pull", "postgres"], check=False)
    if result.returncode == 0:
        print("Image pulled successfully.\n")
    else:
        print("\n")
        print("*********************************************************\n")
        print("* Could not pull image. Possibly reusing current image. *\n")
        print("*********************************************************\n")
        print("\n")


def remove_existing_container(container_name: str):
    """
    If a container with the given name exists, remove it.
    """
    print(f"Removing existing container named '{container_name}' (if any)...")
    result = subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Removed old container '{container_name}'.\n")
    else:
        print(f"No existing container '{container_name}' to remove (or removal failed). Proceeding.\n")


def start_postgres_container(container_name: str, postgres_password: str, host_port: int = 5432):
    """
    Start a new postgres container with the specified name, password, and mapped port.
    """
    # The directory in which *this* script is located:
    script_dir = os.path.dirname(os.path.abspath(__file__))

    print(f"Starting new container '{container_name}' on port {host_port} ...")
    result = subprocess.run([
        "docker", "run",
        "-m", "8g",
        "--name", container_name,
        "-e", f"POSTGRES_PASSWORD={postgres_password}",
        "-p", f"{host_port}:5432",
        "-v", f"{script_dir}:/tmp",
        "-d",
        "postgres"
    ], check=False, capture_output=True, text=True)

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        if "port is already allocated" in stderr:
            raise RuntimeError(
                f"Host port {host_port} is already in use. "
                f"Re-run with another port, for example: --host-port 5433"
            )
        raise RuntimeError(f"Could not start container '{container_name}': {stderr}")

    container_id = subprocess.check_output(
        ["docker", "ps", "-qf", f"name={container_name}"], text=True
    ).strip()

    print(f"Container started: {container_id}\n")

    return container_id


def wait_for_container_startup(container_id: str, indication: str, wait_seconds: int = 10):
    """
    Give the container some time to finish startup procedures.
    """
    print("Awaiting startup...")

    shell_cmd = f"docker logs -f {container_id}"
    child = pexpect.spawn(shell_cmd, encoding='utf-8', timeout=wait_seconds)
    # child.logfile = sys.stdout
    try:
        child.expect(indication)
    except pexpect.EOF:
        print("Unexpected end of log.")
    except pexpect.TIMEOUT:
        print("Timeout waiting for setup to complete")
    finally:
        child.close()

def run_psql_commands_in_container(
        container_name: str,
        postgres_password: str,
        commands: list,
        psql_user: str = "postgres",
        psql_host: str = "localhost"
):
    """
    Spawns a docker run psql session (interactive) and executes SQL commands using pexpect.
    """
    print("Running psql commands in container...")

    # Spawn the psql session
    shell_cmd = f"docker exec -it {container_name} psql -h {psql_host} -U {psql_user}"
    child = pexpect.spawn(shell_cmd, encoding="utf-8", timeout=5)
    # child.logfile = sys.stdout

    child.expect("postgres=#", timeout=5)

    # Execute each command
    for cmd in commands:
        child.sendline(cmd)
        # Wait for psql prompt or an ERROR
        # Some psql prompts end in "postgres=#" or similar.
        idx = child.expect(["postgres=#", "ERROR", pexpect.TIMEOUT, pexpect.EOF], timeout=5)
        if idx == 1:
            # If we matched "ERROR"
            print(f"Error running command: {cmd}\nDetail: {child.before}")
        elif idx in (2, 3):
            print(f"Unexpected response or no response for command: {cmd}")

    # Exit from psql
    child.sendline("\\q")
    child.close()
    print("Commands executed. Exiting psql.\n")

def run_sql_commands_in_container(container_name, repo_password, sql_files, repo_user="repo", psql_host="localhost"):
    """
    Connects to psql inside the running container with pexpect,
    then executes each .sql file via \\i /path/to/file.sql.
    """
    shell_cmd = f"docker exec -it {container_name} psql -h {psql_host} -U {repo_user}"

    child = pexpect.spawn(shell_cmd, encoding="utf-8", timeout=10)
    # child.logfile = sys.stdout

    child.expect(repo_user + "=>", timeout=5)

    # For each SQL file, run the psql command "\i <file>"
    for sql_file in sql_files:
        cmd = f"\\i /tmp/{sql_file}"
        child.sendline(cmd)
        idx = child.expect([repo_user + "=>", "ERROR", pexpect.TIMEOUT, pexpect.EOF], timeout=10)
        if idx == 1:
            print(f"Error running command: {cmd}\nDetail: {child.before}")
        elif idx in (2, 3):
            print(f"Unexpected response or no response for command: {cmd}")

    # Exit psql
    child.sendline("\\q")
    child.close()
    print("All SQL files have been executed.\n")

def is_port_available(port: int, host: str = "127.0.0.1") -> bool:
    """
    Return True if host:port can be bound locally, otherwise False.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, port))
            return True
        except OSError:
            return False


def parse_args():
    parser = argparse.ArgumentParser(
        description="Set up a local PostgreSQL instance in Docker for tests."
    )
    parser.add_argument(
        "--container-name",
        default=os.environ.get("IPTO_PG_CONTAINER_NAME", "repo-postgres"),
        help="Docker container name (default: repo-postgres, or IPTO_PG_CONTAINER_NAME)."
    )
    parser.add_argument(
        "--host-port",
        type=int,
        default=int(os.environ.get("IPTO_PG_PORT", "5432")),
        help="Host port to map to container port 5432 (default: 5432, or IPTO_PG_PORT)."
    )
    return parser.parse_args()


def main():
    """
    Orchestrates pulling the image, removing old container, starting a new one,
    and running psql commands.
    """
    args = parse_args()

    container_name = args.container_name
    postgres_password = "H0nd@666"
    host_port = args.host_port

    repo_user = "repo"
    repo_password = "repo"

    # Pull the postgres image
    pull_postgres_image()

    if not is_port_available(host_port):
        print(f"Host port {host_port} is already in use.")
        print(f"Use a different port, for example:")
        print(f"  python3 {os.path.relpath(__file__)} --host-port 5433")
        print("Or stop the process/container using that port.")
        print("Tip: lsof -nP -iTCP:{0} -sTCP:LISTEN".format(host_port))
        sys.exit(2)

    # Remove any existing container
    remove_existing_container(container_name)

    # Start the container
    container_id = start_postgres_container(container_name, postgres_password, host_port)

    # Wait for container to become ready
    indication=r"listening on IPv4 address \"0.0.0.0\", port 5432"
    wait_for_container_startup(container_id, indication, wait_seconds=10)

    # Commands to run as superuser
    commands = [
        "CREATE USER repo WITH PASSWORD '" + repo_password + "';",
        "CREATE DATABASE repo OWNER repo;"
    ]

    # Run commands in the container (as superuser)
    run_psql_commands_in_container(
        container_name=container_name,
        postgres_password=postgres_password,
        commands=commands
    )

    # Run commands in the container as user 'repo'
    sql_files = ["schema.sql", "procedures.sql", "boot.sql"]
    run_sql_commands_in_container(
            container_name=container_name,
            repo_password=repo_password,
            sql_files=sql_files
    )

    print("Setup completed.\n")
    print(f"PostgreSQL is available on localhost:{host_port}")
    print("Update client config if it assumes port 5432.")

if __name__ == "__main__":
    main()

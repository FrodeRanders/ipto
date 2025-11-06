#!/usr/bin/env python3
"""Set up a local PostgreSQL instance in Docker, initialise
it with schema and initial data.
"""

import subprocess
import time
import pexpect
import sys

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


def start_postgres_container(container_name: str, postgres_password: str, host_port: int = 1402):
    """
    Start a new postgres container with the specified name, password, and mapped port.
    """
    # The directory in which *this* script is located:
    script_dir = os.path.dirname(os.path.abspath(__file__))

    print(f"Starting new container '{container_name}' on port {host_port} ...")
    subprocess.run([
        "docker", "run",
        "-m", "8g",
        "--name", container_name,
        "-e", f"POSTGRES_PASSWORD={postgres_password}",
        "-p", f"{host_port}:5432",
        "-v", f"{script_dir}:/tmp",
        "-d",
        "postgres"
    ], check=True)

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


def main():
    """
    Orchestrates pulling the image, removing old container, starting a new one,
    and running psql commands.
    """
    container_name = "repo-postgres"
    postgres_password = "H0nd@666"
    host_port = 1402

    repo_user = "repo"
    repo_password = "repo"

    # Pull the postgres image
    pull_postgres_image()

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

if __name__ == "__main__":
    main()

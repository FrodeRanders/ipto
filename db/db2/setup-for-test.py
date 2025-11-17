#!/usr/bin/env python3
"""Set up a local IBM DB2 Community Edition instance in Docker, initialise
it with schema, initial data and pull the matching JDBC drivers into the
local Maven repo.
"""

from __future__ import annotations

import re
import os
import subprocess
from pathlib import Path
import pexpect

def pull_db2_image(tag: str = "latest") -> None:
    """Pull the requested DB2 image tag from Docker Hub."""
    image = f"ibmcom/db2:{tag}"
    print(f"Pulling Docker image '{image}' …")
    subprocess.run(["docker", "pull", image], check=True)
    print("Image pulled successfully\n")


def remove_existing_container(name: str) -> None:
    print(f"Removing any existing container named '{name}' …")
    subprocess.run(["docker", "rm", "-f", name], check=False)
    print("Done (ignored any errors if the container was absent).\n")


def start_db2_container(
    name: str,
    db2inst1_password: str,
    image_tag: str = "latest",
    host_port: int = 50000,
) -> str:
    """Run the container detached and return its ID."""
    image = f"ibmcom/db2:{image_tag}"
    script_dir = Path(__file__).resolve().parent

    print(f"Starting DB2 container '{name}' (image '{image}') → 0.0.0.0:{host_port} …")
    subprocess.run(
        [
            "docker",
            "run",
            "--privileged",
            "--name",
            name,
            "-e",
            "LICENSE=accept",
            "-e",
            f"DB2INST1_PASSWORD={db2inst1_password}",
            "-p",
            f"{host_port}:50000",
             "-v",
            f"{script_dir}:/ipto:ro",  # Read-only mount for SQL- and JAR-files
            "-e",
            "LANG=en_US.UTF-8",
            "-e",
            "LANGUAGE=en_US:en",
            "-d",
            image,
        ],
        check=True,
    )

    container_id = (
        subprocess.check_output(["docker", "ps", "-qf", f"name={name}"], text=True)
        .strip()
    )
    print(f"Container started: {container_id}\n")
    return container_id


def wait_for_db2_startup(container_id: str, timeout: int = 180) -> None:
    """Tail logs until DB2 reports it is ready."""
    print("Waiting for DB2 to finish initialisation …")

    ready_regex = r"DB2START processing was successful"
    child = pexpect.spawn(f"docker logs -f {container_id}", encoding="utf-8")
    try:
        child.expect(ready_regex, timeout=timeout)
    except (pexpect.EOF, pexpect.TIMEOUT):
        print("Did not detect successful startup within timeout.")
    finally:
        child.close()

    print("DB2 is ready.\n")


def _docker_exec(
    container: str,
    user: str,
    shell_cmd: str,
    capture: bool = False,
    check: bool = True,
):
    """Run *shell_cmd* inside *container* as *user*.

    Uses a *login* shell (`bash -lc`) so that `/home/db2inst1/.bashrc`
    runs and Db2 binaries are on the PATH.
    """
    full_cmd = [
        "docker",
        "exec",
        "-u",
        user,
        container,
        "bash",
        "-lc", # login shell → loads environment
        shell_cmd,
    ]
    if capture:
        return subprocess.check_output(full_cmd, text=True)
    else:
        subprocess.run(full_cmd, check=check)
        return None

def exec_as_db2inst1(container: str, shell_cmd: str, *, capture: bool = False):
    return _docker_exec(container, "db2inst1", shell_cmd, capture=capture)

def create_database(container: str, db_name: str = "REPO") -> None:
    print(f"Creating database '{db_name}' …")
    exec_as_db2inst1(container, f"db2 create database {db_name}")
    print("Database created.\n")

def run_sql_files(container: str, db_name: str, files: list[str]) -> None:
    """Pipe a *connect* statement plus the file into a single CLP session."""
    print("Running SQL files …")
    for file in files:
        print(f" • {file}")
        cmd = (
            f'(echo "connect to {db_name};"; cat /ipto/{file}) | '
            "db2 -tv "
        )
        exec_as_db2inst1(container, cmd)
        print("All files executed.")


def get_db2_version(container: str) -> str | None:
    """Return *11.5.8.0* style version by parsing the output of `db2level`."""
    out = exec_as_db2inst1(container, "db2level", capture=True)
    m = re.search(r"Informational tokens are \"DB2 v([0-9.]+)", out or "")
    return m.group(1) if m else None

def extract_jdbc_drivers(container: str, dest_dir: Path) -> list[Path]:
    print("Extract JDBC driver jars from container …")
    jars = {
        "db2jcc4.jar": "/database/config/db2inst1/sqllib/java/db2jcc4.jar",
        "db2jcc_license_cu.jar": "/database/config/db2inst1/sqllib/java/db2jcc_license_cu.jar",
    }
    copied: list[Path] = []
    for local_name, container_path in jars.items():
        local_path = dest_dir / local_name
        subprocess.run(
            ["docker", "cp", f"{container}:{container_path}", str(local_path)],
            check=True,
        )
        copied.append(local_path)
        print(f"  • {local_path}")
    print("JDBC jars extracted.\n")
    return copied

def extract_mvn_artifact(coordinate: str, destination : Path = Path(__file__).resolve().parent) -> None:
    cmd = [
        "mvn", "-q", "-B", "-ntp",
        "dependency:copy",
        f"-Dartifact={coordinate}",
        f"-DoutputDirectory={destination}",
        "-DuseBaseVersion=true",
    ]
    subprocess.run(cmd, check=True)
    print("Maven artifact extracted.\n")


def install_jdbc_into_maven(jars: list[Path], version: str) -> None:
    print(f"Installing JDBC drivers into the local Maven repo (version={version}) …")
    for jar in jars:
        is_license = "license" in jar.name
        cmd = [
            "mvn",
            "install:install-file",
            "-DgroupId=ibm.db2",
            "-DartifactId=db2jcc4",
            f"-Dversion={version}",
            "-Dpackaging=jar",
            f"-Dfile={jar}",
        ]
        if is_license:
            cmd += ["-Dclassifier=license", "-DgeneratePom=false"]
        else:
            cmd.append("-DgeneratePom=true")
        subprocess.run(cmd, check=True)
    print("Maven installation complete.\n")

def main() -> None:
    container_name = "repo-db2"

    # Adjust as needed
    db2_tag = "latest"              # Specific version or 'latest'
    host_port = 50000
    db2_password = "H0nd@666"       # Please replace...
    database_name = "REPO"
    # sql_files = ["schema.sql", "load-udf.sql", "procedures.sql", "boot.sql"]
    sql_files = ["schema.sql", "load-udf.sql", "procedures.sql", "boot.sql"]

    #
    extract_mvn_artifact("org.gautelis:repo-udpf:1.0-SNAPSHOT:jar")

    #
    pull_db2_image(db2_tag)
    remove_existing_container(container_name)
    container_id = start_db2_container(
        container_name,
        db2_password,
        image_tag=db2_tag,
        host_port=host_port,
    )
    wait_for_db2_startup(container_id)

    #
    create_database(container_id, database_name)
    run_sql_files(container_id, database_name, sql_files)

    effective_version = db2_tag
    if db2_tag == "latest":
        detected = get_db2_version(container_id)
        if detected:
            effective_version = detected
            print(f"Detected DB2 version: {effective_version}\n")
        else:
            print("Could not detect DB2 version; using 'latest'.\n")

    jars = extract_jdbc_drivers(container_id, Path(__file__).resolve().parent)
    install_jdbc_into_maven(jars, effective_version)

    print("DB2 test environment ready, JDBC drivers installed.")


if __name__ == "__main__":
    main()


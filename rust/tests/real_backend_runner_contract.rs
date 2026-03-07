use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn make_temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("ipto-{label}-{nanos}"));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn write_executable(path: &Path, content: &str) {
    fs::write(path, content).expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(path).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).expect("chmod");
    }
}

#[test]
fn runner_help_prints_usage() {
    let output = Command::new("bash")
        .current_dir(repo_root())
        .arg("scripts/run_real_backend_tests.sh")
        .arg("--help")
        .output()
        .expect("run help");

    assert!(output.status.success(), "help should exit 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Usage: scripts/run_real_backend_tests.sh"));
    assert!(stdout.contains("--backend <all|postgres|neo4j>"));
}

#[test]
fn runner_selects_postgres_integration_only() {
    let temp = make_temp_dir("runner-contract");
    let fake_bin = temp.join("bin");
    fs::create_dir_all(&fake_bin).expect("create fake bin");
    let log_file = temp.join("calls.log");

    write_executable(
        &fake_bin.join("docker"),
        r#"#!/usr/bin/env bash
set -euo pipefail
echo "docker $*" >> "${IPTO_TEST_LOG}"
exit 0
"#,
    );
    write_executable(
        &fake_bin.join("cargo"),
        r#"#!/usr/bin/env bash
set -euo pipefail
echo "cargo $*" >> "${IPTO_TEST_LOG}"
exit 0
"#,
    );

    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = Command::new("bash")
        .current_dir(repo_root())
        .arg("scripts/run_real_backend_tests.sh")
        .args(["--backend", "postgres", "--scope", "integration", "--no-up"])
        .env("PATH", path)
        .env("IPTO_TEST_LOG", &log_file)
        .output()
        .expect("run contract test");

    assert!(
        output.status.success(),
        "script should succeed, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let calls = fs::read_to_string(&log_file).expect("read log");
    assert!(
        calls.contains("docker compose exec -T postgres pg_isready"),
        "expected postgres readiness check, got:\n{calls}"
    );
    assert!(
        calls.contains("cargo test --test postgres_integration"),
        "expected postgres integration test run, got:\n{calls}"
    );
    assert!(
        !calls.contains("neo4j_integration"),
        "should not run neo4j integration in postgres-only mode, got:\n{calls}"
    );
    assert!(
        !calls.contains("backend_parity"),
        "should not run parity in integration-only mode, got:\n{calls}"
    );
}

#[test]
fn runner_selects_neo4j_parity_only() {
    let temp = make_temp_dir("runner-contract-neo4j");
    let fake_bin = temp.join("bin");
    fs::create_dir_all(&fake_bin).expect("create fake bin");
    let log_file = temp.join("calls.log");

    write_executable(
        &fake_bin.join("docker"),
        r#"#!/usr/bin/env bash
set -euo pipefail
echo "docker $*" >> "${IPTO_TEST_LOG}"
exit 0
"#,
    );
    write_executable(
        &fake_bin.join("cargo"),
        r#"#!/usr/bin/env bash
set -euo pipefail
echo "cargo $*" >> "${IPTO_TEST_LOG}"
exit 0
"#,
    );

    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = Command::new("bash")
        .current_dir(repo_root())
        .arg("scripts/run_real_backend_tests.sh")
        .args(["--backend", "neo4j", "--scope", "parity", "--no-up"])
        .env("PATH", path)
        .env("IPTO_TEST_LOG", &log_file)
        .output()
        .expect("run contract test");

    assert!(
        output.status.success(),
        "script should succeed, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let calls = fs::read_to_string(&log_file).expect("read log");
    assert!(
        calls.contains("docker compose exec -T neo4j cypher-shell"),
        "expected neo4j readiness check, got:\n{calls}"
    );
    assert!(
        calls.contains("cargo test --test backend_parity parity_neo4j_core_flow"),
        "expected neo4j parity test run, got:\n{calls}"
    );
    assert!(
        !calls.contains("postgres_integration"),
        "should not run postgres integration in neo4j parity mode, got:\n{calls}"
    );
    assert!(
        !calls.contains("parity_postgres_core_flow"),
        "should not run postgres parity in neo4j-only mode, got:\n{calls}"
    );
}

#[test]
fn runner_down_volumes_triggers_compose_down_v() {
    let temp = make_temp_dir("runner-contract-down");
    let fake_bin = temp.join("bin");
    fs::create_dir_all(&fake_bin).expect("create fake bin");
    let log_file = temp.join("calls.log");

    write_executable(
        &fake_bin.join("docker"),
        r#"#!/usr/bin/env bash
set -euo pipefail
echo "docker $*" >> "${IPTO_TEST_LOG}"
exit 0
"#,
    );
    write_executable(
        &fake_bin.join("cargo"),
        r#"#!/usr/bin/env bash
set -euo pipefail
echo "cargo $*" >> "${IPTO_TEST_LOG}"
exit 0
"#,
    );

    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = Command::new("bash")
        .current_dir(repo_root())
        .arg("scripts/run_real_backend_tests.sh")
        .args([
            "--backend",
            "postgres",
            "--scope",
            "integration",
            "--no-up",
            "--down-volumes",
        ])
        .env("PATH", path)
        .env("IPTO_TEST_LOG", &log_file)
        .output()
        .expect("run contract test");

    assert!(
        output.status.success(),
        "script should succeed, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let calls = fs::read_to_string(&log_file).expect("read log");
    assert!(
        calls.contains("docker compose down -v"),
        "expected teardown with volumes, got:\n{calls}"
    );
}

#[test]
fn runner_verbose_adds_nocapture_to_test_commands() {
    let temp = make_temp_dir("runner-contract-verbose");
    let fake_bin = temp.join("bin");
    fs::create_dir_all(&fake_bin).expect("create fake bin");
    let log_file = temp.join("calls.log");

    write_executable(
        &fake_bin.join("docker"),
        r#"#!/usr/bin/env bash
set -euo pipefail
echo "docker $*" >> "${IPTO_TEST_LOG}"
exit 0
"#,
    );
    write_executable(
        &fake_bin.join("cargo"),
        r#"#!/usr/bin/env bash
set -euo pipefail
echo "cargo $*" >> "${IPTO_TEST_LOG}"
exit 0
"#,
    );

    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = Command::new("bash")
        .current_dir(repo_root())
        .arg("scripts/run_real_backend_tests.sh")
        .args([
            "--backend",
            "postgres",
            "--scope",
            "integration",
            "--no-up",
            "--verbose",
        ])
        .env("PATH", path)
        .env("IPTO_TEST_LOG", &log_file)
        .output()
        .expect("run contract test");

    assert!(
        output.status.success(),
        "script should succeed, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let calls = fs::read_to_string(&log_file).expect("read log");
    assert!(
        calls.contains("cargo test --test postgres_integration -- --nocapture"),
        "expected verbose cargo command with --nocapture, got:\n{calls}"
    );
}

#[test]
fn runner_all_parity_routes_to_combined_backend_parity_test() {
    let temp = make_temp_dir("runner-contract-all-parity");
    let fake_bin = temp.join("bin");
    fs::create_dir_all(&fake_bin).expect("create fake bin");
    let log_file = temp.join("calls.log");

    write_executable(
        &fake_bin.join("docker"),
        r#"#!/usr/bin/env bash
set -euo pipefail
echo "docker $*" >> "${IPTO_TEST_LOG}"
exit 0
"#,
    );
    write_executable(
        &fake_bin.join("cargo"),
        r#"#!/usr/bin/env bash
set -euo pipefail
echo "cargo $*" >> "${IPTO_TEST_LOG}"
exit 0
"#,
    );

    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = Command::new("bash")
        .current_dir(repo_root())
        .arg("scripts/run_real_backend_tests.sh")
        .args(["--backend", "all", "--scope", "parity", "--no-up"])
        .env("PATH", path)
        .env("IPTO_TEST_LOG", &log_file)
        .output()
        .expect("run contract test");

    assert!(
        output.status.success(),
        "script should succeed, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let calls = fs::read_to_string(&log_file).expect("read log");
    assert!(
        calls.contains("docker compose exec -T postgres pg_isready"),
        "expected postgres readiness check, got:\n{calls}"
    );
    assert!(
        calls.contains("docker compose exec -T neo4j cypher-shell"),
        "expected neo4j readiness check, got:\n{calls}"
    );
    assert!(
        calls.contains("cargo test --test backend_parity"),
        "expected combined backend parity test run, got:\n{calls}"
    );
    assert!(
        !calls.contains("parity_postgres_core_flow"),
        "should not filter parity tests in all-backend mode, got:\n{calls}"
    );
    assert!(
        !calls.contains("parity_neo4j_core_flow"),
        "should not filter parity tests in all-backend mode, got:\n{calls}"
    );
}

#[test]
fn runner_postgres_backend_sets_only_postgres_integration_env() {
    let temp = make_temp_dir("runner-contract-env-postgres");
    let fake_bin = temp.join("bin");
    fs::create_dir_all(&fake_bin).expect("create fake bin");
    let log_file = temp.join("calls.log");

    write_executable(
        &fake_bin.join("docker"),
        r#"#!/usr/bin/env bash
set -euo pipefail
echo "docker $*" >> "${IPTO_TEST_LOG}"
exit 0
"#,
    );
    write_executable(
        &fake_bin.join("cargo"),
        r#"#!/usr/bin/env bash
set -euo pipefail
echo "cargo $*" >> "${IPTO_TEST_LOG}"
echo "env PG=${IPTO_PG_INTEGRATION:-} NEO4J=${IPTO_NEO4J_INTEGRATION:-}" >> "${IPTO_TEST_LOG}"
exit 0
"#,
    );

    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = Command::new("bash")
        .current_dir(repo_root())
        .arg("scripts/run_real_backend_tests.sh")
        .args(["--backend", "postgres", "--scope", "integration", "--no-up"])
        .env("PATH", path)
        .env("IPTO_TEST_LOG", &log_file)
        .output()
        .expect("run contract test");

    assert!(
        output.status.success(),
        "script should succeed, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let calls = fs::read_to_string(&log_file).expect("read log");
    assert!(
        calls.contains("env PG=1 NEO4J="),
        "expected postgres-only integration env, got:\n{calls}"
    );
}

#[test]
fn runner_neo4j_backend_sets_only_neo4j_integration_env() {
    let temp = make_temp_dir("runner-contract-env-neo4j");
    let fake_bin = temp.join("bin");
    fs::create_dir_all(&fake_bin).expect("create fake bin");
    let log_file = temp.join("calls.log");

    write_executable(
        &fake_bin.join("docker"),
        r#"#!/usr/bin/env bash
set -euo pipefail
echo "docker $*" >> "${IPTO_TEST_LOG}"
exit 0
"#,
    );
    write_executable(
        &fake_bin.join("cargo"),
        r#"#!/usr/bin/env bash
set -euo pipefail
echo "cargo $*" >> "${IPTO_TEST_LOG}"
echo "env PG=${IPTO_PG_INTEGRATION:-} NEO4J=${IPTO_NEO4J_INTEGRATION:-}" >> "${IPTO_TEST_LOG}"
exit 0
"#,
    );

    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = Command::new("bash")
        .current_dir(repo_root())
        .arg("scripts/run_real_backend_tests.sh")
        .args(["--backend", "neo4j", "--scope", "integration", "--no-up"])
        .env("PATH", path)
        .env("IPTO_TEST_LOG", &log_file)
        .output()
        .expect("run contract test");

    assert!(
        output.status.success(),
        "script should succeed, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let calls = fs::read_to_string(&log_file).expect("read log");
    assert!(
        calls.contains("env PG= NEO4J=1"),
        "expected neo4j-only integration env, got:\n{calls}"
    );
}

#[test]
fn runner_all_backend_sets_both_integration_envs() {
    let temp = make_temp_dir("runner-contract-env-all");
    let fake_bin = temp.join("bin");
    fs::create_dir_all(&fake_bin).expect("create fake bin");
    let log_file = temp.join("calls.log");

    write_executable(
        &fake_bin.join("docker"),
        r#"#!/usr/bin/env bash
set -euo pipefail
echo "docker $*" >> "${IPTO_TEST_LOG}"
exit 0
"#,
    );
    write_executable(
        &fake_bin.join("cargo"),
        r#"#!/usr/bin/env bash
set -euo pipefail
echo "cargo $*" >> "${IPTO_TEST_LOG}"
echo "env PG=${IPTO_PG_INTEGRATION:-} NEO4J=${IPTO_NEO4J_INTEGRATION:-}" >> "${IPTO_TEST_LOG}"
exit 0
"#,
    );

    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = Command::new("bash")
        .current_dir(repo_root())
        .arg("scripts/run_real_backend_tests.sh")
        .args(["--backend", "all", "--scope", "integration", "--no-up"])
        .env("PATH", path)
        .env("IPTO_TEST_LOG", &log_file)
        .output()
        .expect("run contract test");

    assert!(
        output.status.success(),
        "script should succeed, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let calls = fs::read_to_string(&log_file).expect("read log");
    let env_marker = "env PG=1 NEO4J=1";
    let env_marker_count = calls.matches(env_marker).count();
    assert!(
        env_marker_count >= 2,
        "expected both integration env flags for both integration commands, got:\n{calls}"
    );
}

#[test]
fn runner_invalid_backend_exits_with_usage_hint() {
    let output = Command::new("bash")
        .current_dir(repo_root())
        .arg("scripts/run_real_backend_tests.sh")
        .args(["--backend", "invalid-backend", "--no-up"])
        .output()
        .expect("run invalid backend");

    assert_eq!(
        output.status.code(),
        Some(2),
        "invalid backend should exit 2"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Invalid --backend 'invalid-backend'"),
        "expected invalid backend message, got:\n{stderr}"
    );
    assert!(
        stderr.contains("Usage: scripts/run_real_backend_tests.sh"),
        "expected usage hint on invalid backend, got:\n{stderr}"
    );
}

#[test]
fn runner_invalid_scope_exits_with_usage_hint() {
    let output = Command::new("bash")
        .current_dir(repo_root())
        .arg("scripts/run_real_backend_tests.sh")
        .args(["--scope", "invalid-scope", "--no-up"])
        .output()
        .expect("run invalid scope");

    assert_eq!(output.status.code(), Some(2), "invalid scope should exit 2");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Invalid --scope 'invalid-scope'"),
        "expected invalid scope message, got:\n{stderr}"
    );
    assert!(
        stderr.contains("Usage: scripts/run_real_backend_tests.sh"),
        "expected usage hint on invalid scope, got:\n{stderr}"
    );
}

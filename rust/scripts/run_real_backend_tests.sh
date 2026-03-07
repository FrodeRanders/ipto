#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

BACKEND="all"
SCOPE="all"
DO_UP=1
DO_DOWN=0
DOWN_VOLUMES=0
VERBOSE=0

usage() {
  cat <<'EOF'
Usage: scripts/run_real_backend_tests.sh [options]

Run integration/parity tests against real PostgreSQL and/or Neo4j backends.

Options:
  --backend <all|postgres|neo4j>  Backend selection (default: all)
  --scope <all|integration|parity> Test scope (default: all)
  --no-up                          Do not run `docker compose up -d`
  --down                           Run `docker compose down` when finished
  --down-volumes                   Run `docker compose down -v` when finished
  --verbose                        Print test output with `--nocapture`
  -h, --help                       Show this help

Examples:
  scripts/run_real_backend_tests.sh
  scripts/run_real_backend_tests.sh --backend postgres --scope integration
  scripts/run_real_backend_tests.sh --backend neo4j --scope parity --no-up --verbose
  scripts/run_real_backend_tests.sh --down --down-volumes
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --backend)
      BACKEND="${2:-}"
      shift 2
      ;;
    --scope)
      SCOPE="${2:-}"
      shift 2
      ;;
    --no-up)
      DO_UP=0
      shift
      ;;
    --down)
      DO_DOWN=1
      shift
      ;;
    --down-volumes)
      DO_DOWN=1
      DOWN_VOLUMES=1
      shift
      ;;
    --verbose)
      VERBOSE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 2
      ;;
  esac
done

case "${BACKEND}" in
  all|postgres|neo4j) ;;
  *)
    echo "Invalid --backend '${BACKEND}' (expected all|postgres|neo4j)" >&2
    usage >&2
    exit 2
    ;;
esac

case "${SCOPE}" in
  all|integration|parity) ;;
  *)
    echo "Invalid --scope '${SCOPE}' (expected all|integration|parity)" >&2
    usage >&2
    exit 2
    ;;
esac

# Environment defaults used by Rust backends and tests
export IPTO_PG_HOST="${IPTO_PG_HOST:-localhost}"
export IPTO_PG_PORT="${IPTO_PG_PORT:-5432}"
export IPTO_PG_USER="${IPTO_PG_USER:-repo}"
export IPTO_PG_PASSWORD="${IPTO_PG_PASSWORD:-repo}"
export IPTO_PG_DATABASE="${IPTO_PG_DATABASE:-repo}"

export IPTO_NEO4J_HTTP_PORT="${IPTO_NEO4J_HTTP_PORT:-7474}"
export IPTO_NEO4J_BOLT_PORT="${IPTO_NEO4J_BOLT_PORT:-7687}"
export IPTO_NEO4J_URL="${IPTO_NEO4J_URL:-http://localhost:${IPTO_NEO4J_HTTP_PORT}}"
export IPTO_NEO4J_DATABASE="${IPTO_NEO4J_DATABASE:-neo4j}"
export IPTO_NEO4J_USER="${IPTO_NEO4J_USER:-neo4j}"
export IPTO_NEO4J_PASSWORD="${IPTO_NEO4J_PASSWORD:-neo4jtest}"
export IPTO_NEO4J_AUTH="${IPTO_NEO4J_AUTH:-${IPTO_NEO4J_USER}/${IPTO_NEO4J_PASSWORD}}"
export IPTO_NEO4J_BOOTSTRAP="${IPTO_NEO4J_BOOTSTRAP:-true}"

if [[ "${BACKEND}" == "all" || "${BACKEND}" == "postgres" ]]; then
  export IPTO_PG_INTEGRATION=1
fi
if [[ "${BACKEND}" == "all" || "${BACKEND}" == "neo4j" ]]; then
  export IPTO_NEO4J_INTEGRATION=1
fi

compose_down() {
  if [[ "${DO_DOWN}" -eq 1 ]]; then
    if [[ "${DOWN_VOLUMES}" -eq 1 ]]; then
      docker compose down -v
    else
      docker compose down
    fi
  fi
}

trap compose_down EXIT

wait_for_postgres() {
  local timeout_seconds=120
  local start_ts
  start_ts="$(date +%s)"
  until docker compose exec -T postgres pg_isready -U "${IPTO_PG_USER}" -d "${IPTO_PG_DATABASE}" >/dev/null 2>&1; do
    if (( "$(date +%s)" - start_ts > timeout_seconds )); then
      echo "PostgreSQL did not become ready within ${timeout_seconds}s" >&2
      exit 1
    fi
    sleep 2
  done
}

wait_for_neo4j() {
  local timeout_seconds=180
  local start_ts
  start_ts="$(date +%s)"
  until docker compose exec -T neo4j cypher-shell \
      -u "${IPTO_NEO4J_USER}" \
      -p "${IPTO_NEO4J_PASSWORD}" \
      "RETURN 1;" >/dev/null 2>&1; do
    if (( "$(date +%s)" - start_ts > timeout_seconds )); then
      echo "Neo4j did not become ready within ${timeout_seconds}s" >&2
      exit 1
    fi
    sleep 2
  done
}

run_test() {
  local cmd="$1"
  echo "+ ${cmd}"
  eval "${cmd}"
}

test_flags=()
if [[ "${VERBOSE}" -eq 1 ]]; then
  test_flags+=(-- --nocapture)
fi

if [[ "${DO_UP}" -eq 1 ]]; then
  if [[ "${BACKEND}" == "all" ]]; then
    docker compose up -d postgres neo4j
  elif [[ "${BACKEND}" == "postgres" ]]; then
    docker compose up -d postgres
  else
    docker compose up -d neo4j
  fi
fi

if [[ "${BACKEND}" == "all" || "${BACKEND}" == "postgres" ]]; then
  wait_for_postgres
fi
if [[ "${BACKEND}" == "all" || "${BACKEND}" == "neo4j" ]]; then
  wait_for_neo4j
fi

if [[ "${SCOPE}" == "all" || "${SCOPE}" == "integration" ]]; then
  if [[ "${BACKEND}" == "all" || "${BACKEND}" == "postgres" ]]; then
    run_test "cargo test --test postgres_integration ${test_flags[*]:-}"
  fi
  if [[ "${BACKEND}" == "all" || "${BACKEND}" == "neo4j" ]]; then
    run_test "cargo test --test neo4j_integration ${test_flags[*]:-}"
  fi
fi

if [[ "${SCOPE}" == "all" || "${SCOPE}" == "parity" ]]; then
  if [[ "${BACKEND}" == "all" ]]; then
    run_test "cargo test --test backend_parity ${test_flags[*]:-}"
  elif [[ "${BACKEND}" == "postgres" ]]; then
    run_test "cargo test --test backend_parity parity_postgres_core_flow ${test_flags[*]:-}"
  else
    run_test "cargo test --test backend_parity parity_neo4j_core_flow ${test_flags[*]:-}"
  fi
fi

echo "Real-backend test run finished successfully."

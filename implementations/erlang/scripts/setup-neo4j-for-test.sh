#!/usr/bin/env sh
set -eu

CONTAINER_NAME="${IPTO_NEO4J_CONTAINER:-ipto-neo4j}"
IMAGE="${IPTO_NEO4J_IMAGE:-neo4j:latest}"
HTTP_PORT="${IPTO_NEO4J_HTTP_PORT:-7474}"
BOLT_PORT="${IPTO_NEO4J_BOLT_PORT:-7687}"
USER="${IPTO_NEO4J_USER:-neo4j}"
PASSWORD="${IPTO_NEO4J_PASSWORD:-neo4j-repo}"
DATABASE="${IPTO_NEO4J_DATABASE:-neo4j}"
URL="http://127.0.0.1:${HTTP_PORT}/db/${DATABASE}/tx/commit"

usage() {
  cat <<EOF
Usage: $0 <start|stop|status|logs>

Environment overrides:
  IPTO_NEO4J_CONTAINER   (default: ipto-neo4j)
  IPTO_NEO4J_IMAGE       (default: neo4j:latest)
  IPTO_NEO4J_HTTP_PORT   (default: 7474)
  IPTO_NEO4J_BOLT_PORT   (default: 7687)
  IPTO_NEO4J_USER        (default: neo4j)
  IPTO_NEO4J_PASSWORD    (default: neo4j-repo)
  IPTO_NEO4J_DATABASE    (default: neo4j)
EOF
}

wait_ready() {
  i=0
  while [ "$i" -lt 60 ]; do
    body="$(curl -s -u "${USER}:${PASSWORD}" -H 'Content-Type: application/json' -d '{"statements":[{"statement":"RETURN 1"}]}' "${URL}" || true)"
    if echo "$body" | grep -q '"errors":[[:space:]]*\[\]'; then
      echo "Neo4j is ready at ${URL}"
      return 0
    fi
    i=$((i + 1))
    sleep 2
  done
  echo "Neo4j did not become ready in time. Recent container logs:"
  docker logs --tail 80 "${CONTAINER_NAME}" || true
  return 1
}

start() {
  echo "Pulling ${IMAGE}..."
  docker pull "${IMAGE}" >/dev/null
  if docker ps -a --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
    echo "Removing existing container ${CONTAINER_NAME}..."
    docker rm -f "${CONTAINER_NAME}" >/dev/null
  fi
  echo "Starting ${CONTAINER_NAME} on ports ${HTTP_PORT}/${BOLT_PORT}..."
  docker run -d \
    --name "${CONTAINER_NAME}" \
    -p "${HTTP_PORT}:7474" \
    -p "${BOLT_PORT}:7687" \
    -e "NEO4J_AUTH=${USER}/${PASSWORD}" \
    "${IMAGE}" >/dev/null
  wait_ready
}

stop() {
  if docker ps -a --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
    docker rm -f "${CONTAINER_NAME}" >/dev/null
    echo "Removed ${CONTAINER_NAME}"
  else
    echo "Container ${CONTAINER_NAME} does not exist"
  fi
}

status() {
  docker ps --filter "name=^/${CONTAINER_NAME}$" --format '{{.Names}} {{.Status}} {{.Ports}}'
}

logs() {
  docker logs --tail 100 "${CONTAINER_NAME}"
}

cmd="${1:-}"
case "$cmd" in
  start) start ;;
  stop) stop ;;
  status) status ;;
  logs) logs ;;
  *) usage; exit 1 ;;
esac

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/docker/docker-compose.yml"

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  docker compose -f "${COMPOSE_FILE}" up kafka-init --abort-on-container-exit
elif command -v podman-compose >/dev/null 2>&1; then
  podman-compose -f "${COMPOSE_FILE}" up kafka-init --abort-on-container-exit
else
  echo "Error: docker compose or podman-compose is required." >&2
  exit 1
fi

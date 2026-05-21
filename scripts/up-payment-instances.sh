#!/usr/bin/env bash
# Start payment-service with multiple Kafka consumers (same group, partition balancing).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_SERVICES="${ROOT}/docker/services/docker-compose.yml"
COMPOSE_SCALE="${ROOT}/docker/services/docker-compose.payment-scale.yml"

MODE="${PAYMENT_DOCKER_MODE:-named}"
REPLICAS="${PAYMENT_SERVICE_REPLICAS:-2}"

cd "${ROOT}"

if ! podman network exists eventflow-network 2>/dev/null; then
  echo "Create infrastructure first: podman-compose -f docker/docker-compose.yml up -d"
  exit 1
fi

OTHER_SERVICES=(order-service stock-service notification-service dlq-service)

case "${MODE}" in
  named)
    PROFILES=()
    PAYMENT_SERVICES=(payment-service-1 payment-service-2)
    if [[ "${REPLICAS}" -ge 3 ]]; then
      PROFILES+=(--profile payment-scale-3)
      PAYMENT_SERVICES+=(payment-service-3)
    fi
    echo "Starting payment instances: ${PAYMENT_SERVICES[*]}"
    podman-compose -f "${COMPOSE_SERVICES}" "${PROFILES[@]}" up -d --build \
      "${OTHER_SERVICES[@]}" "${PAYMENT_SERVICES[@]}"
    ;;
  scale)
    if [[ "${REPLICAS}" -lt 1 ]]; then
      echo "PAYMENT_SERVICE_REPLICAS must be >= 1"
      exit 1
    fi
    echo "Starting payment-service with --scale ${REPLICAS}"
    podman-compose -f "${COMPOSE_SERVICES}" -f "${COMPOSE_SCALE}" \
      --profile payment-compose-scale up -d --build \
      --scale "payment-service=${REPLICAS}" \
      "${OTHER_SERVICES[@]}" payment-service
    ;;
  *)
    echo "Unknown PAYMENT_DOCKER_MODE=${MODE} (use: named | scale)"
    exit 1
    ;;
esac

echo "Consumer group: eventflow.payment-service (check Kafka UI → Consumers)"
podman ps --filter "name=eventflow-payment-service" --format "table {{.Names}}\t{{.Status}}"

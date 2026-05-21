#!/usr/bin/env bash
# Rebuild and recreate EventFlow app containers (fixes stale images without Kafka consumer).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

echo "→ Rebuilding service images..."
podman-compose -f docker/services/docker-compose.yml build \
  order-service payment-service-1 stock-service notification-service dlq-service

echo "→ Recreating containers..."
podman-compose -f docker/services/docker-compose.yml up -d --force-recreate \
  order-service payment-service-1 payment-service-2 stock-service notification-service dlq-service

echo "→ Recreating kafka-init (order.events topic)..."
podman-compose -f docker/docker-compose.yml up -d --force-recreate kafka-init

echo "→ Payment logs (expect Kafka consumer + groupId):"
sleep 5
podman logs --tail 15 eventflow-payment-service-1 2>&1 | grep -E "Kafka|Consumer|EVENT RECEIVED|groupId" || podman logs --tail 15 eventflow-payment-service-1

echo "Done. POST a new order, then: podman logs -f eventflow-payment-service-1"

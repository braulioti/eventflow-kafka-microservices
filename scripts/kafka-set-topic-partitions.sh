#!/usr/bin/env bash
# Increase partition count for aggregate topics (safe to run multiple times).
#
# Usage (host with Kafka on localhost:9092):
#   npm run kafka:partitions
#
# Usage (inside Docker network):
#   KAFKA_BOOTSTRAP_SERVERS=kafka:29092 bash scripts/kafka-set-topic-partitions.sh
#
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"

TOPICS=(
  "order.events"
  "payment.events"
)

run_kafka_topics() {
  if command -v kafka-topics >/dev/null 2>&1; then
    kafka-topics "$@"
    return
  fi
  local container="${KAFKA_CONTAINER:-eventflow-kafka}"
  local bootstrap="${KAFKA_CONTAINER_BOOTSTRAP:-kafka:29092}"
  if command -v podman >/dev/null 2>&1 && podman container exists "${container}" 2>/dev/null; then
    podman exec "${container}" kafka-topics --bootstrap-server "${bootstrap}" "$@"
    return
  fi
  echo "kafka-topics not found and container ${container} unavailable" >&2
  exit 1
}

echo "Bootstrap: ${BOOTSTRAP}"
echo "Target partitions: ${PARTITIONS}"
echo ""

for topic in "${TOPICS[@]}"; do
  echo "→ Altering ${topic}..."
  if run_kafka_topics --describe --topic "${topic}" >/dev/null 2>&1; then
    run_kafka_topics --alter --topic "${topic}" --partitions "${PARTITIONS}"
    run_kafka_topics --describe --topic "${topic}" | grep -E "Topic:|PartitionCount|Partition:"
  else
    echo "   Topic ${topic} does not exist — run kafka-init first."
  fi
  echo ""
done

echo "Done. Restart payment replicas to rebalance:"
echo "  podman-compose -f docker/services/docker-compose.yml restart payment-service-1 payment-service-2"

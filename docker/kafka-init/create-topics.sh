#!/usr/bin/env bash
# ------------------------------------------------------------------------------
# EventFlow Kafka topic bootstrap (docker-compose kafka-init sidecar).
#
# Creates one topic per domain event plus a companion DLQ topic ({name}.dlq).
# Producers set the message key to orderId so all events for one order share a
# partition and keep per-order ordering.
#
# Defaults: 3 partitions, replication 1, retention 7d, cleanup.policy=delete.
# Override via KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_PARTITIONS,
# KAFKA_TOPIC_REPLICATION_FACTOR, KAFKA_TOPIC_RETENTION_MS, KAFKA_TOPIC_CLEANUP_POLICY.
# ------------------------------------------------------------------------------
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"
REPLICATION="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
RETENTION_MS="${KAFKA_TOPIC_RETENTION_MS:-604800000}"
CLEANUP_POLICY="${KAFKA_TOPIC_CLEANUP_POLICY:-delete}"
DLQ_SUFFIX=".dlq"

TOPIC_CONFIG="retention.ms=${RETENTION_MS},cleanup.policy=${CLEANUP_POLICY}"

TOPICS=(
  "order.created"
  "order.cancelled"
  "payment.requested"
  "payment.processed"
  "payment.failed"
  "stock.reserved"
  "stock.released"
  "stock.failed"
  "notification.send"
  "notification.sent"
  "notification.failed"
)

create_topic() {
  local name="$1"
  kafka-topics --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${name}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION}" \
    --config "${TOPIC_CONFIG}"
}

echo "Waiting for Kafka at ${BOOTSTRAP}..."
cub kafka-ready -b "${BOOTSTRAP}" 1 60

echo "Topic config: partitions=${PARTITIONS} replication=${REPLICATION} ${TOPIC_CONFIG}"

for topic in "${TOPICS[@]}"; do
  echo "Creating topic: ${topic}"
  create_topic "${topic}"

  echo "Creating DLQ topic: ${topic}${DLQ_SUFFIX}"
  create_topic "${topic}${DLQ_SUFFIX}"
done

echo "All Kafka topics created successfully."

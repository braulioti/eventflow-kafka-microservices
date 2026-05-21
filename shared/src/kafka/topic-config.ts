/**
 * Kafka topic provisioning defaults and partition strategy documentation.
 *
 * Used by `docker/kafka-init/create-topics.sh` and local Docker Compose to align
 * partition count, retention, and DLQ companion topics with {@link EVENT_CATALOG}.
 */
import { EVENT_CATALOG } from '../events/event-catalog';
import { ALL_EVENT_TYPES } from '../events/event-types';
import { DLQ_TOPIC_SUFFIX } from '../events/kafka-topics';

/**
 * Payload field used as Kafka message key so all events for one order share a partition.
 * See {@link getPartitionKeyFromPayload} and {@link resolvePartitionKey}.
 */
export const PARTITION_KEY_FIELD = 'orderId' as const;

/** Literal type of {@link PARTITION_KEY_FIELD}. */
export type PartitionKeyField = typeof PARTITION_KEY_FIELD;

/**
 * Default partition count for new topics.
 * Three partitions balance parallelism with per-order ordering when keyed by `orderId`.
 */
export const DEFAULT_TOPIC_PARTITIONS = 3;

/** Default replication factor (1 for local Docker; raise in production). */
export const DEFAULT_REPLICATION_FACTOR = 1;

/** Default log retention (7 days); override with `KAFKA_TOPIC_RETENTION_MS`. */
export const DEFAULT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;

/** Kafka cleanup policy for event topics (time-based delete). */
export const DEFAULT_CLEANUP_POLICY = 'delete' as const;

/** Resolved topic creation parameters passed to kafka-init scripts. */
export interface KafkaTopicConfig {
  partitions: number;
  replicationFactor: number;
  retentionMs: number;
  cleanupPolicy: typeof DEFAULT_CLEANUP_POLICY;
}

/**
 * Partition strategy: hash by `orderId` message key.
 * All events for the same order land in the same partition → per-order ordering.
 */
export const PARTITION_STRATEGY = {
  keyField: PARTITION_KEY_FIELD,
  partitions: DEFAULT_TOPIC_PARTITIONS,
  rationale:
    'Message key = orderId ensures all events for one order are ordered within a partition.',
} as const;

/**
 * Merges env overrides (`KAFKA_TOPIC_*`) with {@link DEFAULT_TOPIC_PARTITIONS} etc.
 * @param overrides - Optional per-call overrides (tests, scripts)
 */
export function resolveKafkaTopicConfig(
  overrides?: Partial<KafkaTopicConfig>,
): KafkaTopicConfig {
  return {
    partitions: Number(
      process.env.KAFKA_TOPIC_PARTITIONS ?? DEFAULT_TOPIC_PARTITIONS,
    ),
    replicationFactor: Number(
      process.env.KAFKA_TOPIC_REPLICATION_FACTOR ?? DEFAULT_REPLICATION_FACTOR,
    ),
    retentionMs: Number(
      process.env.KAFKA_TOPIC_RETENTION_MS ?? DEFAULT_RETENTION_MS,
    ),
    cleanupPolicy: DEFAULT_CLEANUP_POLICY,
    ...overrides,
  };
}

/** CLI `--config` entries for kafka-topics --create */
export function buildTopicCreateConfigs(
  config: KafkaTopicConfig = resolveKafkaTopicConfig(),
): string[] {
  return [
    `retention.ms=${config.retentionMs}`,
    `cleanup.policy=${config.cleanupPolicy}`,
  ];
}

/** All Kafka topics used in the catalog (includes domain topics like order.events). */
export const BASE_KAFKA_TOPICS: readonly string[] = [
  ...new Set([
    ...ALL_EVENT_TYPES,
    ...EVENT_CATALOG.map((entry) => entry.topic),
  ]),
];

/** Base topics + DLQ companions */
export const ALL_KAFKA_TOPICS_WITH_DLQ: readonly string[] =
  BASE_KAFKA_TOPICS.flatMap((topic) => [topic, `${topic}${DLQ_TOPIC_SUFFIX}`]);

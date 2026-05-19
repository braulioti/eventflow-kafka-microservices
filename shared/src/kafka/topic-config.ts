import { ALL_EVENT_TYPES } from '../events/event-types';
import { DLQ_TOPIC_SUFFIX } from '../events/kafka-topics';

/** Payload field used as Kafka message key (partition routing) */
export const PARTITION_KEY_FIELD = 'orderId' as const;

export type PartitionKeyField = typeof PARTITION_KEY_FIELD;

/** Default: 3 partitions — allows parallel consumers while keeping orderId ordering per partition */
export const DEFAULT_TOPIC_PARTITIONS = 3;

export const DEFAULT_REPLICATION_FACTOR = 1;

/** Default: 7 days — adjust per environment via KAFKA_TOPIC_RETENTION_MS */
export const DEFAULT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;

export const DEFAULT_CLEANUP_POLICY = 'delete' as const;

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

/** All base event topics (1:1 with event types) */
export const BASE_KAFKA_TOPICS: readonly string[] = ALL_EVENT_TYPES;

/** Base topics + DLQ companions */
export const ALL_KAFKA_TOPICS_WITH_DLQ: readonly string[] =
  BASE_KAFKA_TOPICS.flatMap((topic) => [topic, `${topic}${DLQ_TOPIC_SUFFIX}`]);

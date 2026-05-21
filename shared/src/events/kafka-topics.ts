/**
 * Kafka topic naming helpers and DLQ companion topic derivation.
 *
 * For events without aggregate overrides, topic name equals {@link EventType} value.
 * {@link EVENT_CATALOG} and {@link BASE_KAFKA_TOPICS} combine catalog + domain topics
 * for `create-topics.sh` provisioning.
 */
import { ALL_EVENT_TYPES, EventType, type EventTypeValue } from './event-types';

/**
 * Alias of {@link EventType} — each event type maps to a topic of the same name
 * when no {@link resolveKafkaTopic} override exists.
 */
export const KafkaTopic = { ...EventType } as const;

/** Topic string type (same universe as {@link EventTypeValue}). */
export type KafkaTopicValue = EventTypeValue;

/** All 1:1 event-type topics (excludes aggregate-only names like `order.events`). */
export const ALL_KAFKA_TOPICS: readonly KafkaTopicValue[] = ALL_EVENT_TYPES;

/** Suffix appended to base topics for dead-letter streams consumed by dlq-service. */
export const DLQ_TOPIC_SUFFIX = '.dlq';

/**
 * Builds the DLQ topic name for a given base topic.
 * @param topic - Base Kafka topic (e.g. `payment.events` or `stock.reserved`)
 * @returns Companion DLQ topic (e.g. `payment.events.dlq`)
 */
export function toDlqTopic(topic: string): string {
  return `${topic}${DLQ_TOPIC_SUFFIX}`;
}

/** Precomputed DLQ topic list for every 1:1 {@link ALL_KAFKA_TOPICS} entry. */
export const ALL_DLQ_TOPICS: readonly string[] = ALL_KAFKA_TOPICS.map(toDlqTopic);

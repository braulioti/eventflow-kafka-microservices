import { ALL_EVENT_TYPES, EventType, type EventTypeValue } from './event-types';

/**
 * Kafka topic per event type (1:1 mapping for explicit routing and observability).
 * DLQ topics use the pattern: `{topic}.dlq`
 */
export const KafkaTopic = { ...EventType } as const;

export type KafkaTopicValue = EventTypeValue;

export const ALL_KAFKA_TOPICS: readonly KafkaTopicValue[] = ALL_EVENT_TYPES;

export const DLQ_TOPIC_SUFFIX = '.dlq';

/** Companion dead-letter topic name for a base event topic. */
export function toDlqTopic(topic: KafkaTopicValue): string {
  return `${topic}${DLQ_TOPIC_SUFFIX}`;
}

export const ALL_DLQ_TOPICS: readonly string[] = ALL_KAFKA_TOPICS.map(toDlqTopic);

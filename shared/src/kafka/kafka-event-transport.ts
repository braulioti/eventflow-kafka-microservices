/**
 * Minimal publish abstraction decoupling retry logic from NestJS.
 *
 * Each service implements this on its `EventPublisher` so {@link KafkaRetryExecutor}
 * can republish and send to DLQ without importing Nest Kafka client types.
 */
import type { EventEnvelope } from '../events/envelope';
import type { EventFailurePayload } from '../events/event-failure';
import type { EventTypeValue } from '../events/event-types';

/** Optional Kafka headers merged on publish (e.g. retry metadata). */
export interface PublishOptions {
  headers?: Record<string, string>;
}

/**
 * Abstraction implemented by each service's {@link EventPublisher}.
 * Used by {@link KafkaRetryExecutor} to republish or route to DLQ without Nest types.
 */
export interface KafkaEventTransport {
  /**
   * Publishes to the topic resolved by {@link resolveKafkaTopic} for `eventType`.
   * Retry republish passes merged headers via `options.headers`.
   */
  publish(
    eventType: EventTypeValue,
    envelope: EventEnvelope<unknown>,
    options?: PublishOptions,
  ): Promise<void>;

  /**
   * Publishes a {@link EventFailurePayload} envelope to the companion `.dlq` topic.
   */
  publishToDlq(
    eventType: EventTypeValue,
    envelope: EventEnvelope<EventFailurePayload>,
    options?: PublishOptions,
  ): Promise<void>;
}

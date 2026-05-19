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
  publish(
    topic: EventTypeValue,
    envelope: EventEnvelope<unknown>,
    options?: PublishOptions,
  ): Promise<void>;

  publishToDlq(
    originalTopic: EventTypeValue,
    envelope: EventEnvelope<EventFailurePayload>,
    options?: PublishOptions,
  ): Promise<void>;
}

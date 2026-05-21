/**
 * End-to-end consumer pipeline for the `order.events` aggregate topic.
 *
 * payment-service uses {@link parseOrderEventsMessage} to accept only `order.created`,
 * skipping other event types that may later share the stream. Enforces
 * `correlationId === payload.orderId` for saga consistency.
 */
import { EventType } from '../events/event-types';
import type { EventEnvelope } from '../events/envelope';
import type { OrderCreatedPayload } from '../events/payloads/order.events';
import {
  EventValidationError,
  isOrderCreatedEvent,
  validateEventEnvelope,
} from '../events/validate-envelope';
import { validateOrderCreatedPayload } from '../events/validate-order-created';
import { deserializeKafkaPayload } from './deserialize-kafka-payload';
import { extractEnvelope } from './envelope-kafka';

/**
 * Discriminated result: either a validated `order.created` envelope or a benign skip
 * (unsupported `eventType` on the shared topic).
 */
export type ParseOrderEventsResult =
  | { kind: 'order.created'; envelope: EventEnvelope<OrderCreatedPayload> }
  | { kind: 'skipped'; reason: string; eventType?: string };

/**
 * Full consumer pipeline for topic `order.events`:
 * deserialize → extract envelope → filter `order.created` → validate structure.
 */
export function parseOrderEventsMessage(
  rawPayload: unknown,
): ParseOrderEventsResult {
  let deserialized: unknown;

  try {
    deserialized = deserializeKafkaPayload(rawPayload);
  } catch (error) {
    throw new EventValidationError(
      'Failed to deserialize Kafka message',
      [error instanceof Error ? error.message : String(error)],
    );
  }

  let envelope: EventEnvelope<unknown>;

  try {
    envelope = extractEnvelope(deserialized);
  } catch (error) {
    throw new EventValidationError(
      'Failed to extract event envelope',
      [error instanceof Error ? error.message : String(error)],
    );
  }

  if (!isOrderCreatedEvent(envelope)) {
    return {
      kind: 'skipped',
      reason: 'unsupported-event-type',
      eventType: envelope.eventType,
    };
  }

  validateEventEnvelope(envelope, EventType.ORDER_CREATED);
  validateOrderCreatedPayload(envelope.payload);

  // Saga invariant: correlation id must match business key used as partition key.
  if (envelope.correlationId !== envelope.payload.orderId) {
    throw new EventValidationError('Invalid order.created envelope', [
      'correlationId must equal payload.orderId',
    ]);
  }

  return { kind: 'order.created', envelope };
}

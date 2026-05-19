import type { EventEnvelope } from '../events/envelope';
import { KafkaHeader } from './kafka-headers';
import { getPartitionKeyFromPayload } from './partition-key';

/** Maps envelope metadata to Kafka record headers for cross-service tracing. */
export function envelopeToKafkaHeaders(
  envelope: EventEnvelope<unknown>,
): Record<string, string> {
  const headers: Record<string, string> = {
    [KafkaHeader.EVENT_ID]: envelope.eventId,
    [KafkaHeader.EVENT_TYPE]: envelope.eventType,
    [KafkaHeader.CORRELATION_ID]: envelope.correlationId,
    [KafkaHeader.SOURCE]: envelope.source,
    [KafkaHeader.SCHEMA_VERSION]: envelope.version,
  };

  if (envelope.causationId) {
    headers[KafkaHeader.CAUSATION_ID] = envelope.causationId;
  }

  return headers;
}

/**
 * Normalizes Nest/Kafka payloads into an {@link EventEnvelope}.
 * Supports both raw envelope JSON and Nest-wrapped `{ data: envelope }` shapes.
 */
export function extractEnvelope<T>(payload: unknown): EventEnvelope<T> {
  if (payload && typeof payload === 'object' && 'eventId' in payload) {
    return payload as EventEnvelope<T>;
  }

  if (payload && typeof payload === 'object' && 'data' in payload) {
    const nested = (payload as { data: unknown }).data;
    if (nested && typeof nested === 'object' && 'eventId' in nested) {
      return nested as EventEnvelope<T>;
    }
  }

  throw new Error('Invalid Kafka event payload');
}

/**
 * @deprecated Prefer {@link resolvePartitionKey} or {@link getPartitionKeyFromPayload}.
 * Kept for backward compatibility with service publishers.
 */
export function getOrderIdFromPayload(payload: { orderId?: string }): string {
  return getPartitionKeyFromPayload(payload);
}

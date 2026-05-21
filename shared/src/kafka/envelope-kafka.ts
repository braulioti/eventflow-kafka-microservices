/**
 * Wire-format helpers: envelope ↔ Kafka headers and payload extraction.
 *
 * Bridges domain {@link EventEnvelope} with kafkajs record shape. Handles Nest's optional
 * `{ data: envelope }` wrapper so consumers stay compatible across transport versions.
 */
import type { EventEnvelope } from '../events/envelope';
import { KafkaHeader } from './kafka-headers';
import { deserializeKafkaPayload } from './deserialize-kafka-payload';
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
  const normalized = deserializeKafkaPayload(payload);

  // Direct envelope JSON (preferred on-the-wire shape).
  if (normalized && typeof normalized === 'object' && 'eventId' in normalized) {
    return normalized as EventEnvelope<T>;
  }

  // Nest microservice wrapper: { data: EventEnvelope }.
  if (normalized && typeof normalized === 'object' && 'data' in normalized) {
    const nested = (normalized as { data: unknown }).data;
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

/**
 * Kafka partition key resolution for EventFlow publishers.
 *
 * Every catalog entry uses `orderId` as the message key ({@link PARTITION_KEY_FIELD}).
 * Hashing by this key keeps all saga events for one order in a single partition, preserving
 * per-order ordering while allowing different orders to process in parallel across partitions.
 *
 * Producers call {@link resolvePartitionKey} after building an envelope; consumers rely on
 * the same field for correlation checks in domain parsers.
 */
import type { EventEnvelope } from '../events/envelope';
import { PARTITION_KEY_FIELD } from './topic-config';

/**
 * Extracts the Kafka record key from a payload object.
 * @param payload - Domain payload or arbitrary object with `orderId`
 * @throws Error when payload is not an object or `orderId` is missing/empty
 */
export function getPartitionKeyFromPayload(payload: unknown): string {
  if (!payload || typeof payload !== 'object') {
    throw new Error(
      `Cannot resolve partition key: payload must be an object with "${PARTITION_KEY_FIELD}"`,
    );
  }

  const key = (payload as Record<string, unknown>)[PARTITION_KEY_FIELD];

  if (typeof key !== 'string' || key.length === 0) {
    throw new Error(
      `Missing or invalid partition key field "${PARTITION_KEY_FIELD}" in event payload`,
    );
  }

  return key;
}

/**
 * Resolves the Kafka message key from a standard {@link EventEnvelope}.
 * Equivalent to `getPartitionKeyFromPayload(envelope.payload)`.
 * @param envelope - Published or consumed envelope with `payload.orderId`
 */
export function resolvePartitionKey(envelope: EventEnvelope<unknown>): string {
  return getPartitionKeyFromPayload(envelope.payload);
}

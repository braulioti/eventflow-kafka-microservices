/**
 * Kafka partition key resolution.
 *
 * All domain topics use `orderId` as the message key so events for one order
 * stay ordered within a partition (see {@link PARTITION_STRATEGY}).
 */
import type { EventEnvelope } from '../events/envelope';
import { PARTITION_KEY_FIELD } from './topic-config';

/** Reads {@link PARTITION_KEY_FIELD} from a payload object (typically `orderId`). */
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

/** Resolves the Kafka message key from a standard event envelope */
export function resolvePartitionKey(envelope: EventEnvelope<unknown>): string {
  return getPartitionKeyFromPayload(envelope.payload);
}

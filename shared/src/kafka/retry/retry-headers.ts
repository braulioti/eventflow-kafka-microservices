/**
 * Kafka headers for consumer-side retry scheduling and observability.
 *
 * Written by {@link buildRetryHeaders} on republish; read by {@link KafkaRetryExecutor}
 * via {@link getRetryCount} and {@link getRetryAt}. Kept separate from {@link KafkaHeader}
 * trace fields but merged in {@link mergeEnvelopeAndRetryHeaders}.
 */
import { KafkaHeader } from '../kafka-headers';

/** `x-retry-*` and `x-original-topic` header names on republished messages. */
export const RetryHeader = {
  RETRY_COUNT: 'x-retry-count',
  RETRY_MAX: 'x-retry-max',
  RETRY_AT: 'x-retry-at',
  ORIGINAL_TOPIC: 'x-original-topic',
} as const;

/** Normalizes kafkajs header values (Buffer | string) to plain strings. */
export function parseKafkaHeaders(
  headers: Record<string, unknown> | undefined,
): Record<string, string> {
  if (!headers) {
    return {};
  }

  const parsed: Record<string, string> = {};

  for (const [key, value] of Object.entries(headers)) {
    if (Buffer.isBuffer(value)) {
      parsed[key] = value.toString('utf8');
    } else if (typeof value === 'string') {
      parsed[key] = value;
    }
  }

  return parsed;
}

/** Zero-based count of completed retry rounds for this message. */
export function getRetryCount(headers: Record<string, string>): number {
  const raw = headers[RetryHeader.RETRY_COUNT];
  if (!raw) {
    return 0;
  }

  const count = Number(raw);
  return Number.isFinite(count) ? count : 0;
}

/** Epoch ms when the consumer should process a scheduled retry (if set). */
export function getRetryAt(headers: Record<string, string>): number | undefined {
  const raw = headers[RetryHeader.RETRY_AT];
  if (!raw) {
    return undefined;
  }

  const timestamp = Number(raw);
  return Number.isFinite(timestamp) ? timestamp : undefined;
}

/** Headers attached when republishing to the same topic after backoff. */
export function buildRetryHeaders(params: {
  retryCount: number;
  maxAttempts: number;
  retryAt: number;
  originalTopic: string;
  envelopeHeaders: Record<string, string>;
}): Record<string, string> {
  return {
    ...params.envelopeHeaders,
    [RetryHeader.RETRY_COUNT]: String(params.retryCount),
    [RetryHeader.RETRY_MAX]: String(params.maxAttempts),
    [RetryHeader.RETRY_AT]: String(params.retryAt),
    [RetryHeader.ORIGINAL_TOPIC]: params.originalTopic,
  };
}

/** Keeps trace headers stable while layering retry metadata on republish. */
export function mergeEnvelopeAndRetryHeaders(
  envelopeHeaders: Record<string, string>,
  retryHeaders: Record<string, string>,
): Record<string, string> {
  return {
    [KafkaHeader.EVENT_ID]: envelopeHeaders[KafkaHeader.EVENT_ID],
    [KafkaHeader.EVENT_TYPE]: envelopeHeaders[KafkaHeader.EVENT_TYPE],
    [KafkaHeader.CORRELATION_ID]: envelopeHeaders[KafkaHeader.CORRELATION_ID],
    [KafkaHeader.SOURCE]: envelopeHeaders[KafkaHeader.SOURCE],
    [KafkaHeader.SCHEMA_VERSION]: envelopeHeaders[KafkaHeader.SCHEMA_VERSION],
    ...(envelopeHeaders[KafkaHeader.CAUSATION_ID]
      ? { [KafkaHeader.CAUSATION_ID]: envelopeHeaders[KafkaHeader.CAUSATION_ID] }
      : {}),
    ...retryHeaders,
  };
}

import type { ServiceName } from './event-catalog';
import type { EventEnvelope } from './envelope';
import { createEventEnvelope } from './create-envelope';
import type { EventTypeValue } from './event-types';
import type { RetryPolicyConfig } from '../kafka/retry/retry-policy';

export interface EventErrorDetails {
  message: string;
  name: string;
  stack?: string;
  occurredAt: string;
}

export interface EventRetryDetails {
  attempt: number;
  maxAttempts: number;
  exhausted: boolean;
  nextRetryAt?: string;
}

/**
 * Structured error event published to DLQ after retries are exhausted.
 */
export interface EventFailurePayload {
  kind: 'event.failure';
  originalTopic: string;
  originalEventId: string;
  originalEventType: EventTypeValue;
  originalSource: string;
  correlationId: string;
  originalPayload: unknown;
  originalEnvelope: EventEnvelope<unknown>;
  error: EventErrorDetails;
  retry: EventRetryDetails;
  failedBy: ServiceName;
  failedAt: string;
}

/** Coerces thrown values into a serializable shape for DLQ payloads. */
export function normalizeError(error: unknown): EventErrorDetails {
  if (error instanceof Error) {
    return {
      message: error.message,
      name: error.name,
      stack: error.stack,
      occurredAt: new Date().toISOString(),
    };
  }

  return {
    message: String(error),
    name: 'UnknownError',
    occurredAt: new Date().toISOString(),
  };
}

/**
 * Wraps a failed message as an {@link EventFailurePayload} for the companion DLQ topic.
 * Preserves the original envelope and retry metadata for operators and the dlq-service.
 */
export function createFailureEnvelope(params: {
  original: EventEnvelope<unknown>;
  originalTopic: string;
  service: ServiceName;
  error: unknown;
  attempt: number;
  policy: RetryPolicyConfig;
  nextRetryAt?: string;
}): EventEnvelope<EventFailurePayload> {
  const errorDetails = normalizeError(params.error);

  return createEventEnvelope({
    eventType: params.original.eventType,
    source: params.service,
    correlationId: params.original.correlationId,
    causationId: params.original.eventId,
    payload: {
      kind: 'event.failure',
      originalTopic: params.originalTopic,
      originalEventId: params.original.eventId,
      originalEventType: params.original.eventType,
      originalSource: params.original.source,
      correlationId: params.original.correlationId,
      originalPayload: params.original.payload,
      originalEnvelope: params.original,
      error: errorDetails,
      retry: {
        attempt: params.attempt,
        maxAttempts: params.policy.maxAttempts,
        exhausted: params.attempt >= params.policy.maxAttempts,
        nextRetryAt: params.nextRetryAt,
      },
      failedBy: params.service,
      failedAt: new Date().toISOString(),
    },
  });
}

/** Type guard for DLQ messages produced by {@link KafkaRetryExecutor}. */
export function isEventFailurePayload(
  payload: unknown,
): payload is EventFailurePayload {
  return (
    typeof payload === 'object' &&
    payload !== null &&
    'kind' in payload &&
    (payload as EventFailurePayload).kind === 'event.failure'
  );
}

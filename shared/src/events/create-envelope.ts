import { randomUUID } from 'crypto';
import {
  EVENT_SCHEMA_VERSION,
  type CreateEventEnvelopeParams,
  type EventEnvelope,
} from './envelope';

/**
 * Builds a fully populated {@link EventEnvelope} with sensible defaults.
 *
 * Generates `eventId` and `timestamp` when omitted so producers stay consistent.
 * `correlationId` is typically the business key (e.g. orderId) for the whole flow.
 */
export function createEventEnvelope<TPayload>(
  params: CreateEventEnvelopeParams<TPayload>,
): EventEnvelope<TPayload> {
  return {
    eventId: params.eventId ?? randomUUID(),
    eventType: params.eventType,
    version: params.version ?? EVENT_SCHEMA_VERSION,
    timestamp: params.timestamp ?? new Date().toISOString(),
    correlationId: params.correlationId,
    causationId: params.causationId,
    source: params.source,
    payload: params.payload,
  };
}

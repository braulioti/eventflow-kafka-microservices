import type { EventTypeValue } from './event-types';

/** Current contract version for all domain events */
export const EVENT_SCHEMA_VERSION = '1.0';

/**
 * Standard event envelope for consistency and traceability across services.
 *
 * Field rules:
 * - **eventId** — UUID v4; unique per message; use for idempotency and deduplication.
 * - **correlationId** — business flow id (typically `orderId`); ties all related events.
 * - **timestamp** — ISO-8601 UTC; set at publish time unless overridden for replay.
 * - **eventType** — canonical name; matches Kafka topic name.
 * - **version** — schema version for safe payload evolution.
 * - **source** — producing service name (e.g. `order-service`).
 * - **causationId** — optional; `eventId` of the event that caused this one.
 * - **payload** — domain data; must include `orderId` for partition routing.
 */
export interface EventEnvelope<TPayload> {
  /** Unique idempotency key for this message (UUID) */
  eventId: string;
  eventType: EventTypeValue;
  version: string;
  /** ISO-8601 UTC timestamp when the event was produced */
  timestamp: string;
  /** Business correlation id — use orderId for the order flow */
  correlationId: string;
  causationId?: string;
  source: string;
  payload: TPayload;
}

/** Input for {@link createEventEnvelope}; optional fields receive generated defaults. */
export interface CreateEventEnvelopeParams<TPayload> {
  eventType: EventTypeValue;
  source: string;
  /** Should match orderId for order-scoped flows */
  correlationId: string;
  causationId?: string;
  payload: TPayload;
  version?: string;
  eventId?: string;
  timestamp?: string;
}

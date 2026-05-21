/**
 * Event envelope contract — the on-the-wire shape for every Kafka message in EventFlow.
 *
 * Producers wrap domain payloads in {@link EventEnvelope}; consumers validate with
 * {@link validateEventEnvelope} before handling. Versioning (`EVENT_SCHEMA_VERSION`) allows
 * coordinated schema evolution across services without breaking idempotency keys.
 */
import type { EventTypeValue } from './event-types';

/**
 * Current envelope + payload schema version shared by all services.
 * Bump only when making breaking envelope or cross-service payload changes.
 */
export const EVENT_SCHEMA_VERSION = '1.0';

/**
 * Standard event envelope for consistency and traceability across services.
 *
 * Field rules:
 * - **eventId** — UUID v4; unique per message; use for idempotency and deduplication.
 * - **correlationId** — business flow id (typically `orderId`); ties all related events.
 * - **timestamp** — ISO-8601 UTC; set at publish time unless overridden for replay.
 * - **eventType** — canonical name (e.g. `order.created`); Kafka topic may differ (e.g. `order.events`).
 * - **version** — schema version for safe payload evolution.
 * - **source** — producing service name (e.g. `order-service`).
 * - **causationId** — optional; `eventId` of the event that caused this one.
 * - **payload** — domain data; must include `orderId` for partition routing.
 */
export interface EventEnvelope<TPayload> {
  /** Unique idempotency key for this message (UUID v4). Stored by consumers to dedupe replays. */
  eventId: string;
  /** Canonical domain name (e.g. `order.created`); may differ from physical Kafka topic name. */
  eventType: EventTypeValue;
  /** Schema version; must match {@link EVENT_SCHEMA_VERSION} unless explicitly negotiated. */
  version: string;
  /** ISO-8601 UTC timestamp when the event was produced (publisher clock). */
  timestamp: string;
  /**
   * Business correlation id tying all events in one saga.
   * For order flows this MUST equal `payload.orderId` (enforced by domain parsers).
   */
  correlationId: string;
  /** Optional parent `eventId` that caused this message (audit / causality chain). */
  causationId?: string;
  /** Producing microservice identifier (e.g. `order-service`), from {@link ServiceName}. */
  source: string;
  /** Domain-specific body; type narrowed via {@link EventPayloadMap} and validators. */
  payload: TPayload;
}

/**
 * Input for {@link createEventEnvelope}; optional fields receive generated defaults.
 * @typeParam TPayload - Domain payload type from {@link EventPayloadMap}
 */
export interface CreateEventEnvelopeParams<TPayload> {
  /** Canonical event name from {@link EventType}. */
  eventType: EventTypeValue;
  /** Producing service (e.g. `order-service`). */
  source: string;
  /** Business flow id; for order saga must equal `payload.orderId`. */
  correlationId: string;
  /** Parent message id when this event reacts to another (retry/DLQ preserve chain). */
  causationId?: string;
  /** Domain body published inside the envelope. */
  payload: TPayload;
  /** Defaults to {@link EVENT_SCHEMA_VERSION}. */
  version?: string;
  /** Defaults to `randomUUID()` — set when reusing aggregate idempotency key. */
  eventId?: string;
  /** Defaults to `new Date().toISOString()` — set only for deterministic tests/replay. */
  timestamp?: string;
}


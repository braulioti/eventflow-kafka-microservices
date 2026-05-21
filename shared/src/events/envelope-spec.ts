/**
 * Human- and machine-readable specification of envelope fields.
 *
 * Documents required formats for docs generators, onboarding, and {@link ENVELOPE_FIELD_SPEC}
 * metadata. Runtime validation lives in {@link validateEventEnvelope}; this module is the
 * contract reference aligned with `docs/EVENT_MODELING.md`.
 */

/** Unique identifier for this message instance (UUID v4). Used for idempotency and deduplication. */
export type EventId = string;

/**
 * Business flow identifier. All events belonging to the same order/process
 * share the same correlationId (typically the orderId).
 */
export type CorrelationId = string;

/** ISO-8601 UTC timestamp set at publish time (e.g. 2026-05-19T12:00:00.000Z). */
export type EventTimestamp = string;

/**
 * Standard envelope fields (see EventEnvelope interface):
 *
 * | Field          | Required | Description                                      |
 * |----------------|----------|--------------------------------------------------|
 * | eventId        | yes      | Unique message id (idempotency)                  |
 * | eventType      | yes      | Domain event name (= Kafka topic name)           |
 * | version        | yes      | Schema version (current: 1.0)                    |
 * | timestamp      | yes      | ISO-8601 publish time                            |
 * | correlationId  | yes      | Ties events in the same business flow            |
 * | causationId    | no       | eventId of the message that caused this one      |
 * | source         | yes      | Producing service (e.g. order-service)           |
 * | payload        | yes      | Domain-specific data                             |
 */

/**
 * Structured field metadata for tooling and documentation (not used at runtime).
 * Keys mirror {@link EventEnvelope} property names.
 */
export const ENVELOPE_FIELD_SPEC = {
  eventId: {
    required: true,
    format: 'uuid',
    description: 'Unique identifier for this event instance',
  },
  correlationId: {
    required: true,
    format: 'string',
    description: 'Shared across all events in the same order flow (usually orderId)',
  },
  timestamp: {
    required: true,
    format: 'iso-8601',
    description: 'UTC time when the event was published',
  },
  eventType: {
    required: true,
    format: 'domain.action',
    description: 'Event name and Kafka topic',
  },
  version: {
    required: true,
    format: 'semver',
    description: 'Payload schema version',
  },
  source: {
    required: true,
    format: 'service-name',
    description: 'Microservice that produced the event',
  },
  causationId: {
    required: false,
    format: 'uuid',
    description: 'Parent event that triggered this event',
  },
  payload: {
    required: true,
    format: 'object',
    description: 'Domain payload (must include orderId for partition key)',
  },
} as const;

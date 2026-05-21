/**
 * Runtime validation and type guards for {@link EventEnvelope} on the consumer edge.
 *
 * Throws {@link EventValidationError} with aggregated field errors so handlers can
 * distinguish poison messages from transient failures. Domain payloads have dedicated
 * validators in sibling modules (`validate-order-created`, `validate-payment-events`).
 */
import { EVENT_SCHEMA_VERSION, type EventEnvelope } from './envelope';
import { EventType, type EventTypeValue } from './event-types';

/** RFC 4122 UUID v4 pattern for `eventId` and optional `causationId`. */
const UUID_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

/** Strict UTC ISO-8601 with optional milliseconds and `Z` suffix. */
const ISO_TIMESTAMP_REGEX =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z$/;

/**
 * Validation failure with machine-readable `details` array.
 * Consumers typically let this propagate to {@link KafkaRetryExecutor} (non-retryable
 * if policy treats validation as permanent — today surfaced as handler error).
 */
export class EventValidationError extends Error {
  readonly details: string[];

  constructor(message: string, details: string[] = []) {
    super(details.length > 0 ? `${message}: ${details.join('; ')}` : message);
    this.name = 'EventValidationError';
    this.details = details;
  }
}

/** Internal: non-blank string check for required envelope string fields. */
function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}

/** Internal: UUID v4 format gate for idempotency-related fields. */
function isUuid(value: unknown): value is string {
  return isNonEmptyString(value) && UUID_REGEX.test(value);
}

/** Internal: publish timestamp format gate. */
function isIsoTimestamp(value: unknown): value is string {
  return isNonEmptyString(value) && ISO_TIMESTAMP_REGEX.test(value);
}

/**
 * Validates the standard {@link EventEnvelope} shape.
 * @param value - Parsed JSON or object from Kafka consumer
 * @param expectedEventType - When set, asserts `eventType` matches (domain parsers)
 * @throws {@link EventValidationError} when structure or version is invalid
 */
export function validateEventEnvelope(
  value: unknown,
  expectedEventType?: EventTypeValue,
): asserts value is EventEnvelope<unknown> {
  const errors: string[] = [];

  if (!value || typeof value !== 'object') {
    throw new EventValidationError('Envelope must be an object');
  }

  const envelope = value as Record<string, unknown>;

  if (!isUuid(envelope.eventId)) {
    errors.push('eventId must be a UUID');
  }

  if (!isNonEmptyString(envelope.eventType)) {
    errors.push('eventType is required');
  } else if (
    expectedEventType &&
    envelope.eventType !== expectedEventType
  ) {
    errors.push(`eventType must be ${expectedEventType}`);
  }

  if (envelope.version !== EVENT_SCHEMA_VERSION) {
    errors.push(`version must be ${EVENT_SCHEMA_VERSION}`);
  }

  if (!isIsoTimestamp(envelope.timestamp)) {
    errors.push('timestamp must be ISO-8601 UTC');
  }

  if (!isNonEmptyString(envelope.correlationId)) {
    errors.push('correlationId is required');
  }

  if (!isNonEmptyString(envelope.source)) {
    errors.push('source is required');
  }

  if (envelope.causationId !== undefined && !isUuid(envelope.causationId)) {
    errors.push('causationId must be a UUID when present');
  }

  if (!envelope.payload || typeof envelope.payload !== 'object') {
    errors.push('payload must be an object');
  }

  if (errors.length > 0) {
    throw new EventValidationError('Invalid event envelope', errors);
  }
}

/** Returns true when the envelope is the expected event type (filter helper). */
export function isEventType(
  envelope: EventEnvelope<unknown>,
  eventType: EventTypeValue,
): boolean {
  return envelope.eventType === eventType;
}

/** Filter for messages on `order.events` that represent `order.created`. */
export function isOrderCreatedEvent(
  envelope: EventEnvelope<unknown>,
): envelope is EventEnvelope<import('./payloads/order.events').OrderCreatedPayload> {
  return envelope.eventType === EventType.ORDER_CREATED;
}

/** Filter for messages on `payment.events` that represent `payment.processed`. */
export function isPaymentProcessedEvent(
  envelope: EventEnvelope<unknown>,
): envelope is EventEnvelope<import('./payloads/payment.events').PaymentProcessedPayload> {
  return envelope.eventType === EventType.PAYMENT_PROCESSED;
}

/** Filter for messages on `payment.events` that represent `payment.failed`. */
export function isPaymentFailedEvent(
  envelope: EventEnvelope<unknown>,
): envelope is EventEnvelope<import('./payloads/payment.events').PaymentFailedPayload> {
  return envelope.eventType === EventType.PAYMENT_FAILED;
}

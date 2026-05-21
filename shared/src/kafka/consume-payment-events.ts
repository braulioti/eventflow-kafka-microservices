/**
 * End-to-end consumer pipelines for the `payment.events` aggregate topic.
 *
 * stock-service calls {@link parsePaymentProcessedMessage}; order-service and
 * notification-service call {@link parsePaymentFailedMessage}. Unsupported types return
 * `{ kind: 'skipped' }` without throwing.
 */
import { EventType } from '../events/event-types';
import type { EventEnvelope } from '../events/envelope';
import type {
  PaymentFailedPayload,
  PaymentProcessedPayload,
} from '../events/payloads/payment.events';
import {
  EventValidationError,
  isPaymentFailedEvent,
  isPaymentProcessedEvent,
  validateEventEnvelope,
} from '../events/validate-envelope';
import {
  validatePaymentFailedPayload,
  validatePaymentProcessedPayload,
} from '../events/validate-payment-events';
import { deserializeKafkaPayload } from './deserialize-kafka-payload';
import { extractEnvelope } from './envelope-kafka';

/** Shared skip branch when `eventType` on `payment.events` is not handled by this parser. */
type Skipped = { kind: 'skipped'; reason: string; eventType?: string };

/** Result of {@link parsePaymentProcessedMessage}. */
export type ParsePaymentProcessedResult =
  | { kind: 'payment.processed'; envelope: EventEnvelope<PaymentProcessedPayload> }
  | Skipped;

/** Result of {@link parsePaymentFailedMessage}. */
export type ParsePaymentFailedResult =
  | { kind: 'payment.failed'; envelope: EventEnvelope<PaymentFailedPayload> }
  | Skipped;

/** Shared deserialize + extract steps for both payment parsers. */
function deserializeAndExtract(rawPayload: unknown): EventEnvelope<unknown> {
  let deserialized: unknown;

  try {
    deserialized = deserializeKafkaPayload(rawPayload);
  } catch (error) {
    throw new EventValidationError('Failed to deserialize Kafka message', [
      error instanceof Error ? error.message : String(error),
    ]);
  }

  try {
    return extractEnvelope(deserialized);
  } catch (error) {
    throw new EventValidationError('Failed to extract event envelope', [
      error instanceof Error ? error.message : String(error),
    ]);
  }
}

/** Consumer pipeline for `payment.events` → `payment.processed`. */
export function parsePaymentProcessedMessage(
  rawPayload: unknown,
): ParsePaymentProcessedResult {
  const envelope = deserializeAndExtract(rawPayload);

  if (!isPaymentProcessedEvent(envelope)) {
    return {
      kind: 'skipped',
      reason: 'unsupported-event-type',
      eventType: envelope.eventType,
    };
  }

  validateEventEnvelope(envelope, EventType.PAYMENT_PROCESSED);
  validatePaymentProcessedPayload(envelope.payload);

  // Saga invariant: same as order.created — correlation tracks orderId.
  if (envelope.correlationId !== envelope.payload.orderId) {
    throw new EventValidationError('Invalid payment.processed envelope', [
      'correlationId must equal payload.orderId',
    ]);
  }

  return { kind: 'payment.processed', envelope };
}

/** Consumer pipeline for `payment.events` → `payment.failed`. */
export function parsePaymentFailedMessage(
  rawPayload: unknown,
): ParsePaymentFailedResult {
  const envelope = deserializeAndExtract(rawPayload);

  if (!isPaymentFailedEvent(envelope)) {
    return {
      kind: 'skipped',
      reason: 'unsupported-event-type',
      eventType: envelope.eventType,
    };
  }

  validateEventEnvelope(envelope, EventType.PAYMENT_FAILED);
  validatePaymentFailedPayload(envelope.payload);

  // Saga invariant: failure events still keyed by order for compensation routing.
  if (envelope.correlationId !== envelope.payload.orderId) {
    throw new EventValidationError('Invalid payment.failed envelope', [
      'correlationId must equal payload.orderId',
    ]);
  }

  return { kind: 'payment.failed', envelope };
}

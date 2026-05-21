/**
 * Structural validators for payment outcome events on `payment.events`.
 *
 * Used by {@link parsePaymentProcessedMessage} and {@link parsePaymentFailedMessage}
 * after envelope extraction. Complements payment-service business rules (simulator, fraud).
 */
import type { PaymentFailedPayload, PaymentProcessedPayload } from './payloads/payment.events';
import { EventValidationError } from './validate-envelope';

/** Shared guard for required string identifiers and timestamps. */
function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}

/**
 * Asserts {@link PaymentProcessedPayload} shape (paymentId, orderId, transactionId, processedAt).
 * @throws {@link EventValidationError} when any required field is missing or empty
 */
export function validatePaymentProcessedPayload(
  payload: unknown,
): asserts payload is PaymentProcessedPayload {
  const errors: string[] = [];

  if (!payload || typeof payload !== 'object') {
    throw new EventValidationError('payment.processed payload must be an object');
  }

  const data = payload as Record<string, unknown>;

  if (!isNonEmptyString(data.paymentId)) {
    errors.push('payload.paymentId is required');
  }
  if (!isNonEmptyString(data.orderId)) {
    errors.push('payload.orderId is required');
  }
  if (!isNonEmptyString(data.transactionId)) {
    errors.push('payload.transactionId is required');
  }
  if (!isNonEmptyString(data.processedAt)) {
    errors.push('payload.processedAt is required');
  }

  if (errors.length > 0) {
    throw new EventValidationError('Invalid payment.processed payload', errors);
  }
}

/**
 * Asserts {@link PaymentFailedPayload} shape (paymentId, orderId, reason, failedAt).
 * @throws {@link EventValidationError} when any required field is missing or empty
 */
export function validatePaymentFailedPayload(
  payload: unknown,
): asserts payload is PaymentFailedPayload {
  const errors: string[] = [];

  if (!payload || typeof payload !== 'object') {
    throw new EventValidationError('payment.failed payload must be an object');
  }

  const data = payload as Record<string, unknown>;

  if (!isNonEmptyString(data.paymentId)) {
    errors.push('payload.paymentId is required');
  }
  if (!isNonEmptyString(data.orderId)) {
    errors.push('payload.orderId is required');
  }
  if (!isNonEmptyString(data.reason)) {
    errors.push('payload.reason is required');
  }
  if (!isNonEmptyString(data.failedAt)) {
    errors.push('payload.failedAt is required');
  }

  if (errors.length > 0) {
    throw new EventValidationError('Invalid payment.failed payload', errors);
  }
}

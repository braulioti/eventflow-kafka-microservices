/**
 * Structural validator for `order.created` domain payload.
 *
 * Invoked after {@link validateEventEnvelope} inside {@link parseOrderEventsMessage}.
 * Ensures line items, totals, and currency are present before payment-service charges the order.
 */
import type { CurrencyCode, OrderItem } from './payloads/common';
import type { OrderCreatedPayload } from './payloads/order.events';
import { EventValidationError } from './validate-envelope';

/** Allowed ISO currency codes for `order.created` monetary fields. */
const CURRENCIES: readonly CurrencyCode[] = ['BRL', 'USD', 'EUR'];

function isPositiveNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0;
}

/** Validates a single line item; returns error strings (empty if valid). */
function validateOrderItem(item: unknown, index: number): string[] {
  const errors: string[] = [];

  if (!item || typeof item !== 'object') {
    return [`items[${index}] must be an object`];
  }

  const row = item as Record<string, unknown>;

  if (typeof row.productId !== 'string' || !row.productId.trim()) {
    errors.push(`items[${index}].productId is required`);
  }

  if (!isPositiveNumber(row.quantity) || row.quantity <= 0) {
    errors.push(`items[${index}].quantity must be a positive number`);
  }

  if (!isPositiveNumber(row.unitPrice)) {
    errors.push(`items[${index}].unitPrice must be a non-negative number`);
  }

  return errors;
}

/** Validates {@link OrderCreatedPayload} fields after envelope parsing. */
export function validateOrderCreatedPayload(
  payload: unknown,
): asserts payload is OrderCreatedPayload {
  const errors: string[] = [];

  if (!payload || typeof payload !== 'object') {
    throw new EventValidationError('order.created payload must be an object');
  }

  const data = payload as Record<string, unknown>;

  if (typeof data.orderId !== 'string' || !data.orderId.trim()) {
    errors.push('payload.orderId is required');
  }

  if (typeof data.customerId !== 'string' || !data.customerId.trim()) {
    errors.push('payload.customerId is required');
  }

  if (!Array.isArray(data.items) || data.items.length === 0) {
    errors.push('payload.items must be a non-empty array');
  } else {
    data.items.forEach((item, index) => {
      errors.push(...validateOrderItem(item, index));
    });
  }

  if (!isPositiveNumber(data.totalAmount)) {
    errors.push('payload.totalAmount must be a non-negative number');
  }

  if (
    typeof data.currency !== 'string' ||
    !CURRENCIES.includes(data.currency as CurrencyCode)
  ) {
    errors.push(`payload.currency must be one of: ${CURRENCIES.join(', ')}`);
  }

  if (errors.length > 0) {
    throw new EventValidationError('Invalid order.created payload', errors);
  }
}

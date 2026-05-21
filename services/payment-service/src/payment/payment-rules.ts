/**
 * @file payment-rules.ts
 * @module payment-service — pre-gateway business validation
 *
 * Pure functions that validate `order.created` payload **before** the payment
 * gateway simulator runs. Violations produce `payment.failed` with
 * `source: business_rule` — distinct from transient gateway errors.
 *
 * ## Rule categories
 *
 * - Amount bounds (`MIN_AMOUNT`, `MAX_AMOUNT`)
 * - Cart shape (`NON_EMPTY_ITEMS`, `MAX_ITEMS`)
 * - Arithmetic consistency (`AMOUNT_MATCHES_ITEMS` vs line totals)
 * - Per-line sanity (`POSITIVE_QUANTITY`, `NON_NEGATIVE_PRICE`)
 *
 * Thresholds are env-configurable via `resolvePaymentBusinessRules()`.
 *
 * @see PaymentService.processPayment
 */
import type { OrderCreatedPayload } from '@eventflow/shared';

/**
 * Environment-backed thresholds for order validation before charging.
 */
export interface PaymentBusinessRulesConfig {
  /** Minimum allowed `totalAmount` (inclusive). */
  minAmount: number;
  /** Maximum allowed `totalAmount` (inclusive). */
  maxAmount: number;
  /** Maximum number of line items in the order. */
  maxItems: number;
  /** Allowed drift between sum(items) and `totalAmount` (currency units). */
  amountTolerance: number;
}

/**
 * Single rule failure with machine-readable code and human message.
 */
export interface PaymentRuleViolation {
  rule: string;
  message: string;
}

/**
 * Builds the active rules config from env vars with optional test overrides.
 *
 * | Env variable              | Default   |
 * |---------------------------|-----------|
 * | `PAYMENT_MIN_AMOUNT`      | `0.01`    |
 * | `PAYMENT_MAX_AMOUNT`      | `10000`   |
 * | `PAYMENT_MAX_ITEMS`       | `50`      |
 * | `PAYMENT_AMOUNT_TOLERANCE`| `0.01`    |
 *
 * @param overrides - Partial config for unit tests
 */
export function resolvePaymentBusinessRules(
  overrides?: Partial<PaymentBusinessRulesConfig>,
): PaymentBusinessRulesConfig {
  return {
    minAmount: Number(process.env.PAYMENT_MIN_AMOUNT ?? 0.01),
    maxAmount: Number(process.env.PAYMENT_MAX_AMOUNT ?? 10_000),
    maxItems: Number(process.env.PAYMENT_MAX_ITEMS ?? 50),
    amountTolerance: Number(process.env.PAYMENT_AMOUNT_TOLERANCE ?? 0.01),
    ...overrides,
  };
}

/** Sums `quantity * unitPrice` across all line items. */
function sumLineItems(order: OrderCreatedPayload): number {
  return order.items.reduce(
    (sum, item) => sum + item.quantity * item.unitPrice,
    0,
  );
}

/**
 * Validates order payload against business rules.
 *
 * @param order - `order.created` payload
 * @param config - Rules snapshot (defaults from env)
 * @returns Empty array if charge may proceed to gateway simulation
 */
export function evaluatePaymentRules(
  order: OrderCreatedPayload,
  config: PaymentBusinessRulesConfig = resolvePaymentBusinessRules(),
): PaymentRuleViolation[] {
  const violations: PaymentRuleViolation[] = [];

  if (order.totalAmount < config.minAmount) {
    violations.push({
      rule: 'MIN_AMOUNT',
      message: `totalAmount ${order.totalAmount} is below minimum ${config.minAmount}`,
    });
  }

  if (order.totalAmount > config.maxAmount) {
    violations.push({
      rule: 'MAX_AMOUNT',
      message: `totalAmount ${order.totalAmount} exceeds maximum ${config.maxAmount}`,
    });
  }

  if (order.items.length === 0) {
    violations.push({
      rule: 'NON_EMPTY_ITEMS',
      message: 'order must contain at least one item',
    });
  }

  if (order.items.length > config.maxItems) {
    violations.push({
      rule: 'MAX_ITEMS',
      message: `order has ${order.items.length} items; maximum is ${config.maxItems}`,
    });
  }

  const computedTotal = sumLineItems(order);
  const drift = Math.abs(computedTotal - order.totalAmount);
  if (drift > config.amountTolerance) {
    violations.push({
      rule: 'AMOUNT_MATCHES_ITEMS',
      message: `totalAmount ${order.totalAmount} does not match items sum ${computedTotal}`,
    });
  }

  for (const item of order.items) {
    if (item.quantity <= 0) {
      violations.push({
        rule: 'POSITIVE_QUANTITY',
        message: `invalid quantity for product ${item.productId}`,
      });
    }
    if (item.unitPrice < 0) {
      violations.push({
        rule: 'NON_NEGATIVE_PRICE',
        message: `invalid unitPrice for product ${item.productId}`,
      });
    }
  }

  return violations;
}

/**
 * Collapses violations into a single semicolon-separated string for `payment.failed.reason`.
 *
 * @param violations - Output from {@link evaluatePaymentRules}
 */
export function formatRuleViolations(violations: PaymentRuleViolation[]): string {
  return violations.map((v) => `${v.rule}: ${v.message}`).join('; ');
}

/**
 * @file payment-simulator.ts
 * @module payment-service — gateway simulation (no real PSP)
 *
 * Stand-in for an external payment provider API. Uses randomness and env flags
 * to demonstrate success/failure paths, timeouts, and decline reasons in a
 * local Kafka lab without network calls to Stripe/Adyen/etc.
 *
 * ## Default behavior
 *
 * - ~80% approval / ~20% failure (`PAYMENT_FAILURE_RATE`, default `0.2`)
 * - `PAYMENT_FORCE_FAILURE=true` forces failure for chaos testing
 * - Failure scenarios rotate through {@link PAYMENT_FAILURE_SCENARIOS}
 * - `timeout` scenario optionally sleeps (`PAYMENT_TIMEOUT_DELAY_MS` or 3000ms)
 *
 * Business rules in `payment-rules.ts` run **before** this module in
 * {@link PaymentService.processPayment}.
 *
 * @see PaymentService
 */
import { randomUUID } from 'crypto';

/**
 * Categories of simulated gateway errors (for logs and failure typing).
 */
export type PaymentFailureType =
  | 'timeout'
  | 'gateway_unavailable'
  | 'card_declined'
  | 'connection_reset';

/** High-level simulation result used by PaymentService branching. */
export type PaymentSimulationOutcome = 'approved' | 'failed';

/**
 * Full outcome of {@link simulatePaymentGateway} including optional delay.
 */
export interface PaymentSimulationResult {
  outcome: PaymentSimulationOutcome;
  paymentId: string;
  status: 'approved' | 'failed';
  failureType?: PaymentFailureType;
  failureReason?: string;
  /** Non-zero when simulating slow/timeout gateway responses. */
  simulateDelayMs?: number;
}

/**
 * Catalog of realistic failure messages for demos and `payment.failed` payloads.
 */
export const PAYMENT_FAILURE_SCENARIOS: ReadonlyArray<{
  type: PaymentFailureType;
  message: string;
  delayMs?: number;
}> = [
  { type: 'timeout', message: 'Payment gateway timeout', delayMs: 3000 },
  { type: 'gateway_unavailable', message: 'Gateway unavailable' },
  { type: 'card_declined', message: 'Card declined' },
  { type: 'connection_reset', message: 'Connection reset by peer' },
];

/**
 * Reads `PAYMENT_FAILURE_RATE` (0–1). NaN falls back to 0.2.
 *
 * Equivalent intent: `const shouldFail = Math.random() < failureRate`
 */
export function resolvePaymentFailureRate(): number {
  const raw = Number(process.env.PAYMENT_FAILURE_RATE ?? 0.2);
  if (Number.isNaN(raw)) {
    return 0.2;
  }
  return Math.min(1, Math.max(0, raw));
}

/** True when `PAYMENT_FORCE_FAILURE` env is exactly `'true'`. */
export function isPaymentForceFailureEnabled(): boolean {
  return process.env.PAYMENT_FORCE_FAILURE === 'true';
}

/** Picks a random entry from {@link PAYMENT_FAILURE_SCENARIOS}. */
export function pickSimulatedFailure(): (typeof PAYMENT_FAILURE_SCENARIOS)[number] {
  const index = Math.floor(Math.random() * PAYMENT_FAILURE_SCENARIOS.length);
  return PAYMENT_FAILURE_SCENARIOS[index]!;
}

/**
 * Simulates an external payment gateway approve/decline decision.
 *
 * @param _orderId - Reserved for future per-order scenarios (unused)
 * @param options.paymentId - Reuse id from `payment.requested` when provided
 * @param options.failureRate - Override env failure rate
 * @param options.forceFailure - Override env force-failure flag
 */
export function simulatePaymentGateway(
  _orderId: string,
  options?: {
    paymentId?: string;
    failureRate?: number;
    forceFailure?: boolean;
  },
): PaymentSimulationResult {
  const paymentId = options?.paymentId ?? randomUUID();
  const failureRate = options?.failureRate ?? resolvePaymentFailureRate();
  const forceFailure = options?.forceFailure ?? isPaymentForceFailureEnabled();
  const shouldFail = forceFailure || Math.random() < failureRate;

  if (!shouldFail) {
    return {
      outcome: 'approved',
      paymentId,
      status: 'approved',
    };
  }

  const scenario = pickSimulatedFailure();
  const timeoutDelay = Number(process.env.PAYMENT_TIMEOUT_DELAY_MS ?? scenario.delayMs ?? 0);

  return {
    outcome: 'failed',
    paymentId,
    status: 'failed',
    failureType: scenario.type,
    failureReason: scenario.message,
    simulateDelayMs: scenario.type === 'timeout' ? timeoutDelay : undefined,
  };
}

/**
 * Promise-based delay for timeout simulation in PaymentService.
 *
 * @param ms - Milliseconds to wait before resolving
 */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

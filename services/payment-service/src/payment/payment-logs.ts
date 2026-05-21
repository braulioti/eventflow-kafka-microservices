/**
 * @file payment-logs.ts
 * @module payment-service — structured logging helpers
 *
 * Centralizes multi-line, grep-friendly log blocks for the payment Kafka flow.
 * Tag prefixes (`[EVENT RECEIVED]`, `[PAYMENT SUCCESS]`, etc.) align with
 * EventFlow documentation and terminal filtering during saga demos.
 *
 * ## Log stages in a happy path
 *
 * 1. `[EVENT RECEIVED]` — consumer got `order.created`
 * 2. `[PAYMENT STARTED]` — `PaymentService.processPayment` entered
 * 3. `[KAFKA PUBLISH]` — `payment.requested` (in PaymentService logger)
 * 4. `[PAYMENT SUCCESS]` or `[PAYMENT FAILED]` — terminal outcome
 * 5. SQLite row via `ProcessedEventsService` (no dedicated log tag)
 *
 * Idempotency skips use `[IDEMPOTENCY SKIP]`; consumer retries use `[PAYMENT RETRY]`.
 */
import { Logger } from '@nestjs/common';

/**
 * Logs inbound Kafka consumption metadata (topic, ids, retry header).
 */
export function logEventReceived(
  logger: Logger,
  params: {
    topic: string;
    eventType: string;
    eventId: string;
    orderId: string;
    partitionKey?: string;
    retryCount?: number;
  },
): void {
  logger.log(`
[EVENT RECEIVED]
topic=${params.topic}
eventType=${params.eventType}
eventId=${params.eventId}
orderId=${params.orderId}
partitionKey=${params.partitionKey ?? 'n/a'}
retryCount=${params.retryCount ?? 0}
`.trim());
}

/**
 * Logs when idempotency short-circuits before payment logic runs.
 */
export function logIdempotencySkip(
  logger: Logger,
  params: { reason: string; eventId: string; orderId: string },
): void {
  logger.warn(`
[IDEMPOTENCY SKIP]
reason=${params.reason}
eventId=${params.eventId}
orderId=${params.orderId}
`.trim());
}

/**
 * Logs consumer retry scheduling (from `x-retry-count` headers).
 */
export function logRetryScheduled(
  logger: Logger,
  params: {
    eventId: string;
    orderId: string;
    attempt: number;
    maxAttempts: number;
    backoffMs: number;
  },
): void {
  logger.warn(`
[PAYMENT RETRY]
eventId=${params.eventId}
orderId=${params.orderId}
attempt=${params.attempt}/${params.maxAttempts}
backoffMs=${params.backoffMs}
`.trim());
}

/**
 * Logs successful simulated charge and published `payment.processed`.
 */
export function logPaymentSuccess(
  logger: Logger,
  params: {
    orderId: string;
    paymentId: string;
    transactionId?: string;
    amount?: number;
    currency?: string;
  },
): void {
  logger.log(`
[PAYMENT SUCCESS]
orderId=${params.orderId}
paymentId=${params.paymentId}
status=approved
transactionId=${params.transactionId ?? 'n/a'}
amount=${params.amount ?? 'n/a'}
currency=${params.currency ?? 'n/a'}
`.trim());
}

/**
 * Logs decline from business rules or gateway simulation before `payment.failed`.
 */
export function logPaymentFailed(
  logger: Logger,
  params: {
    orderId: string;
    paymentId: string;
    reason: string;
    failureType?: string;
    source?: 'business_rule' | 'gateway_simulation';
  },
): void {
  logger.error(`
[PAYMENT FAILED]
orderId=${params.orderId}
paymentId=${params.paymentId}
reason=${params.reason}
failureType=${params.failureType ?? 'n/a'}
source=${params.source ?? 'gateway_simulation'}
`.trim());
}

/**
 * Logs entry into `processPayment` with configured simulation failure rate.
 */
export function logPaymentStarted(
  logger: Logger,
  params: { orderId: string; amount: number; currency: string; failureRate: number },
): void {
  logger.log(`
[PAYMENT STARTED]
orderId=${params.orderId}
amount=${params.amount}
currency=${params.currency}
simulatedFailureRate=${params.failureRate}
expectedApprovalRate=${1 - params.failureRate}
`.trim());
}

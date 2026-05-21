/**
 * Payment bounded context event payloads.
 *
 * Success/failure outcomes on `payment.events` drive stock reservation or order/notification
 * compensation; `payment.requested` is an internal step on a dedicated topic.
 */
import type { CurrencyCode } from './common';

/**
 * Payload for {@link EventType.PAYMENT_REQUESTED} — payment-service self-consume / audit step.
 */
export interface PaymentRequestedPayload {
  paymentId: string;
  orderId: string;
  amount: number;
  currency: CurrencyCode;
}

/** Success path — triggers stock reservation. */
export interface PaymentProcessedPayload {
  paymentId: string;
  orderId: string;
  transactionId: string;
  processedAt: string;
}

/** Failure path — order-service and notification-service react. */
export interface PaymentFailedPayload {
  paymentId: string;
  orderId: string;
  reason: string;
  failedAt: string;
}

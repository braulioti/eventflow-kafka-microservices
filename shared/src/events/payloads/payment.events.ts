import type { CurrencyCode } from './common';

/** Internal step: payment-service accepted work for an order. */
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

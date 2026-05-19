import type { CurrencyCode, OrderItem } from './common';

/** Emitted when a customer places an order; starts the happy-path saga. */
export interface OrderCreatedPayload {
  /** Partition key and correlationId for downstream events */
  orderId: string;
  customerId: string;
  items: OrderItem[];
  totalAmount: number;
  currency: CurrencyCode;
}

/** Emitted when an order is voided; fans out to payment, stock, and notification. */
export interface OrderCancelledPayload {
  orderId: string;
  reason: string;
  /** Who initiated the cancellation (audit / routing hints) */
  cancelledBy: 'customer' | 'system' | 'admin';
}

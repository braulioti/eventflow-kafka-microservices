/**
 * Order bounded context event payloads.
 *
 * `order.created` is the saga entry point published to `order.events`; `order.cancelled`
 * fans out compensation signals to payment, stock, and notification services.
 */
import type { CurrencyCode, OrderItem } from './common';

/**
 * Payload for {@link EventType.ORDER_CREATED} — triggers payment-service consumption.
 * `orderId` doubles as Kafka partition key and envelope `correlationId`.
 */
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

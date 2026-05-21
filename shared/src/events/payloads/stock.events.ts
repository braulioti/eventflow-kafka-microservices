/**
 * Stock / inventory bounded context event payloads.
 *
 * Emitted after successful payment; `stock.reserved` advances the saga to notification.
 * Release and failure events support cancellation and compensation paths.
 */
import type { OrderItem } from './common';

/**
 * Payload for {@link EventType.STOCK_RESERVED} — confirms inventory hold for the order.
 */
export interface StockReservedPayload {
  reservationId: string;
  orderId: string;
  items: OrderItem[];
  reservedAt: string;
}

/** Reservation undone (e.g. after cancellation). */
export interface StockReleasedPayload {
  reservationId: string;
  orderId: string;
  reason: string;
  releasedAt: string;
}

/** Could not reserve stock; order-service may compensate. */
export interface StockFailedPayload {
  orderId: string;
  reason: string;
  failedAt: string;
}

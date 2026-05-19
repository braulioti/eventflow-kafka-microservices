import type { OrderItem } from './common';

/** Inventory held for the order; triggers customer notification. */
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

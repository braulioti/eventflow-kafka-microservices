/**
 * Shared value objects referenced by multiple domain payload modules.
 *
 * Keeps order line items and currency codes consistent between `order.created` and
 * `stock.reserved` events (same product/qty/price shape for reservation replay).
 */

/** Single catalog line on an order or stock reservation (product, quantity, unit price). */
export interface OrderItem {
  productId: string;
  quantity: number;
  unitPrice: number;
}

/** Supported ISO currency codes for monetary fields. */
export type CurrencyCode = 'BRL' | 'USD' | 'EUR';

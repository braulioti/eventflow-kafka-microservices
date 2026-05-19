/**
 * Canonical event type names (also used as Kafka topic names).
 * Format: {domain}.{action}
 */
export const EventType = {
  ORDER_CREATED: 'order.created',
  ORDER_CANCELLED: 'order.cancelled',
  PAYMENT_REQUESTED: 'payment.requested',
  PAYMENT_PROCESSED: 'payment.processed',
  PAYMENT_FAILED: 'payment.failed',
  STOCK_RESERVED: 'stock.reserved',
  STOCK_RELEASED: 'stock.released',
  STOCK_FAILED: 'stock.failed',
  NOTIFICATION_SEND: 'notification.send',
  NOTIFICATION_SENT: 'notification.sent',
  NOTIFICATION_FAILED: 'notification.failed',
} as const;

export type EventTypeValue = (typeof EventType)[keyof typeof EventType];

export const ALL_EVENT_TYPES: readonly EventTypeValue[] = Object.values(EventType);

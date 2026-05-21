/**
 * Canonical registry of all domain event type strings in EventFlow.
 *
 * **Naming:** `{domain}.{action}` (e.g. `order.created`). These values are the `eventType`
 * field inside every envelope. Physical Kafka topics sometimes differ — see
 * {@link resolveKafkaTopic} and {@link domain-topics}.
 *
 * **Usage:** Producers pass `EventType.*` to publishers; consumers filter and validate
 * against the same constants to avoid string drift across services.
 */

/** All supported domain event identifiers keyed by constant name. */
export const EventType = {
  // --- Order bounded context ---
  /** Saga start: customer placed order (topic: `order.events`). */
  ORDER_CREATED: 'order.created',
  /** Compensation: order voided before/during fulfillment. */
  ORDER_CANCELLED: 'order.cancelled',

  // --- Payment bounded context ---
  /** Internal: payment processing initiated for an order. */
  PAYMENT_REQUESTED: 'payment.requested',
  /** Happy path: funds captured (topic: `payment.events`). */
  PAYMENT_PROCESSED: 'payment.processed',
  /** Failure path: charge declined (topic: `payment.events`). */
  PAYMENT_FAILED: 'payment.failed',

  // --- Stock bounded context ---
  /** Inventory reserved after successful payment. */
  STOCK_RESERVED: 'stock.reserved',
  /** Previously held stock released (cancellation). */
  STOCK_RELEASED: 'stock.released',
  /** Could not reserve inventory for the order. */
  STOCK_FAILED: 'stock.failed',

  // --- Notification bounded context ---
  /** Dispatch requested (template + channel). */
  NOTIFICATION_SEND: 'notification.send',
  /** Delivery confirmed — core flow terminal success for order-service. */
  NOTIFICATION_SENT: 'notification.sent',
  /** Delivery failed — observed by dlq-service. */
  NOTIFICATION_FAILED: 'notification.failed',
} as const;

/** Union of every value in {@link EventType} — safe for switches and catalog lookups. */
export type EventTypeValue = (typeof EventType)[keyof typeof EventType];

/**
 * Flat list of all event types (topic provisioning, integration tests, catalog validation).
 * Order follows object definition order, not saga sequence.
 */
export const ALL_EVENT_TYPES: readonly EventTypeValue[] = Object.values(EventType);

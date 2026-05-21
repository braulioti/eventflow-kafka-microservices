/**
 * Physical Kafka topic names for aggregate-style streams.
 *
 * EventFlow uses **topic-per-aggregate** for high-volume domains: multiple `eventType`
 * values can share one topic (e.g. `payment.events` carries both `payment.processed` and
 * `payment.failed`). Envelope `eventType` remains the fine-grained discriminator; consumers
 * filter with {@link isPaymentProcessedEvent} and related guards.
 *
 * See {@link resolveKafkaTopic} and {@link EVENT_CATALOG} for full routing tables.
 */

/** Topics owned by the order bounded context. */
export const OrderKafkaTopic = {
  /** Order bounded context stream (carries order.created, etc.) */
  ORDER_EVENTS: 'order.events',
} as const;

/** Union of all {@link OrderKafkaTopic} string values. */
export type OrderKafkaTopicValue =
  (typeof OrderKafkaTopic)[keyof typeof OrderKafkaTopic];

/** Topics owned by the payment bounded context (success and failure on one stream). */
export const PaymentKafkaTopic = {
  PAYMENT_EVENTS: 'payment.events',
} as const;

/** Union of all {@link PaymentKafkaTopic} string values. */
export type PaymentKafkaTopicValue =
  (typeof PaymentKafkaTopic)[keyof typeof PaymentKafkaTopic];

/**
 * Kafka topic names for domain aggregates (may differ from {@link EventType}).
 * Envelope `eventType` stays canonical; routing uses these topic constants.
 */
export const OrderKafkaTopic = {
  /** Order bounded context stream (carries order.created, etc.) */
  ORDER_EVENTS: 'order.events',
} as const;

export type OrderKafkaTopicValue =
  (typeof OrderKafkaTopic)[keyof typeof OrderKafkaTopic];

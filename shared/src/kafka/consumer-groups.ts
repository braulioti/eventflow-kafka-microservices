/**
 * Kafka consumer group identifiers — one stable group per microservice.
 *
 * Consumer groups define offset commit boundaries: all instances of payment-service share
 * `eventflow.payment-service` and compete for partitions. Override via env vars resolved
 * in {@link resolveConsumerGroup} for blue/green or local multi-instance testing.
 */
import type { ServiceName } from '../events/event-catalog';

/**
 * Default consumer group id prefix per service (`eventflow.<service>`).
 * Horizontal scaling adds consumers with the same groupId to share partition load.
 */
export const ConsumerGroup = {
  ORDER: 'eventflow.order-service',
  PAYMENT: 'eventflow.payment-service',
  STOCK: 'eventflow.stock-service',
  NOTIFICATION: 'eventflow.notification-service',
  DLQ: 'eventflow.dlq-service',
} as const;

/** Lookup table from {@link ServiceName} to default {@link ConsumerGroup} string. */
export const CONSUMER_GROUP_BY_SERVICE: Record<ServiceName, string> = {
  'order-service': ConsumerGroup.ORDER,
  'payment-service': ConsumerGroup.PAYMENT,
  'stock-service': ConsumerGroup.STOCK,
  'notification-service': ConsumerGroup.NOTIFICATION,
  'dlq-service': ConsumerGroup.DLQ,
};

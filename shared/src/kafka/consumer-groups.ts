import type { ServiceName } from '../events/event-catalog';

/** Consumer group per service — enables horizontal scaling within a service */
export const ConsumerGroup = {
  ORDER: 'eventflow.order-service',
  PAYMENT: 'eventflow.payment-service',
  STOCK: 'eventflow.stock-service',
  NOTIFICATION: 'eventflow.notification-service',
  DLQ: 'eventflow.dlq-service',
} as const;

export const CONSUMER_GROUP_BY_SERVICE: Record<ServiceName, string> = {
  'order-service': ConsumerGroup.ORDER,
  'payment-service': ConsumerGroup.PAYMENT,
  'stock-service': ConsumerGroup.STOCK,
  'notification-service': ConsumerGroup.NOTIFICATION,
  'dlq-service': ConsumerGroup.DLQ,
};

import { OrderKafkaTopic } from './domain-topics';
import { EventType, type EventTypeValue } from './event-types';
import { KafkaTopic, toDlqTopic } from './kafka-topics';

export type ServiceName =
  | 'order-service'
  | 'payment-service'
  | 'stock-service'
  | 'notification-service'
  | 'dlq-service';

export interface EventCatalogEntry {
  eventType: EventTypeValue;
  topic: string;
  dlqTopic: string;
  producer: ServiceName;
  consumers: ServiceName[];
  /** Kafka message key — keeps related events in the same partition */
  partitionKey: string;
  description: string;
}

/**
 * System event catalog: ownership, routing, and flow documentation.
 */
export const EVENT_CATALOG: readonly EventCatalogEntry[] = [
  {
    eventType: EventType.ORDER_CREATED,
    topic: OrderKafkaTopic.ORDER_EVENTS,
    dlqTopic: toDlqTopic(OrderKafkaTopic.ORDER_EVENTS),
    producer: 'order-service',
    consumers: ['payment-service'],
    partitionKey: 'orderId',
    description:
      'A new order was placed and is ready for payment processing (published to order.events).',
  },
  {
    eventType: EventType.ORDER_CANCELLED,
    topic: KafkaTopic.ORDER_CANCELLED,
    dlqTopic: toDlqTopic(KafkaTopic.ORDER_CANCELLED),
    producer: 'order-service',
    consumers: ['payment-service', 'stock-service', 'notification-service'],
    partitionKey: 'orderId',
    description: 'An order was cancelled before or during fulfillment.',
  },
  {
    eventType: EventType.PAYMENT_REQUESTED,
    topic: KafkaTopic.PAYMENT_REQUESTED,
    dlqTopic: toDlqTopic(KafkaTopic.PAYMENT_REQUESTED),
    producer: 'payment-service',
    consumers: ['payment-service'],
    partitionKey: 'orderId',
    description: 'Payment processing was initiated for an order.',
  },
  {
    eventType: EventType.PAYMENT_PROCESSED,
    topic: KafkaTopic.PAYMENT_PROCESSED,
    dlqTopic: toDlqTopic(KafkaTopic.PAYMENT_PROCESSED),
    producer: 'payment-service',
    consumers: ['stock-service'],
    partitionKey: 'orderId',
    description: 'Payment completed successfully.',
  },
  {
    eventType: EventType.PAYMENT_FAILED,
    topic: KafkaTopic.PAYMENT_FAILED,
    dlqTopic: toDlqTopic(KafkaTopic.PAYMENT_FAILED),
    producer: 'payment-service',
    consumers: ['order-service', 'notification-service'],
    partitionKey: 'orderId',
    description: 'Payment could not be completed.',
  },
  {
    eventType: EventType.STOCK_RESERVED,
    topic: KafkaTopic.STOCK_RESERVED,
    dlqTopic: toDlqTopic(KafkaTopic.STOCK_RESERVED),
    producer: 'stock-service',
    consumers: ['notification-service'],
    partitionKey: 'orderId',
    description: 'Inventory was reserved for the order.',
  },
  {
    eventType: EventType.STOCK_RELEASED,
    topic: KafkaTopic.STOCK_RELEASED,
    dlqTopic: toDlqTopic(KafkaTopic.STOCK_RELEASED),
    producer: 'stock-service',
    consumers: ['order-service'],
    partitionKey: 'orderId',
    description: 'Previously reserved stock was released.',
  },
  {
    eventType: EventType.STOCK_FAILED,
    topic: KafkaTopic.STOCK_FAILED,
    dlqTopic: toDlqTopic(KafkaTopic.STOCK_FAILED),
    producer: 'stock-service',
    consumers: ['order-service', 'notification-service'],
    partitionKey: 'orderId',
    description: 'Stock could not be reserved.',
  },
  {
    eventType: EventType.NOTIFICATION_SEND,
    topic: KafkaTopic.NOTIFICATION_SEND,
    dlqTopic: toDlqTopic(KafkaTopic.NOTIFICATION_SEND),
    producer: 'notification-service',
    consumers: ['notification-service'],
    partitionKey: 'orderId',
    description: 'A notification dispatch was requested.',
  },
  {
    eventType: EventType.NOTIFICATION_SENT,
    topic: KafkaTopic.NOTIFICATION_SENT,
    dlqTopic: toDlqTopic(KafkaTopic.NOTIFICATION_SENT),
    producer: 'notification-service',
    consumers: ['order-service'],
    partitionKey: 'orderId',
    description: 'Notification was delivered successfully.',
  },
  {
    eventType: EventType.NOTIFICATION_FAILED,
    topic: KafkaTopic.NOTIFICATION_FAILED,
    dlqTopic: toDlqTopic(KafkaTopic.NOTIFICATION_FAILED),
    producer: 'notification-service',
    consumers: ['dlq-service'],
    partitionKey: 'orderId',
    description: 'Notification delivery failed.',
  },
];

/** Events this service is allowed to publish (ownership in the catalog). */
export function getEventsByProducer(service: ServiceName): EventCatalogEntry[] {
  return EVENT_CATALOG.filter((entry) => entry.producer === service);
}

/** Events this service subscribes to (may be multiple per topic via consumer group). */
export function getEventsByConsumer(service: ServiceName): EventCatalogEntry[] {
  return EVENT_CATALOG.filter((entry) => entry.consumers.includes(service));
}

/** Lookup routing metadata for a single event type. */
export function getCatalogEntry(eventType: EventTypeValue): EventCatalogEntry | undefined {
  return EVENT_CATALOG.find((entry) => entry.eventType === eventType);
}

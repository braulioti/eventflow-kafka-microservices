/**
 * Authoritative event catalog for EventFlow — ownership, topics, and consumer matrix.
 *
 * Single source of truth mirrored in `docs/EVENT_CATALOG.md`. Each {@link EventCatalogEntry}
 * records who produces/consumes an event, which Kafka topic and DLQ apply, and the partition
 * key field. Services use {@link getEventsByProducer} / {@link getEventsByConsumer} at
 * startup to validate subscriptions and for operational discovery.
 */
import { OrderKafkaTopic, PaymentKafkaTopic } from './domain-topics';
import { EventType, type EventTypeValue } from './event-types';
import { KafkaTopic, toDlqTopic } from './kafka-topics';

/** Microservice identifiers that participate in the EventFlow Kafka mesh. */
export type ServiceName =
  | 'order-service'
  | 'payment-service'
  | 'stock-service'
  | 'notification-service'
  | 'dlq-service';

/** One row in the system event catalog — routing and ownership for a single event type. */
export interface EventCatalogEntry {
  /** Canonical envelope `eventType` (not always equal to `topic`). */
  eventType: EventTypeValue;
  /** Physical Kafka topic name (may be aggregate stream like `order.events`). */
  topic: string;
  /** Dead-letter topic where exhausted retries land (`{topic}.dlq`). */
  dlqTopic: string;
  /** Sole publisher for this event in the reference architecture. */
  producer: ServiceName;
  /** Services that subscribe via their consumer group (may share a topic). */
  consumers: ServiceName[];
  /** Payload field used as Kafka message key — always `orderId` in EventFlow. */
  partitionKey: string;
  /** Human-readable saga step description for docs and onboarding. */
  description: string;
}

/**
 * Full catalog of domain events in the reference order-processing saga.
 * Order matters for readability only; lookups use {@link getCatalogEntry}.
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
    topic: PaymentKafkaTopic.PAYMENT_EVENTS,
    dlqTopic: toDlqTopic(PaymentKafkaTopic.PAYMENT_EVENTS),
    producer: 'payment-service',
    consumers: ['stock-service'],
    partitionKey: 'orderId',
    description: 'Payment completed successfully (published to payment.events).',
  },
  {
    eventType: EventType.PAYMENT_FAILED,
    topic: PaymentKafkaTopic.PAYMENT_EVENTS,
    dlqTopic: toDlqTopic(PaymentKafkaTopic.PAYMENT_EVENTS),
    producer: 'payment-service',
    consumers: ['order-service', 'notification-service'],
    partitionKey: 'orderId',
    description: 'Payment could not be completed (published to payment.events).',
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

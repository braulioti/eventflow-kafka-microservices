/**
 * Compile-time map from event type string to payload interface.
 * Use with {@link TypedEventEnvelope} for type-safe Kafka handlers and publishers.
 */
import { EventType } from './event-types';
import type {
  NotificationFailedPayload,
  NotificationSendPayload,
  NotificationSentPayload,
} from './payloads/notification.events';
import type {
  OrderCancelledPayload,
  OrderCreatedPayload,
} from './payloads/order.events';
import type {
  PaymentFailedPayload,
  PaymentProcessedPayload,
  PaymentRequestedPayload,
} from './payloads/payment.events';
import type {
  StockFailedPayload,
  StockReleasedPayload,
  StockReservedPayload,
} from './payloads/stock.events';

/** Maps each event type to its payload shape for type-safe producers/consumers */
export interface EventPayloadMap {
  [EventType.ORDER_CREATED]: OrderCreatedPayload;
  [EventType.ORDER_CANCELLED]: OrderCancelledPayload;
  [EventType.PAYMENT_REQUESTED]: PaymentRequestedPayload;
  [EventType.PAYMENT_PROCESSED]: PaymentProcessedPayload;
  [EventType.PAYMENT_FAILED]: PaymentFailedPayload;
  [EventType.STOCK_RESERVED]: StockReservedPayload;
  [EventType.STOCK_RELEASED]: StockReleasedPayload;
  [EventType.STOCK_FAILED]: StockFailedPayload;
  [EventType.NOTIFICATION_SEND]: NotificationSendPayload;
  [EventType.NOTIFICATION_SENT]: NotificationSentPayload;
  [EventType.NOTIFICATION_FAILED]: NotificationFailedPayload;
}

/** Payload type for a given event type key. */
export type EventPayload<T extends keyof EventPayloadMap> = EventPayloadMap[T];

/** Envelope whose payload matches the event type. */
export type TypedEventEnvelope<T extends keyof EventPayloadMap> = import('./envelope').EventEnvelope<
  EventPayloadMap[T]
>;

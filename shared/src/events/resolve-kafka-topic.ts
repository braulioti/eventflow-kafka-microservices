/**
 * Resolves canonical `eventType` strings to physical Kafka topic names.
 *
 * Most events use 1:1 mapping (`eventType` === topic). Aggregate streams override here;
 * publishers and {@link KafkaRetryExecutor} call {@link resolveKafkaTopic} so republish/DLQ
 * target the correct topic without hardcoding `order.events` in each service.
 */
import { OrderKafkaTopic, PaymentKafkaTopic } from './domain-topics';
import { EventType, type EventTypeValue } from './event-types';

/**
 * Explicit overrides where topic name ≠ `eventType`.
 * Unlisted types default to their `eventType` string (see {@link resolveKafkaTopic}).
 */
const EVENT_TO_KAFKA_TOPIC: Partial<Record<EventTypeValue, string>> = {
  [EventType.ORDER_CREATED]: OrderKafkaTopic.ORDER_EVENTS,
  [EventType.PAYMENT_PROCESSED]: PaymentKafkaTopic.PAYMENT_EVENTS,
  [EventType.PAYMENT_FAILED]: PaymentKafkaTopic.PAYMENT_EVENTS,
};

/**
 * Resolves the Kafka topic for publishing/consuming an event.
 * Defaults to the event type name when no domain topic override exists.
 */
export function resolveKafkaTopic(eventType: EventTypeValue): string {
  return EVENT_TO_KAFKA_TOPIC[eventType] ?? eventType;
}

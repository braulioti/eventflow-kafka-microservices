import { OrderKafkaTopic } from './domain-topics';
import { EventType, type EventTypeValue } from './event-types';

/** Maps event types to physical Kafka topics when they differ from eventType. */
const EVENT_TO_KAFKA_TOPIC: Partial<Record<EventTypeValue, string>> = {
  [EventType.ORDER_CREATED]: OrderKafkaTopic.ORDER_EVENTS,
};

/**
 * Resolves the Kafka topic for publishing/consuming an event.
 * Defaults to the event type name when no domain topic override exists.
 */
export function resolveKafkaTopic(eventType: EventTypeValue): string {
  return EVENT_TO_KAFKA_TOPIC[eventType] ?? eventType;
}

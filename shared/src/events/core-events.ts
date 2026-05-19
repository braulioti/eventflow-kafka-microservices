import { EventType, type EventTypeValue } from './event-types';

/**
 * Core domain events that define the primary order-processing flow.
 * Other events in the catalog support cancellations, internal steps, and failures.
 */
export const CORE_SYSTEM_EVENTS = [
  EventType.ORDER_CREATED,
  EventType.PAYMENT_PROCESSED,
  EventType.PAYMENT_FAILED,
  EventType.STOCK_RESERVED,
  EventType.NOTIFICATION_SENT,
] as const satisfies readonly EventTypeValue[];

export type CoreSystemEvent = (typeof CORE_SYSTEM_EVENTS)[number];

/** Happy-path sequence (failure branch: payment.failed) */
export const CORE_EVENT_FLOW: readonly CoreSystemEvent[] = [
  EventType.ORDER_CREATED,
  EventType.PAYMENT_PROCESSED,
  EventType.STOCK_RESERVED,
  EventType.NOTIFICATION_SENT,
];

export const CORE_EVENT_FLOW_LABEL =
  'order.created → payment.processed → stock.reserved → notification.sent';

export const CORE_FAILURE_EVENT: CoreSystemEvent = EventType.PAYMENT_FAILED;

/** Narrows an event type to the documented happy-path / primary failure set. */
export function isCoreSystemEvent(
  eventType: EventTypeValue,
): eventType is CoreSystemEvent {
  return (CORE_SYSTEM_EVENTS as readonly EventTypeValue[]).includes(eventType);
}

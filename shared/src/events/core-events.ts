/**
 * Happy-path and primary-failure subset of the full event catalog.
 *
 * Used in documentation, demos, and `verify-payment-full-flow` to describe the minimal
 * order → payment → stock → notification saga without auxiliary events like
 * `payment.requested` or `stock.released`.
 */
import { EventType, type EventTypeValue } from './event-types';

/**
 * Events considered "core" to understanding the main business flow.
 * Superset of {@link CORE_EVENT_FLOW} plus {@link CORE_FAILURE_EVENT}.
 */
export const CORE_SYSTEM_EVENTS = [
  EventType.ORDER_CREATED,
  EventType.PAYMENT_PROCESSED,
  EventType.PAYMENT_FAILED,
  EventType.STOCK_RESERVED,
  EventType.NOTIFICATION_SENT,
] as const satisfies readonly EventTypeValue[];

/** Narrowed type for events in {@link CORE_SYSTEM_EVENTS}. */
export type CoreSystemEvent = (typeof CORE_SYSTEM_EVENTS)[number];

/**
 * Ordered happy-path sequence for diagrams and integration checks.
 * On payment failure the branch emits {@link CORE_FAILURE_EVENT} instead of continuing.
 */
export const CORE_EVENT_FLOW: readonly CoreSystemEvent[] = [
  EventType.ORDER_CREATED,
  EventType.PAYMENT_PROCESSED,
  EventType.STOCK_RESERVED,
  EventType.NOTIFICATION_SENT,
];

/** Human-readable arrow chain matching {@link CORE_EVENT_FLOW} (logs, README). */
export const CORE_EVENT_FLOW_LABEL =
  'order.created → payment.processed → stock.reserved → notification.sent';

/** Primary compensating/failure event on the payment step of the core flow. */
export const CORE_FAILURE_EVENT: CoreSystemEvent = EventType.PAYMENT_FAILED;

/** Narrows an event type to the documented happy-path / primary failure set. */
export function isCoreSystemEvent(
  eventType: EventTypeValue,
): eventType is CoreSystemEvent {
  return (CORE_SYSTEM_EVENTS as readonly EventTypeValue[]).includes(eventType);
}

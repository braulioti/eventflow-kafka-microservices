/**
 * Idempotency strategies for EventFlow producers and consumers.
 *
 * **Producer (order.created):** {@link IdempotencyStrategy.EVENT_ID_PER_AGGREGATE}
 * — one `eventId` per order; persist before publish; skip republish if already set.
 *
 * **Consumer:** dedupe by `x-event-id` / `envelope.eventId` (see payment handler).
 *
 * **Partition key:** always `orderId` so all events for one order stay ordered.
 */
export const IdempotencyStrategy = {
  /** Single outbound event per aggregate, keyed by envelope eventId */
  EVENT_ID_PER_AGGREGATE: 'event-id-per-aggregate',
  /** Consumers ignore duplicate eventId within a processing window */
  CONSUMER_EVENT_ID_DEDUPE: 'consumer-event-id-dedupe',
} as const;

export type IdempotencyStrategyValue =
  (typeof IdempotencyStrategy)[keyof typeof IdempotencyStrategy];

/** True when the aggregate already has a published event id stored. */
export function hasPublishedEventId(eventId: string | null | undefined): boolean {
  return typeof eventId === 'string' && eventId.length > 0;
}

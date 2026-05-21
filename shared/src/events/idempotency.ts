/**
 * Idempotency conventions for EventFlow producers and consumers.
 *
 * Kafka delivers at-least-once; retries and republish can duplicate messages. This module
 * documents the agreed strategies and provides small helpers — actual storage (TypeORM
 * `processed_events`, order aggregate `eventId` column) lives in each service.
 *
 * **Producer (order.created):** {@link IdempotencyStrategy.EVENT_ID_PER_AGGREGATE}
 * — one `eventId` per order; persist before publish; skip republish if already set.
 *
 * **Consumer:** dedupe by `x-event-id` / `envelope.eventId` (see payment-service).
 *
 * **Partition key:** always `orderId` so all events for one order stay ordered per partition.
 */

/** Named idempotency patterns referenced in docs and service implementations. */
export const IdempotencyStrategy = {
  /** Single outbound event per aggregate, keyed by envelope eventId */
  EVENT_ID_PER_AGGREGATE: 'event-id-per-aggregate',
  /** Consumers ignore duplicate eventId within a processing window */
  CONSUMER_EVENT_ID_DEDUPE: 'consumer-event-id-dedupe',
} as const;

/** Union of {@link IdempotencyStrategy} string values. */
export type IdempotencyStrategyValue =
  (typeof IdempotencyStrategy)[keyof typeof IdempotencyStrategy];

/**
 * Whether an aggregate already recorded a published `eventId` (order-service guard).
 * @param eventId - Stored id from DB, or null/undefined if not yet published
 */
export function hasPublishedEventId(eventId: string | null | undefined): boolean {
  return typeof eventId === 'string' && eventId.length > 0;
}

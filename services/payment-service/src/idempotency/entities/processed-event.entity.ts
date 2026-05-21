/**
 * @file processed-event.entity.ts
 * @module payment-service — idempotency ORM model
 *
 * Maps the `processed_events` SQLite table. Each row records that a specific
 * **inbound Kafka event** (by envelope `eventId`) has been handled, along with
 * the business outcome for saga observability and duplicate-order detection.
 *
 * ## Table semantics
 *
 * | Column            | Purpose                                                |
 * |-------------------|--------------------------------------------------------|
 * | `inbound_event_id`| Primary key — idempotency token from consumed envelope |
 * | `order_id`        | Indexed — find prior approved payment for same order   |
 * | `outcome`         | `approved` \| `failed` \| `skipped_duplicate_order`    |
 * | `payment_id`      | Correlates to published `payment.*` events (nullable)  |
 *
 * ## Interaction with Kafka
 *
 * Written **after** successful or terminal failure handling in `PaymentService`,
 * or when skipping a duplicate order in `PaymentEventsConsumer`. Read **before**
 * invoking `PaymentService.handleOrderCreated` to short-circuit duplicates.
 */
import { Column, CreateDateColumn, Entity, Index, PrimaryColumn } from 'typeorm';

/**
 * Terminal processing outcomes stored for audit and duplicate-order checks.
 *
 * - `approved` — `payment.processed` path completed for this inbound event
 * - `failed` — business rule or simulated gateway decline recorded
 * - `skipped_duplicate_order` — inbound event acknowledged but order already paid
 */
export type ProcessedEventOutcome = 'approved' | 'failed' | 'skipped_duplicate_order';

/**
 * TypeORM entity for consumer idempotency records (one row per inbound `eventId`).
 */
@Entity({ name: 'processed_events' })
export class ProcessedEventEntity {
  /**
   * Envelope `eventId` of the consumed message (e.g. `order.created`).
   * Acts as the primary idempotency key under at-least-once Kafka delivery.
   */
  @PrimaryColumn({ name: 'inbound_event_id', type: 'text' })
  inboundEventId!: string;

  /** Canonical event type string (e.g. `order.created`) for troubleshooting. */
  @Column({ name: 'event_type', type: 'text' })
  eventType!: string;

  /** Business order identifier — indexed for duplicate-order lookups. */
  @Index()
  @Column({ name: 'order_id', type: 'text' })
  orderId!: string;

  /** Payment correlation id emitted on `payment.requested` / outcomes (if any). */
  @Column({ name: 'payment_id', type: 'text', nullable: true })
  paymentId!: string | null;

  /** How this inbound event was resolved — see {@link ProcessedEventOutcome}. */
  @Column({ type: 'text' })
  outcome!: ProcessedEventOutcome;

  /** UTC timestamp when the row was inserted (processing completed or skipped). */
  @CreateDateColumn({ name: 'processed_at' })
  processedAt!: Date;
}

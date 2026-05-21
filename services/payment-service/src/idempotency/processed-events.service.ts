/**
 * @file processed-events.service.ts
 * @module payment-service — idempotency application service
 *
 * Repository facade over `processed_events` (SQLite). All deduplication for
 * `order.created` consumption should flow through this service so Kafka retry,
 * consumer rebalance, and manual replays behave consistently.
 *
 * ## Check-then-act flow (order.created)
 *
 * ```
 * PaymentEventsConsumer.handleOrderEvents
 *   → checkOrderCreated(inboundEventId, orderId)
 *       ├─ duplicate_event   → ack, no charge
 *       ├─ duplicate_order   → record skipped_duplicate_order, ack
 *       └─ new               → PaymentService (may record approved/failed after)
 * ```
 *
 * @see ProcessedEventEntity
 * @see PaymentEventsConsumer
 */
import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import {
  ProcessedEventEntity,
  type ProcessedEventOutcome,
} from './entities/processed-event.entity';

/**
 * Result of pre-flight idempotency before running payment logic.
 *
 * - `new` — safe to process (no prior row for event or approved order)
 * - `duplicate_event` — same inbound `eventId` already stored
 * - `duplicate_order` — different event but order already has `approved` outcome
 */
export type IdempotencyCheckResult =
  | { status: 'new' }
  | { status: 'duplicate_event'; record: ProcessedEventEntity }
  | { status: 'duplicate_order'; record: ProcessedEventEntity };

/**
 * Durable idempotency API backed by SQLite via TypeORM.
 */
@Injectable()
export class ProcessedEventsService {
  constructor(
    @InjectRepository(ProcessedEventEntity)
    private readonly repository: Repository<ProcessedEventEntity>,
  ) {}

  /**
   * Returns whether an inbound Kafka envelope id was already processed.
   *
   * @param inboundEventId - Envelope `eventId` from the consumed message
   */
  async isInboundEventProcessed(inboundEventId: string): Promise<boolean> {
    const count = await this.repository.count({ where: { inboundEventId } });
    return count > 0;
  }

  /**
   * Returns whether this order already has a successful (approved) payment record.
   *
   * Used to prevent a second charge when Kafka delivers a new `eventId` for
   * the same `orderId` (e.g. duplicate publish upstream).
   *
   * @param orderId - Order aggregate identifier from `order.created` payload
   */
  async hasApprovedPaymentForOrder(orderId: string): Promise<boolean> {
    const count = await this.repository.count({
      where: { orderId, outcome: 'approved' },
    });
    return count > 0;
  }

  /**
   * Combined pre-flight check invoked by `PaymentEventsConsumer` before retry/handler.
   *
   * Order of checks: inbound event id first (exactly-once semantics per message),
   * then approved payment per order (business-level dedup).
   *
   * @param inboundEventId - Consumed envelope `eventId`
   * @param orderId - Payload `orderId`
   */
  async checkOrderCreated(
    inboundEventId: string,
    orderId: string,
  ): Promise<IdempotencyCheckResult> {
    const byEvent = await this.repository.findOne({ where: { inboundEventId } });
    if (byEvent) {
      return { status: 'duplicate_event', record: byEvent };
    }

    const byOrder = await this.repository.findOne({
      where: { orderId, outcome: 'approved' },
    });
    if (byOrder) {
      return { status: 'duplicate_order', record: byOrder };
    }

    return { status: 'new' };
  }

  /**
   * Persists the final outcome after payment handling (or explicit skip).
   *
   * Should be called once per inbound `eventId` when processing reaches a
   * terminal state so subsequent redeliveries hit `duplicate_event`.
   *
   * @param params.inboundEventId - Consumed envelope id
   * @param params.eventType - e.g. `order.created`
   * @param params.orderId - Order id from payload
   * @param params.paymentId - Published payment correlation id (nullable on skip)
   * @param params.outcome - {@link ProcessedEventOutcome}
   */
  async recordProcessed(params: {
    inboundEventId: string;
    eventType: string;
    orderId: string;
    paymentId: string | null;
    outcome: ProcessedEventOutcome;
  }): Promise<ProcessedEventEntity> {
    const entity = this.repository.create({
      inboundEventId: params.inboundEventId,
      eventType: params.eventType,
      orderId: params.orderId,
      paymentId: params.paymentId,
      outcome: params.outcome,
    });

    return this.repository.save(entity);
  }
}

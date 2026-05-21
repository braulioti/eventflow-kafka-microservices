/**
 * @file payment-events.consumer.ts
 * @module payment-service — Kafka inbound handlers
 *
 * NestJS `@Controller` hosting `@EventPattern` methods — the **consumer edge**
 * of the payment saga. Validates envelopes, applies SQLite idempotency, delegates
 * to `PaymentService`, and wraps handlers with `KafkaRetryRunner`.
 *
 * ## Subscribed patterns
 *
 * | Pattern / topic      | Event(s) handled        | Action                                      |
 * |----------------------|-------------------------|---------------------------------------------|
 * | `order.events`       | `order.created` (filter)| Charge simulation + publish `payment.*`     |
 * | `order.cancelled`    | `order.cancelled`       | Log skip — no refund simulation in demo     |
 *
 * Non-`order.created` messages on `order.events` are skipped by shared parser
 * (`parseOrderEventsMessage` → `kind: 'skipped'`).
 *
 * ## Idempotency (order.created)
 *
 * Before `retryRunner.execute`:
 *
 * 1. `duplicate_event` — same inbound `eventId` already in `processed_events`
 * 2. `duplicate_order` — order already has `approved` row; records `skipped_duplicate_order`
 * 3. `new` — proceeds to payment handler inside retry wrapper
 *
 * Invalid envelopes are acked without retry (poison message avoidance).
 *
 * @see ProcessedEventsService
 * @see PaymentService
 * @see KafkaRetryRunner
 */
import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import {
  EventType,
  EventValidationError,
  OrderKafkaTopic,
  extractEnvelope,
  getRetryCount,
  parseKafkaHeaders,
  parseOrderEventsMessage,
  type OrderCancelledPayload,
} from '@eventflow/shared';
import { ProcessedEventsService } from '../idempotency/processed-events.service';
import { PaymentService } from '../payment/payment.service';
import {
  logEventReceived,
  logIdempotencySkip,
  logRetryScheduled,
} from '../payment/payment-logs';
import { KafkaRetryRunner } from './kafka-retry.runner';

/**
 * Kafka message router for payment-service — bridges topics to domain services.
 */
@Controller()
export class PaymentEventsConsumer {
  private readonly logger = new Logger(PaymentEventsConsumer.name);

  constructor(
    private readonly paymentService: PaymentService,
    private readonly processedEvents: ProcessedEventsService,
    private readonly retryRunner: KafkaRetryRunner,
  ) {}

  /**
   * Handles `order.events` — processes only validated `order.created` envelopes.
   *
   * Flow: parse → log → idempotency check → retry-wrapped `handleOrderCreated`.
   *
   * @param payload - Raw Kafka value (JSON envelope or wrapped object)
   * @param context - Nest Kafka context (headers, key, partition)
   * @returns Ack metadata for observability (does not commit offsets directly)
   */
  @EventPattern(OrderKafkaTopic.ORDER_EVENTS)
  async handleOrderEvents(
    @Payload() payload: unknown,
    @Ctx() context: KafkaContext,
  ) {
    let parsed;

    try {
      parsed = parseOrderEventsMessage(payload);
    } catch (error) {
      if (error instanceof EventValidationError) {
        this.logger.error(
          `[EVENT INVALID] topic=${OrderKafkaTopic.ORDER_EVENTS} ${error.message}`,
        );
        return { acknowledged: true, outcome: 'invalid', details: error.details };
      }
      throw error;
    }

    if (parsed.kind === 'skipped') {
      return { acknowledged: true, skipped: true, reason: parsed.reason };
    }

    const envelope = parsed.envelope;
    const headers = parseKafkaHeaders(context.getMessage().headers);
    const partitionKey = context.getMessage().key?.toString();
    const retryCount = getRetryCount(headers);

    logEventReceived(this.logger, {
      topic: OrderKafkaTopic.ORDER_EVENTS,
      eventType: envelope.eventType,
      eventId: envelope.eventId,
      orderId: envelope.payload.orderId,
      partitionKey,
      retryCount,
    });

    if (retryCount > 0) {
      logRetryScheduled(this.logger, {
        eventId: envelope.eventId,
        orderId: envelope.payload.orderId,
        attempt: retryCount + 1,
        maxAttempts: Number(headers['x-retry-max'] ?? 3),
        backoffMs: 0,
      });
    }

    const idempotency = await this.processedEvents.checkOrderCreated(
      envelope.eventId,
      envelope.payload.orderId,
    );

    if (idempotency.status === 'duplicate_event') {
      logIdempotencySkip(this.logger, {
        reason: 'duplicate_inbound_event_id',
        eventId: envelope.eventId,
        orderId: envelope.payload.orderId,
      });
      return { acknowledged: true, outcome: 'duplicate' };
    }

    if (idempotency.status === 'duplicate_order') {
      logIdempotencySkip(this.logger, {
        reason: 'duplicate_payment_for_order',
        eventId: envelope.eventId,
        orderId: envelope.payload.orderId,
      });
      await this.processedEvents.recordProcessed({
        inboundEventId: envelope.eventId,
        eventType: EventType.ORDER_CREATED,
        orderId: envelope.payload.orderId,
        paymentId: idempotency.record.paymentId,
        outcome: 'skipped_duplicate_order',
      });
      return { acknowledged: true, outcome: 'duplicate_order' };
    }

    const outcome = await this.retryRunner.execute({
      eventType: EventType.ORDER_CREATED,
      envelope,
      headers,
      handler: async () => {
        await this.paymentService.handleOrderCreated(envelope);
      },
    });

    return { acknowledged: true, outcome };
  }

  /**
   * Handles `order.cancelled` — informational skip in the demo (no charge reversal).
   *
   * Wrapped in retry executor for consistency with other consumers.
   *
   * @param payload - Envelope payload for order cancellation
   * @param context - Kafka context for headers
   */
  @EventPattern(EventType.ORDER_CANCELLED)
  async handleOrderCancelled(
    @Payload() payload: unknown,
    @Ctx() context: KafkaContext,
  ) {
    const envelope = extractEnvelope<OrderCancelledPayload>(payload);
    const headers = parseKafkaHeaders(context.getMessage().headers);

    logEventReceived(this.logger, {
      topic: EventType.ORDER_CANCELLED,
      eventType: envelope.eventType,
      eventId: envelope.eventId,
      orderId: envelope.payload.orderId,
    });

    const outcome = await this.retryRunner.execute({
      eventType: EventType.ORDER_CANCELLED,
      envelope,
      headers,
      handler: () => this.paymentService.handleOrderCancelled(envelope),
    });

    return { acknowledged: true, outcome };
  }
}

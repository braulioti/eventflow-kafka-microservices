/**
 * @file payment.service.ts
 * @module payment-service — payment saga orchestration
 *
 * Core domain service triggered by `order.created`. Models a payment processor
 * that publishes the payment leg of the EventFlow choreography and records
 * idempotent outcomes in SQLite.
 *
 * ## End-to-end flow (order.created)
 *
 * ```
 * order.created (Kafka)
 *   → PaymentEventsConsumer (idempotency pre-check)
 *   → processPayment()
 *       1. logPaymentStarted
 *       2. publish payment.requested
 *       3. evaluatePaymentRules → on fail: payment.failed + record failed
 *       4. simulatePaymentGateway → on fail: payment.failed + record failed
 *       5. publish payment.processed + record approved
 * ```
 *
 * ## Kafka events produced
 *
 * | Event               | When                                      |
 * |---------------------|-------------------------------------------|
 * | `payment.requested` | Always first — announces charge attempt   |
 * | `payment.failed`    | Business rule or simulator decline        |
 * | `payment.processed` | Simulator approved (~80% default)         |
 *
 * ## Idempotency writes
 *
 * `ProcessedEventsService.recordProcessed` is called on terminal paths so
 * redelivered `order.created` messages are skipped at the consumer edge.
 *
 * ## Payment simulation
 *
 * No external PSP — see `payment-simulator.ts` (`PAYMENT_FAILURE_RATE`, etc.).
 *
 * @see PaymentEventsConsumer
 * @see ProcessedEventsService
 * @see EventPublisher
 */
import { Injectable, Logger } from '@nestjs/common';
import { randomUUID } from 'crypto';
import {
  EventType,
  createEventEnvelope,
  type EventEnvelope,
  type OrderCancelledPayload,
  type OrderCreatedPayload,
  type PaymentRequestedPayload,
} from '@eventflow/shared';
import { ProcessedEventsService } from '../idempotency/processed-events.service';
import { EventPublisher } from '../kafka/event-publisher.service';
import { logPaymentFailed, logPaymentStarted, logPaymentSuccess } from './payment-logs';
import {
  evaluatePaymentRules,
  formatRuleViolations,
  resolvePaymentBusinessRules,
} from './payment-rules';
import {
  resolvePaymentFailureRate,
  simulatePaymentGateway,
  sleep,
} from './payment-simulator';

/**
 * Handles payment reactions to order events and publishes `payment.*` outcomes.
 */
@Injectable()
export class PaymentService {
  private readonly logger = new Logger(PaymentService.name);

  constructor(
    private readonly eventPublisher: EventPublisher,
    private readonly processedEvents: ProcessedEventsService,
  ) {}

  /**
   * Entry point from Kafka consumer for `order.created`.
   *
   * @param event - Validated envelope with order payload
   */
  async handleOrderCreated(event: EventEnvelope<OrderCreatedPayload>): Promise<void> {
    await this.processPayment(event);
  }

  /**
   * Executes the full simulated payment pipeline for one `order.created`.
   *
   * Scenario A (~80%): publishes `payment.processed`, records `approved`.
   * Scenario B (~20% or rules): publishes `payment.failed`, records `failed`.
   *
   * @param orderEvent - Inbound `order.created` envelope (idempotency key = `eventId`)
   */
  async processPayment(
    orderEvent: EventEnvelope<OrderCreatedPayload>,
  ): Promise<void> {
    const order = orderEvent.payload;
    const { orderId, totalAmount, currency } = order;
    const inboundEventId = orderEvent.eventId;

    logPaymentStarted(this.logger, {
      orderId,
      amount: totalAmount,
      currency,
      failureRate: resolvePaymentFailureRate(),
    });

    const paymentId = randomUUID();
    const requested = await this.publishPaymentRequested(orderEvent, paymentId);

    const violations = evaluatePaymentRules(order, resolvePaymentBusinessRules());
    if (violations.length > 0) {
      const reason = formatRuleViolations(violations);
      logPaymentFailed(this.logger, {
        orderId,
        paymentId,
        reason,
        failureType: 'business_rule',
        source: 'business_rule',
      });
      await this.publishPaymentFailed({
        paymentId,
        orderId,
        reason: `business_rule: ${reason}`,
        causationId: requested.eventId,
        correlationId: orderId,
      });
      await this.processedEvents.recordProcessed({
        inboundEventId,
        eventType: EventType.ORDER_CREATED,
        orderId,
        paymentId,
        outcome: 'failed',
      });
      return;
    }

    const result = simulatePaymentGateway(orderId, { paymentId });

    if (result.simulateDelayMs && result.simulateDelayMs > 0) {
      this.logger.warn(
        `[PAYMENT SIMULATION] orderId=${orderId} simulating timeout delay ${result.simulateDelayMs}ms`,
      );
      await sleep(result.simulateDelayMs);
    }

    if (result.outcome === 'failed') {
      const reason = result.failureReason ?? 'Payment gateway error';
      logPaymentFailed(this.logger, {
        orderId,
        paymentId,
        reason,
        failureType: result.failureType,
        source: 'gateway_simulation',
      });
      await this.publishPaymentFailed({
        paymentId,
        orderId,
        reason,
        causationId: requested.eventId,
        correlationId: orderId,
      });
      await this.processedEvents.recordProcessed({
        inboundEventId,
        eventType: EventType.ORDER_CREATED,
        orderId,
        paymentId,
        outcome: 'failed',
      });
      return;
    }

    const transactionId = randomUUID();
    await this.publishPaymentProcessed({
      paymentId,
      orderId,
      transactionId,
      causationId: requested.eventId,
      correlationId: orderId,
    });

    logPaymentSuccess(this.logger, {
      orderId,
      paymentId,
      transactionId,
      amount: totalAmount,
      currency,
    });

    await this.processedEvents.recordProcessed({
      inboundEventId,
      eventType: EventType.ORDER_CREATED,
      orderId,
      paymentId,
      outcome: 'approved',
    });
  }

  /**
   * Reacts to `order.cancelled` — logs only (no refund/chargeback simulation).
   *
   * @param event - Cancellation envelope with `orderId` and `reason`
   */
  async handleOrderCancelled(event: EventEnvelope<OrderCancelledPayload>): Promise<void> {
    this.logger.log(
      `[PAYMENT SKIPPED] orderId=${event.payload.orderId} reason=${event.payload.reason}`,
    );
  }

  /**
   * Publishes `payment.requested` — signals charge attempt to downstream observers.
   *
   * @param orderCreated - Source `order.created` envelope
   * @param paymentId - New payment correlation uuid
   * @returns Published envelope (for causation chain)
   */
  private async publishPaymentRequested(
    orderCreated: EventEnvelope<OrderCreatedPayload>,
    paymentId: string,
  ): Promise<EventEnvelope<PaymentRequestedPayload>> {
    const { orderId, totalAmount, currency } = orderCreated.payload;

    const requested = createEventEnvelope({
      eventType: EventType.PAYMENT_REQUESTED,
      source: 'payment-service',
      correlationId: orderId,
      causationId: orderCreated.eventId,
      payload: { paymentId, orderId, amount: totalAmount, currency },
    });

    await this.eventPublisher.publish(EventType.PAYMENT_REQUESTED, requested);
    this.logger.log(
      `[KAFKA PUBLISH] event=${EventType.PAYMENT_REQUESTED} orderId=${orderId} paymentId=${paymentId}`,
    );

    return requested;
  }

  /**
   * Publishes `payment.processed` on `payment.events` (happy path).
   */
  private async publishPaymentProcessed(params: {
    paymentId: string;
    orderId: string;
    transactionId: string;
    causationId: string;
    correlationId: string;
  }): Promise<void> {
    const processed = createEventEnvelope({
      eventType: EventType.PAYMENT_PROCESSED,
      source: 'payment-service',
      correlationId: params.correlationId,
      causationId: params.causationId,
      payload: {
        paymentId: params.paymentId,
        orderId: params.orderId,
        transactionId: params.transactionId,
        processedAt: new Date().toISOString(),
      },
    });

    await this.eventPublisher.publish(EventType.PAYMENT_PROCESSED, processed);
    this.logger.log(
      `[KAFKA PUBLISH] topic=payment.events event=${EventType.PAYMENT_PROCESSED} orderId=${params.orderId} paymentId=${params.paymentId}`,
    );
  }

  /**
   * Publishes `payment.failed` on `payment.events` (compensation trigger for order-service).
   */
  private async publishPaymentFailed(params: {
    paymentId: string;
    orderId: string;
    reason: string;
    causationId: string;
    correlationId: string;
  }): Promise<void> {
    const failed = createEventEnvelope({
      eventType: EventType.PAYMENT_FAILED,
      source: 'payment-service',
      correlationId: params.correlationId,
      causationId: params.causationId,
      payload: {
        paymentId: params.paymentId,
        orderId: params.orderId,
        reason: params.reason,
        failedAt: new Date().toISOString(),
      },
    });

    await this.eventPublisher.publish(EventType.PAYMENT_FAILED, failed);
    this.logger.log(
      `[KAFKA PUBLISH] topic=payment.events event=${EventType.PAYMENT_FAILED} orderId=${params.orderId} paymentId=${params.paymentId}`,
    );
  }
}

/**
 * Stock Service — Kafka Event Consumer
 *
 * NestJS `@Controller` registered as a microservice consumer via `AppModule`.
 * Subscribes to upstream saga topics, validates payloads where multi-event
 * topics are used, and delegates business logic to {@link StockService} inside
 * {@link KafkaRetryRunner} for resilient processing.
 *
 * ## Subscriptions
 *
 * | Pattern / Topic              | Event handled        | Domain handler                    |
 * |------------------------------|----------------------|-----------------------------------|
 * | `PaymentKafkaTopic.PAYMENT_EVENTS` | `payment.processed` (filtered) | `handlePaymentProcessed` |
 * | `EventType.ORDER_CANCELLED`  | `order.cancelled`    | `handleOrderCancelled`            |
 *
 * Invalid messages on `payment.events` are logged and acknowledged without
 * retry to avoid poison-looping on permanently bad payloads.
 *
 * @module stock-service/kafka/stock-events.consumer
 */
import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import {
  EventType,
  EventValidationError,
  PaymentKafkaTopic,
  extractEnvelope,
  parseKafkaHeaders,
  parsePaymentProcessedMessage,
  type OrderCancelledPayload,
} from '@eventflow/shared';
import { StockService } from '../stock/stock.service';
import { KafkaRetryRunner } from './kafka-retry.runner';

/**
 * Kafka consumer controller for payment and order cancellation events.
 */
@Controller()
export class StockEventsConsumer {
  private readonly logger = new Logger(StockEventsConsumer.name);

  /**
   * @param stockService - Domain logic for reserve/release workflows.
   * @param retryRunner  - Shared retry and DLQ executor wrapper.
   */
  constructor(
    private readonly stockService: StockService,
    private readonly retryRunner: KafkaRetryRunner,
  ) {}

  /**
   * Handles `payment.events`, processing only `payment.processed` messages.
   *
   * Other event types on the topic are skipped. Validation errors return
   * `outcome: 'invalid'` without rethrowing to prevent infinite redelivery.
   *
   * @param payload - Raw Kafka message value (JSON envelope or wrapped object).
   * @param context - Nest Kafka context for headers and metadata.
   */
  @EventPattern(PaymentKafkaTopic.PAYMENT_EVENTS)
  async handlePaymentEvents(
    @Payload() payload: unknown,
    @Ctx() context: KafkaContext,
  ) {
    let parsed;

    try {
      parsed = parsePaymentProcessedMessage(payload);
    } catch (error) {
      if (error instanceof EventValidationError) {
        this.logger.error(
          `Invalid message on ${PaymentKafkaTopic.PAYMENT_EVENTS}: ${error.message}`,
        );
        return { acknowledged: true, outcome: 'invalid' };
      }
      throw error;
    }

    if (parsed.kind === 'skipped') {
      return { acknowledged: true, skipped: true, eventType: parsed.eventType };
    }

    const envelope = parsed.envelope;
    const headers = parseKafkaHeaders(context.getMessage().headers);

    const outcome = await this.retryRunner.execute({
      eventType: EventType.PAYMENT_PROCESSED,
      envelope,
      headers,
      handler: async () => {
        this.logger.log(
          `Processing ${EventType.PAYMENT_PROCESSED} from ${PaymentKafkaTopic.PAYMENT_EVENTS} orderId=${envelope.payload.orderId}`,
        );
        await this.stockService.handlePaymentProcessed(envelope);
      },
    });

    return { acknowledged: true, outcome };
  }

  /**
   * Handles `order.cancelled` and triggers stock release via {@link StockService}.
   *
   * @param payload - Kafka message value containing the order cancellation envelope.
   * @param context - Nest Kafka context for retry header extraction.
   */
  @EventPattern(EventType.ORDER_CANCELLED)
  async handleOrderCancelled(
    @Payload() payload: unknown,
    @Ctx() context: KafkaContext,
  ) {
    const envelope = extractEnvelope<OrderCancelledPayload>(payload);
    const headers = parseKafkaHeaders(context.getMessage().headers);

    const outcome = await this.retryRunner.execute({
      eventType: EventType.ORDER_CANCELLED,
      envelope,
      headers,
      handler: () => this.stockService.handleOrderCancelled(envelope),
    });

    return { acknowledged: true, outcome };
  }
}

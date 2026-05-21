/**
 * Notification Service — Kafka Event Consumer
 *
 * Subscribes to stock, payment, and order topics that drive customer notification
 * workflows. Handlers validate multi-event topics where needed, wrap domain logic
 * in {@link KafkaRetryRunner}, and return acknowledgment metadata to Nest Kafka.
 *
 * ## Subscriptions
 *
 * | Pattern / Topic                    | Event handled           | Domain handler           |
 * |------------------------------------|-------------------------|--------------------------|
 * | `EventType.STOCK_RESERVED`         | `stock.reserved`        | `handleStockReserved`    |
 * | `PaymentKafkaTopic.PAYMENT_EVENTS` | `payment.failed` (filter)| `handlePaymentFailed`   |
 * | `EventType.ORDER_CANCELLED`        | `order.cancelled`       | `handleOrderCancelled`   |
 *
 * @module notification-service/kafka/notification-events.consumer
 */
import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import {
  EventType,
  EventValidationError,
  PaymentKafkaTopic,
  extractEnvelope,
  parseKafkaHeaders,
  parsePaymentFailedMessage,
  type OrderCancelledPayload,
  type StockReservedPayload,
} from '@eventflow/shared';
import { NotificationService } from '../notification/notification.service';
import { KafkaRetryRunner } from './kafka-retry.runner';

/**
 * Kafka `@Controller` bridging topics to {@link NotificationService}.
 */
@Controller()
export class NotificationEventsConsumer {
  private readonly logger = new Logger(NotificationEventsConsumer.name);

  /**
   * @param notificationService - Notification domain workflows.
   * @param retryRunner         - Shared retry/DLQ executor.
   */
  constructor(
    private readonly notificationService: NotificationService,
    private readonly retryRunner: KafkaRetryRunner,
  ) {}

  /**
   * Processes `stock.reserved` and triggers send/sent notification events.
   *
   * @param payload - Raw Kafka message value.
   * @param context - Kafka context for header parsing (retry counts).
   */
  @EventPattern(EventType.STOCK_RESERVED)
  async handleStockReserved(
    @Payload() payload: unknown,
    @Ctx() context: KafkaContext,
  ) {
    const envelope = extractEnvelope<StockReservedPayload>(payload);
    const headers = parseKafkaHeaders(context.getMessage().headers);

    const outcome = await this.retryRunner.execute({
      eventType: EventType.STOCK_RESERVED,
      envelope,
      headers,
      handler: async () => {
        this.logger.log(
          `Processing ${EventType.STOCK_RESERVED} for ${envelope.payload.orderId}`,
        );
        await this.notificationService.handleStockReserved(envelope);
      },
    });

    return { acknowledged: true, outcome };
  }

  /**
   * Processes `payment.events`, handling only `payment.failed` payloads.
   *
   * Skips unrelated event types. Invalid envelopes are acknowledged as
   * `invalid` without retry to avoid poison-message loops.
   *
   * @param payload - Raw Kafka message value from `payment.events`.
   * @param context - Kafka context for retry header extraction.
   */
  @EventPattern(PaymentKafkaTopic.PAYMENT_EVENTS)
  async handlePaymentEvents(
    @Payload() payload: unknown,
    @Ctx() context: KafkaContext,
  ) {
    let parsed;

    try {
      parsed = parsePaymentFailedMessage(payload);
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
      eventType: EventType.PAYMENT_FAILED,
      envelope,
      headers,
      handler: () => this.notificationService.handlePaymentFailed(envelope),
    });

    return { acknowledged: true, outcome };
  }

  /**
   * Processes `order.cancelled` for cancellation notification logging (demo).
   *
   * @param payload - Cancellation envelope from Kafka.
   * @param context - Kafka context supplying message headers.
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
      handler: () => this.notificationService.handleOrderCancelled(envelope),
    });

    return { acknowledged: true, outcome };
  }
}

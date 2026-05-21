/**
 * @file order-events.consumer.ts
 * @module order-service — Kafka inbound handlers
 *
 * Updates persisted `OrderStatus` in SQLite when downstream saga steps complete
 * or fail. Does **not** publish new domain events — order-service is mostly a
 * status projector after the initial `order.created`.
 *
 * ## Handled events
 *
 * | Pattern              | Event type           | New status   | Saga meaning              |
 * |----------------------|----------------------|--------------|---------------------------|
 * | `payment.events`     | `payment.failed`     | `failed`     | Compensation — pay failed |
 * | `stock.released`     | `stock.released`     | `cancelled`  | Inventory rolled back     |
 * | `stock.failed`       | `stock.failed`       | `failed`     | Reservation error       |
 * | `notification.sent`  | `notification.sent`  | `completed`  | Happy path terminal       |
 *
 * Each handler uses `KafkaRetryRunner` for transient DB errors. Invalid
 * `payment.events` payloads are acked without retry.
 *
 * @see OrdersService.updateOrderStatus
 * @see KafkaRetryRunner
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
  type NotificationSentPayload,
  type StockFailedPayload,
  type StockReleasedPayload,
} from '@eventflow/shared';
import { OrderStatus } from '../orders/entities/order-status.enum';
import { OrdersService } from '../orders/orders.service';
import { KafkaRetryRunner } from './kafka-retry.runner';

/**
 * Kafka router updating order aggregate status from saga feedback events.
 */
@Controller()
export class OrderEventsConsumer {
  private readonly logger = new Logger(OrderEventsConsumer.name);

  constructor(
    private readonly retryRunner: KafkaRetryRunner,
    private readonly ordersService: OrdersService,
  ) {}

  /**
   * Listens on `payment.events` — processes validated `payment.failed` only.
   *
   * Other payment event types are skipped by `parsePaymentFailedMessage`.
   * Marks order `failed` so API/clients reflect compensation.
   *
   * @param payload - Raw Kafka message body
   * @param context - Headers and metadata for retry counting
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
      handler: async () => {
        await this.ordersService.updateOrderStatus(
          envelope.payload.orderId,
          OrderStatus.FAILED,
        );
        this.logger.warn(
          `Payment failed for order ${envelope.payload.orderId}: ${envelope.payload.reason}`,
        );
      },
    });

    return { acknowledged: true, outcome };
  }

  /**
   * Handles `stock.released` — order cancelled after inventory compensation.
   */
  @EventPattern(EventType.STOCK_RELEASED)
  async handleStockReleased(
    @Payload() payload: unknown,
    @Ctx() context: KafkaContext,
  ) {
    const envelope = extractEnvelope<StockReleasedPayload>(payload);
    const headers = parseKafkaHeaders(context.getMessage().headers);

    const outcome = await this.retryRunner.execute({
      eventType: EventType.STOCK_RELEASED,
      envelope,
      headers,
      handler: async () => {
        await this.ordersService.updateOrderStatus(
          envelope.payload.orderId,
          OrderStatus.CANCELLED,
        );
        this.logger.log(
          `Stock released for order ${envelope.payload.orderId}: ${envelope.payload.reason}`,
        );
      },
    });

    return { acknowledged: true, outcome };
  }

  /**
   * Handles `stock.failed` — marks order failed when reservation cannot complete.
   */
  @EventPattern(EventType.STOCK_FAILED)
  async handleStockFailed(
    @Payload() payload: unknown,
    @Ctx() context: KafkaContext,
  ) {
    const envelope = extractEnvelope<StockFailedPayload>(payload);
    const headers = parseKafkaHeaders(context.getMessage().headers);

    const outcome = await this.retryRunner.execute({
      eventType: EventType.STOCK_FAILED,
      envelope,
      headers,
      handler: async () => {
        await this.ordersService.updateOrderStatus(
          envelope.payload.orderId,
          OrderStatus.FAILED,
        );
        this.logger.warn(
          `Stock failed for order ${envelope.payload.orderId}: ${envelope.payload.reason}`,
        );
      },
    });

    return { acknowledged: true, outcome };
  }

  /**
   * Handles `notification.sent` — terminal happy-path status `completed`.
   */
  @EventPattern(EventType.NOTIFICATION_SENT)
  async handleNotificationSent(
    @Payload() payload: unknown,
    @Ctx() context: KafkaContext,
  ) {
    const envelope = extractEnvelope<NotificationSentPayload>(payload);
    const headers = parseKafkaHeaders(context.getMessage().headers);

    const outcome = await this.retryRunner.execute({
      eventType: EventType.NOTIFICATION_SENT,
      envelope,
      headers,
      handler: async () => {
        await this.ordersService.updateOrderStatus(
          envelope.payload.orderId,
          OrderStatus.COMPLETED,
        );
        this.logger.log(
          `Order flow completed for ${envelope.payload.orderId} — notification sent via ${envelope.payload.channel}`,
        );
      },
    });

    return { acknowledged: true, outcome, status: 'completed' };
  }
}

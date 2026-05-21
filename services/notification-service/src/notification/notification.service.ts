/**
 * Notification Service — Customer Notification Domain Logic
 *
 * Implements the happy-path notification saga step after stock reservation and
 * provides demo handlers for failure and cancellation signals (logging only).
 *
 * ## Event reactions
 *
 * | Incoming event      | Outgoing event(s)              | Behavior                          |
 * |---------------------|--------------------------------|-----------------------------------|
 * | `stock.reserved`    | `notification.send`, `notification.sent` | Two-step send confirmation |
 * | `payment.failed`    | (none in demo)                 | Warn log only                     |
 * | `order.cancelled`   | (none in demo)                 | Info log only                     |
 *
 * Production extensions would publish `notification.failed` on errors and
 * integrate real email/SMS providers instead of hard-coded recipient data.
 *
 * @module notification-service/notification/notification.service
 */
import { Injectable, Logger } from '@nestjs/common';
import { randomUUID } from 'crypto';
import {
  EventType,
  createEventEnvelope,
  type OrderCancelledPayload,
  type PaymentFailedPayload,
  type StockReservedPayload,
} from '@eventflow/shared';
import { EventPublisher } from '../kafka/event-publisher.service';

/**
 * Domain service orchestrating notification send/sent publication and side-effect logs.
 */
@Injectable()
export class NotificationService {
  private readonly logger = new Logger(NotificationService.name);

  /**
   * @param eventPublisher - Kafka transport for notification domain events.
   */
  constructor(private readonly eventPublisher: EventPublisher) {}

  /**
   * Publishes `notification.send` then `notification.sent` after stock reservation.
   *
   * Uses a generated `notificationId`, fixed demo recipient, and `email` channel.
   * Causation chains: `stock.reserved` → send → sent.
   *
   * @param event - Validated `stock.reserved` envelope from stock-service.
   */
  async handleStockReserved(
    event: ReturnType<typeof createEventEnvelope<StockReservedPayload>>,
  ) {
    const { orderId } = event.payload;
    const notificationId = randomUUID();
    const channel = 'email' as const;

    const send = createEventEnvelope({
      eventType: EventType.NOTIFICATION_SEND,
      source: 'notification-service',
      correlationId: orderId,
      causationId: event.eventId,
      payload: {
        notificationId,
        orderId,
        channel,
        recipient: 'customer@example.com',
        template: 'order-completed',
      },
    });
    await this.eventPublisher.publish(EventType.NOTIFICATION_SEND, send);

    const sent = createEventEnvelope({
      eventType: EventType.NOTIFICATION_SENT,
      source: 'notification-service',
      correlationId: orderId,
      causationId: send.eventId,
      payload: {
        notificationId,
        orderId,
        channel,
        sentAt: new Date().toISOString(),
      },
    });
    await this.eventPublisher.publish(EventType.NOTIFICATION_SENT, sent);
    this.logger.log(`Notification sent for order ${orderId}`);
  }

  /**
   * Logs a payment-failure notification path (demo — does not publish events).
   *
   * @param event - `payment.failed` envelope from payment-service.
   */
  async handlePaymentFailed(
    event: ReturnType<typeof createEventEnvelope<PaymentFailedPayload>>,
  ) {
    this.logger.warn(
      `Payment failure notification for order ${event.payload.orderId}`,
    );
  }

  /**
   * Logs an order-cancellation notification path (demo — does not publish events).
   *
   * @param event - `order.cancelled` envelope with cancellation metadata.
   */
  async handleOrderCancelled(
    event: ReturnType<typeof createEventEnvelope<OrderCancelledPayload>>,
  ) {
    this.logger.log(
      `Cancellation notification for order ${event.payload.orderId}`,
    );
  }
}

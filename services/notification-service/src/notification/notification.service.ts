/**
 * Notification workflow: stock.reserved triggers notification.send then notification.sent.
 * Failure/cancel paths log only in this demo (extend to publish notification.failed).
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

@Injectable()
export class NotificationService {
  private readonly logger = new Logger(NotificationService.name);

  constructor(private readonly eventPublisher: EventPublisher) {}

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

  async handlePaymentFailed(
    event: ReturnType<typeof createEventEnvelope<PaymentFailedPayload>>,
  ) {
    this.logger.warn(
      `Payment failure notification for order ${event.payload.orderId}`,
    );
  }

  async handleOrderCancelled(
    event: ReturnType<typeof createEventEnvelope<OrderCancelledPayload>>,
  ) {
    this.logger.log(
      `Cancellation notification for order ${event.payload.orderId}`,
    );
  }
}

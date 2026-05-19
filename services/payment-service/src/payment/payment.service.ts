/**
 * Simulated payment processor: on order.created emits payment.requested then payment.processed.
 * Uses causationId to chain envelopes for traceability.
 */
import { Injectable, Logger } from '@nestjs/common';
import { randomUUID } from 'crypto';
import {
  EventType,
  createEventEnvelope,
  type OrderCancelledPayload,
  type OrderCreatedPayload,
} from '@eventflow/shared';
import { EventPublisher } from '../kafka/event-publisher.service';

@Injectable()
export class PaymentService {
  private readonly logger = new Logger(PaymentService.name);

  constructor(private readonly eventPublisher: EventPublisher) {}

  async handleOrderCreated(
    event: ReturnType<typeof createEventEnvelope<OrderCreatedPayload>>,
  ) {
    const { orderId, totalAmount, currency } = event.payload;
    const paymentId = randomUUID();

    const requested = createEventEnvelope({
      eventType: EventType.PAYMENT_REQUESTED,
      source: 'payment-service',
      correlationId: orderId,
      causationId: event.eventId,
      payload: { paymentId, orderId, amount: totalAmount, currency },
    });
    await this.eventPublisher.publish(EventType.PAYMENT_REQUESTED, requested);

    const processed = createEventEnvelope({
      eventType: EventType.PAYMENT_PROCESSED,
      source: 'payment-service',
      correlationId: orderId,
      causationId: requested.eventId,
      payload: {
        paymentId,
        orderId,
        transactionId: randomUUID(),
        processedAt: new Date().toISOString(),
      },
    });
    await this.eventPublisher.publish(EventType.PAYMENT_PROCESSED, processed);
    this.logger.log(`Payment processed for order ${orderId}`);
  }

  async handleOrderCancelled(
    event: ReturnType<typeof createEventEnvelope<OrderCancelledPayload>>,
  ) {
    this.logger.log(
      `Order cancelled — skipping payment for ${event.payload.orderId}`,
    );
  }
}

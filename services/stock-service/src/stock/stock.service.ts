/**
 * Inventory simulation: payment.processed → stock.reserved; order.cancelled → stock.released.
 */
import { Injectable, Logger } from '@nestjs/common';
import { randomUUID } from 'crypto';
import {
  EventType,
  createEventEnvelope,
  type OrderCancelledPayload,
  type PaymentProcessedPayload,
} from '@eventflow/shared';
import { EventPublisher } from '../kafka/event-publisher.service';

@Injectable()
export class StockService {
  private readonly logger = new Logger(StockService.name);

  constructor(private readonly eventPublisher: EventPublisher) {}

  async handlePaymentProcessed(
    event: ReturnType<typeof createEventEnvelope<PaymentProcessedPayload>>,
  ) {
    const { orderId } = event.payload;
    const reservationId = randomUUID();

    const reserved = createEventEnvelope({
      eventType: EventType.STOCK_RESERVED,
      source: 'stock-service',
      correlationId: orderId,
      causationId: event.eventId,
      payload: {
        reservationId,
        orderId,
        items: [],
        reservedAt: new Date().toISOString(),
      },
    });

    await this.eventPublisher.publish(EventType.STOCK_RESERVED, reserved);
    this.logger.log(`Stock reserved for order ${orderId}`);
  }

  async handleOrderCancelled(
    event: ReturnType<typeof createEventEnvelope<OrderCancelledPayload>>,
  ) {
    const { orderId } = event.payload;
    const released = createEventEnvelope({
      eventType: EventType.STOCK_RELEASED,
      source: 'stock-service',
      correlationId: orderId,
      causationId: event.eventId,
      payload: {
        reservationId: randomUUID(),
        orderId,
        reason: event.payload.reason,
        releasedAt: new Date().toISOString(),
      },
    });

    await this.eventPublisher.publish(EventType.STOCK_RELEASED, released);
    this.logger.log(`Stock released for cancelled order ${orderId}`);
  }
}

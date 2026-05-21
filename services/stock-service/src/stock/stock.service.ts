/**
 * Stock Service — Inventory Domain Logic
 *
 * Simulates stock reservation and release in response to upstream saga events.
 * This is a demonstration implementation: no persistent inventory store is
 * used; each handler builds canonical envelopes and publishes downstream events.
 *
 * ## Event reactions
 *
 * | Incoming event        | Outgoing event(s)   | Behavior                              |
 * |-----------------------|---------------------|---------------------------------------|
 * | `payment.processed`   | `stock.reserved`    | Generate reservation id, empty items  |
 * | `order.cancelled`     | `stock.released`    | Release with cancellation reason      |
 *
 * Correlation ids follow the order id; causation ids chain to the triggering
 * event id for traceability across the pipeline.
 *
 * @module stock-service/stock/stock.service
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

/**
 * Domain service translating payment and cancellation events into stock events.
 */
@Injectable()
export class StockService {
  private readonly logger = new Logger(StockService.name);

  /**
   * @param eventPublisher - Kafka transport for publishing stock domain events.
   */
  constructor(private readonly eventPublisher: EventPublisher) {}

  /**
   * Reserves inventory after successful payment and publishes `stock.reserved`.
   *
   * @param event - Validated `payment.processed` envelope from payment-service.
   */
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

  /**
   * Releases inventory when an order is cancelled and publishes `stock.released`.
   *
   * @param event - `order.cancelled` envelope including cancellation reason.
   */
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

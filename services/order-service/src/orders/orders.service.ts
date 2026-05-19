/**
 * Order aggregate: assigns orderId, builds envelope, publishes order.created.
 * correlationId = orderId so all downstream events stay in one trace.
 */
import { Injectable, Logger } from '@nestjs/common';
import { randomUUID } from 'crypto';
import {
  EventType,
  createEventEnvelope,
  type OrderCreatedPayload,
} from '@eventflow/shared';
import { EventPublisher } from '../kafka/event-publisher.service';
import { CreateOrderDto } from './dto/create-order.dto';

@Injectable()
export class OrdersService {
  private readonly logger = new Logger(OrdersService.name);

  constructor(private readonly eventPublisher: EventPublisher) {}

  async createOrder(dto: CreateOrderDto) {
    const orderId = randomUUID();
    const totalAmount = dto.items.reduce(
      (sum, item) => sum + item.quantity * item.unitPrice,
      0,
    );

    const payload: OrderCreatedPayload = {
      orderId,
      customerId: dto.customerId,
      items: dto.items,
      totalAmount,
      currency: 'BRL',
    };

    const envelope = createEventEnvelope({
      eventType: EventType.ORDER_CREATED,
      source: 'order-service',
      correlationId: orderId,
      payload,
    });

    await this.eventPublisher.publish(EventType.ORDER_CREATED, envelope);
    this.logger.log(`Published ${EventType.ORDER_CREATED} for order ${orderId}`);

    return { orderId, status: 'pending', eventId: envelope.eventId };
  }
}

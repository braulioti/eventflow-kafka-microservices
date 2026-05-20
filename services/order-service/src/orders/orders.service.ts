import {
  Injectable,
  InternalServerErrorException,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { randomUUID } from 'crypto';
import { Repository } from 'typeorm';
import {
  EventType,
  IdempotencyStrategy,
  OrderKafkaTopic,
  createEventEnvelope,
  hasPublishedEventId,
  type CurrencyCode,
  type OrderCreatedPayload,
} from '@eventflow/shared';
import { EventPublisher } from '../kafka/event-publisher.service';
import { CreateOrderDto } from './dto/create-order.dto';
import { OrderEntity } from './entities/order.entity';
import { OrderStatus } from './entities/order-status.enum';

@Injectable()
export class OrdersService {
  private readonly logger = new Logger(OrdersService.name);

  constructor(
    @InjectRepository(OrderEntity)
    private readonly ordersRepository: Repository<OrderEntity>,
    private readonly eventPublisher: EventPublisher,
  ) {}

  /**
   * Persists the order, builds an {@link EventEnvelope}, publishes to `order.events`.
   *
   * Idempotency ({@link IdempotencyStrategy.EVENT_ID_PER_AGGREGATE}):
   * - `eventId` generated once per order
   * - stored before publish so retries reuse the same id
   * - skips publish when `eventId` is already set
   * - Kafka message key = `orderId` (partition ordering)
   */
  async createOrder(dto: CreateOrderDto) {
    const orderId = randomUUID();
    const currency = (dto.currency ?? 'BRL') as CurrencyCode;
    const totalAmount = dto.items.reduce(
      (sum, item) => sum + item.quantity * item.unitPrice,
      0,
    );

    const order = this.ordersRepository.create({
      id: orderId,
      customerId: dto.customerId,
      status: OrderStatus.PENDING,
      totalAmount,
      currency,
      eventId: null,
      items: dto.items.map((item) => ({
        productId: item.productId,
        quantity: item.quantity,
        unitPrice: item.unitPrice,
      })),
    });

    await this.ordersRepository.save(order);

    this.logger.log(
      `Order created: orderId=${orderId} customerId=${dto.customerId} totalAmount=${totalAmount} ${currency} status=${OrderStatus.PENDING}`,
    );

    if (hasPublishedEventId(order.eventId)) {
      this.logger.log(
        `Skipping duplicate ${EventType.ORDER_CREATED} for order ${orderId}`,
      );
      return this.toCreateOrderResponse(order);
    }

    const payload: OrderCreatedPayload = {
      orderId,
      customerId: dto.customerId,
      items: dto.items,
      totalAmount,
      currency,
    };

    const envelope = createEventEnvelope({
      eventType: EventType.ORDER_CREATED,
      source: 'order-service',
      correlationId: orderId,
      payload,
    });

    order.eventId = envelope.eventId;
    await this.ordersRepository.save(order);

    this.logger.log(
      `Publishing ${EventType.ORDER_CREATED} to ${OrderKafkaTopic.ORDER_EVENTS}: eventId=${envelope.eventId} correlationId=${orderId}`,
    );

    try {
      await this.eventPublisher.publish(EventType.ORDER_CREATED, envelope);
    } catch (error) {
      order.status = OrderStatus.FAILED;
      await this.ordersRepository.save(order);
      this.logger.error(
        `Order ${orderId} marked failed: could not publish ${EventType.ORDER_CREATED} (eventId=${envelope.eventId})`,
        error instanceof Error ? error.stack : String(error),
      );
      throw new InternalServerErrorException(
        'Order saved but event could not be published',
      );
    }

    this.logger.log(
      `Order ${orderId} pipeline started: ${EventType.ORDER_CREATED} published (eventId=${envelope.eventId}, kafkaKey=${orderId})`,
    );

    return this.toCreateOrderResponse(order, envelope.eventId);
  }

  /** Updates persisted status when downstream saga completes or fails. */
  async updateOrderStatus(orderId: string, status: OrderStatus): Promise<void> {
    const result = await this.ordersRepository.update({ id: orderId }, { status });
    if (result.affected === 0) {
      throw new NotFoundException(`Order ${orderId} not found`);
    }
  }

  private toCreateOrderResponse(order: OrderEntity, eventId?: string) {
    return {
      orderId: order.id,
      status: order.status,
      totalAmount: order.totalAmount,
      currency: order.currency,
      eventId: eventId ?? order.eventId,
      correlationId: order.id,
      createdAt: order.createdAt,
    };
  }
}

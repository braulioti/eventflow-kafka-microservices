/**
 * @file orders.service.ts
 * @module order-service — order domain + saga initiation
 *
 * Owns the order aggregate in SQLite and coordinates the **first Kafka event**
 * in the choreography. Also applies status transitions when feedback events arrive
 * from payment, stock, and notification services.
 *
 * ## createOrder flow (HTTP → Kafka)
 *
 * ```
 * POST /orders
 *   → compute totalAmount, persist OrderEntity (status=pending, eventId=null)
 *   → assign envelope.eventId, save (producer idempotency)
 *   → EventPublisher.publish(order.created) key=orderId
 *   → on publish failure: status=failed, 500 to client
 * ```
 *
 * ## Producer idempotency (SQLite + shared strategy)
 *
 * Uses {@link IdempotencyStrategy.EVENT_ID_PER_AGGREGATE}:
 *
 * - One `eventId` per order, generated before publish
 * - Stored on the row before emitting so retries reuse the same id
 * - `hasPublishedEventId` skips duplicate publish if HTTP client retries
 *
 * ## Consumer-driven status updates
 *
 * `updateOrderStatus` is called from `OrderEventsConsumer` when saga branches
 * complete (`completed`, `failed`, `cancelled`).
 *
 * @see OrderEntity
 * @see EventPublisher
 * @see OrderEventsConsumer
 */
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

/**
 * Application service for order persistence and Kafka saga orchestration.
 */
@Injectable()
export class OrdersService {
  private readonly logger = new Logger(OrdersService.name);

  constructor(
    @InjectRepository(OrderEntity)
    private readonly ordersRepository: Repository<OrderEntity>,
    private readonly eventPublisher: EventPublisher,
  ) {}

  /**
   * Creates order in SQLite and publishes `order.created` to `order.events`.
   *
   * Idempotency ({@link IdempotencyStrategy.EVENT_ID_PER_AGGREGATE}):
   * - `eventId` generated once per order
   * - Stored before publish so retries reuse the same id
   * - Skips publish when `eventId` is already set on the row
   * - Kafka message key = `orderId` for partition ordering
   *
   * @param dto - Validated REST body
   * @returns API response with orderId, status, amounts, eventId, correlationId
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

  /**
   * Updates order status when Kafka feedback events are processed.
   *
   * @param orderId - Aggregate id from event payload
   * @param status - Target {@link OrderStatus}
   * @throws NotFoundException when no row matches orderId
   */
  async updateOrderStatus(orderId: string, status: OrderStatus): Promise<void> {
    const result = await this.ordersRepository.update({ id: orderId }, { status });
    if (result.affected === 0) {
      throw new NotFoundException(`Order ${orderId} not found`);
    }
  }

  /**
   * Maps entity to HTTP response shape for create order endpoint.
   *
   * @param order - Persisted aggregate
   * @param eventId - Optional override when just published
   */
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

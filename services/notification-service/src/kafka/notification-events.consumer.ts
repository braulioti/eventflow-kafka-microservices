/**
 * Listens for stock.reserved, payment.failed, and order.cancelled.
 */
import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import {
  EventType,
  extractEnvelope,
  parseKafkaHeaders,
  type OrderCancelledPayload,
  type PaymentFailedPayload,
  type StockReservedPayload,
} from '@eventflow/shared';
import { NotificationService } from '../notification/notification.service';
import { KafkaRetryRunner } from './kafka-retry.runner';

@Controller()
export class NotificationEventsConsumer {
  private readonly logger = new Logger(NotificationEventsConsumer.name);

  constructor(
    private readonly notificationService: NotificationService,
    private readonly retryRunner: KafkaRetryRunner,
  ) {}

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

  @EventPattern(EventType.PAYMENT_FAILED)
  async handlePaymentFailed(
    @Payload() payload: unknown,
    @Ctx() context: KafkaContext,
  ) {
    const envelope = extractEnvelope<PaymentFailedPayload>(payload);
    const headers = parseKafkaHeaders(context.getMessage().headers);

    const outcome = await this.retryRunner.execute({
      eventType: EventType.PAYMENT_FAILED,
      envelope,
      headers,
      handler: () => this.notificationService.handlePaymentFailed(envelope),
    });

    return { acknowledged: true, outcome };
  }

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

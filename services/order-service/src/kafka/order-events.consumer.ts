import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import {
  EventType,
  extractEnvelope,
  parseKafkaHeaders,
  type NotificationSentPayload,
  type PaymentFailedPayload,
  type StockFailedPayload,
  type StockReleasedPayload,
} from '@eventflow/shared';
import { KafkaRetryRunner } from './kafka-retry.runner';

@Controller()
export class OrderEventsConsumer {
  private readonly logger = new Logger(OrderEventsConsumer.name);

  constructor(private readonly retryRunner: KafkaRetryRunner) {}

  /** Compensation: payment could not complete for this order. */
  @EventPattern(EventType.PAYMENT_FAILED)
  async handlePaymentFailed(
    @Payload() payload: unknown,
    @Ctx() context: KafkaContext,
  ) {
    const envelope = extractEnvelope<PaymentFailedPayload>(payload);
    const headers = parseKafkaHeaders(context.getMessage().headers);

    const outcome = await this.retryRunner.execute({
      topic: EventType.PAYMENT_FAILED,
      envelope,
      headers,
      handler: async () => {
        this.logger.warn(
          `Payment failed for order ${envelope.payload.orderId}: ${envelope.payload.reason}`,
        );
      },
    });

    return { acknowledged: true, outcome };
  }

  @EventPattern(EventType.STOCK_RELEASED)
  async handleStockReleased(
    @Payload() payload: unknown,
    @Ctx() context: KafkaContext,
  ) {
    const envelope = extractEnvelope<StockReleasedPayload>(payload);
    const headers = parseKafkaHeaders(context.getMessage().headers);

    const outcome = await this.retryRunner.execute({
      topic: EventType.STOCK_RELEASED,
      envelope,
      headers,
      handler: async () => {
        this.logger.log(
          `Stock released for order ${envelope.payload.orderId}: ${envelope.payload.reason}`,
        );
      },
    });

    return { acknowledged: true, outcome };
  }

  @EventPattern(EventType.STOCK_FAILED)
  async handleStockFailed(
    @Payload() payload: unknown,
    @Ctx() context: KafkaContext,
  ) {
    const envelope = extractEnvelope<StockFailedPayload>(payload);
    const headers = parseKafkaHeaders(context.getMessage().headers);

    const outcome = await this.retryRunner.execute({
      topic: EventType.STOCK_FAILED,
      envelope,
      headers,
      handler: async () => {
        this.logger.warn(
          `Stock failed for order ${envelope.payload.orderId}: ${envelope.payload.reason}`,
        );
      },
    });

    return { acknowledged: true, outcome };
  }

  /** Marks end of happy path when customer notification is confirmed. */
  @EventPattern(EventType.NOTIFICATION_SENT)
  async handleNotificationSent(
    @Payload() payload: unknown,
    @Ctx() context: KafkaContext,
  ) {
    const envelope = extractEnvelope<NotificationSentPayload>(payload);
    const headers = parseKafkaHeaders(context.getMessage().headers);

    const outcome = await this.retryRunner.execute({
      topic: EventType.NOTIFICATION_SENT,
      envelope,
      headers,
      handler: async () => {
        this.logger.log(
          `Order flow completed for ${envelope.payload.orderId} — notification sent via ${envelope.payload.channel}`,
        );
      },
    });

    return { acknowledged: true, outcome, status: 'completed' };
  }
}

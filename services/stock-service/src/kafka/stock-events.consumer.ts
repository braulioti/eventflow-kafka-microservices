/**
 * Consumes payment.processed and order.cancelled; delegates to StockService with retry.
 */
import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import {
  EventType,
  extractEnvelope,
  parseKafkaHeaders,
  type OrderCancelledPayload,
  type PaymentProcessedPayload,
} from '@eventflow/shared';
import { StockService } from '../stock/stock.service';
import { KafkaRetryRunner } from './kafka-retry.runner';

@Controller()
export class StockEventsConsumer {
  private readonly logger = new Logger(StockEventsConsumer.name);

  constructor(
    private readonly stockService: StockService,
    private readonly retryRunner: KafkaRetryRunner,
  ) {}

  @EventPattern(EventType.PAYMENT_PROCESSED)
  async handlePaymentProcessed(
    @Payload() payload: unknown,
    @Ctx() context: KafkaContext,
  ) {
    const envelope = extractEnvelope<PaymentProcessedPayload>(payload);
    const headers = parseKafkaHeaders(context.getMessage().headers);

    const outcome = await this.retryRunner.execute({
      topic: EventType.PAYMENT_PROCESSED,
      envelope,
      headers,
      handler: async () => {
        this.logger.log(
          `Processing ${EventType.PAYMENT_PROCESSED} for ${envelope.payload.orderId}`,
        );
        await this.stockService.handlePaymentProcessed(envelope);
      },
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
      topic: EventType.ORDER_CANCELLED,
      envelope,
      headers,
      handler: () => this.stockService.handleOrderCancelled(envelope),
    });

    return { acknowledged: true, outcome };
  }
}

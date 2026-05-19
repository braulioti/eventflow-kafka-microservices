/**
 * Subscribes to order.created / order.cancelled and invokes PaymentService under retry.
 */
import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import {
  EventType,
  extractEnvelope,
  parseKafkaHeaders,
  type OrderCancelledPayload,
  type OrderCreatedPayload,
} from '@eventflow/shared';
import { PaymentService } from '../payment/payment.service';
import { KafkaRetryRunner } from './kafka-retry.runner';

@Controller()
export class PaymentEventsConsumer {
  private readonly logger = new Logger(PaymentEventsConsumer.name);

  constructor(
    private readonly paymentService: PaymentService,
    private readonly retryRunner: KafkaRetryRunner,
  ) {}

  @EventPattern(EventType.ORDER_CREATED)
  async handleOrderCreated(
    @Payload() payload: unknown,
    @Ctx() context: KafkaContext,
  ) {
    const envelope = extractEnvelope<OrderCreatedPayload>(payload);
    const headers = parseKafkaHeaders(context.getMessage().headers);

    const outcome = await this.retryRunner.execute({
      topic: EventType.ORDER_CREATED,
      envelope,
      headers,
      handler: async () => {
        this.logger.log(
          `Processing ${EventType.ORDER_CREATED} for ${envelope.payload.orderId}`,
        );
        await this.paymentService.handleOrderCreated(envelope);
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
      handler: () => this.paymentService.handleOrderCancelled(envelope),
    });

    return { acknowledged: true, outcome };
  }
}

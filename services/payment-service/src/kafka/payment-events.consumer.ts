/**
 * Subscribes to order.events (order.created) / order.cancelled and invokes PaymentService under retry.
 */
import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import {
  EventType,
  OrderKafkaTopic,
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
  /** In-process dedupe by envelope eventId (consumer idempotency). */
  private readonly processedEventIds = new Set<string>();

  constructor(
    private readonly paymentService: PaymentService,
    private readonly retryRunner: KafkaRetryRunner,
  ) {}

  @EventPattern(OrderKafkaTopic.ORDER_EVENTS)
  async handleOrderEvents(
    @Payload() payload: unknown,
    @Ctx() context: KafkaContext,
  ) {
    const envelope = extractEnvelope<OrderCreatedPayload>(payload);

    if (envelope.eventType !== EventType.ORDER_CREATED) {
      return { acknowledged: true, skipped: true, reason: 'unsupported-event-type' };
    }

    if (this.processedEventIds.has(envelope.eventId)) {
      this.logger.log(
        `Skipping duplicate ${EventType.ORDER_CREATED} (eventId=${envelope.eventId})`,
      );
      return { acknowledged: true, outcome: 'duplicate' };
    }

    const headers = parseKafkaHeaders(context.getMessage().headers);

    const outcome = await this.retryRunner.execute({
      eventType: EventType.ORDER_CREATED,
      envelope,
      headers,
      handler: async () => {
        this.logger.log(
          `Processing ${EventType.ORDER_CREATED} for ${envelope.payload.orderId}`,
        );
        await this.paymentService.handleOrderCreated(envelope);
        this.processedEventIds.add(envelope.eventId);
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
      eventType: EventType.ORDER_CANCELLED,
      envelope,
      headers,
      handler: () => this.paymentService.handleOrderCancelled(envelope),
    });

    return { acknowledged: true, outcome };
  }
}

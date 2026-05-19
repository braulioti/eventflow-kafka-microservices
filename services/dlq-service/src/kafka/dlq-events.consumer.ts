import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import {
  ALL_DLQ_TOPICS,
  EventType,
  extractEnvelope,
  isEventFailurePayload,
  type NotificationFailedPayload,
} from '@eventflow/shared';

@Controller()
export class DlqEventsConsumer {
  private readonly logger = new Logger(DlqEventsConsumer.name);

  /** Parses DLQ payload; prefers structured event.failure from retry exhaustion. */
  private handleDlqMessage(topic: string, payload: unknown) {
    try {
      const event = extractEnvelope<unknown>(payload);

      if (isEventFailurePayload(event.payload)) {
        const failure = event.payload;
        this.logger.error(
          `[DLQ] ${topic} | exhausted retries (${failure.retry.attempt}/${failure.retry.maxAttempts}) | ` +
            `original=${failure.originalEventType} eventId=${failure.originalEventId} ` +
            `correlationId=${failure.correlationId} error=${failure.error.message}`,
        );
        return { acknowledged: true, topic, failure: failure.kind };
      }

      this.logger.error(
        `[DLQ] ${topic} | eventId=${event.eventId} type=${event.eventType} correlationId=${event.correlationId}`,
      );
    } catch {
      this.logger.error(
        `[DLQ] ${topic} | invalid payload: ${JSON.stringify(payload)}`,
      );
    }

    return { acknowledged: true, topic };
  }

  @EventPattern(EventType.NOTIFICATION_FAILED)
  handleNotificationFailed(@Payload() payload: unknown) {
    const event = extractEnvelope<NotificationFailedPayload>(payload);
    this.logger.error(
      `[FAILED] notification ${event.payload.notificationId} order=${event.payload.orderId}: ${event.payload.reason}`,
    );
    return { acknowledged: true };
  }

  @EventPattern(ALL_DLQ_TOPICS[0])
  handleOrderCreatedDlq(@Payload() payload: unknown) {
    return this.handleDlqMessage(ALL_DLQ_TOPICS[0], payload);
  }

  @EventPattern(ALL_DLQ_TOPICS[1])
  handleOrderCancelledDlq(@Payload() payload: unknown) {
    return this.handleDlqMessage(ALL_DLQ_TOPICS[1], payload);
  }

  @EventPattern(ALL_DLQ_TOPICS[2])
  handlePaymentRequestedDlq(@Payload() payload: unknown) {
    return this.handleDlqMessage(ALL_DLQ_TOPICS[2], payload);
  }

  @EventPattern(ALL_DLQ_TOPICS[3])
  handlePaymentProcessedDlq(@Payload() payload: unknown) {
    return this.handleDlqMessage(ALL_DLQ_TOPICS[3], payload);
  }

  @EventPattern(ALL_DLQ_TOPICS[4])
  handlePaymentFailedDlq(@Payload() payload: unknown) {
    return this.handleDlqMessage(ALL_DLQ_TOPICS[4], payload);
  }

  @EventPattern(ALL_DLQ_TOPICS[5])
  handleStockReservedDlq(@Payload() payload: unknown) {
    return this.handleDlqMessage(ALL_DLQ_TOPICS[5], payload);
  }

  @EventPattern(ALL_DLQ_TOPICS[6])
  handleStockReleasedDlq(@Payload() payload: unknown) {
    return this.handleDlqMessage(ALL_DLQ_TOPICS[6], payload);
  }

  @EventPattern(ALL_DLQ_TOPICS[7])
  handleStockFailedDlq(@Payload() payload: unknown) {
    return this.handleDlqMessage(ALL_DLQ_TOPICS[7], payload);
  }

  @EventPattern(ALL_DLQ_TOPICS[8])
  handleNotificationSendDlq(@Payload() payload: unknown) {
    return this.handleDlqMessage(ALL_DLQ_TOPICS[8], payload);
  }

  @EventPattern(ALL_DLQ_TOPICS[9])
  handleNotificationSentDlq(@Payload() payload: unknown) {
    return this.handleDlqMessage(ALL_DLQ_TOPICS[9], payload);
  }

  @EventPattern(ALL_DLQ_TOPICS[10])
  handleNotificationFailedDlq(@Payload() payload: unknown) {
    return this.handleDlqMessage(ALL_DLQ_TOPICS[10], payload);
  }
}

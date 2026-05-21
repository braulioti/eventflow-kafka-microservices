/**
 * DLQ Service — Kafka Dead-Letter and Failure Consumer
 *
 * Central subscriber for every `*.dlq` topic defined in {@link ALL_DLQ_TOPICS}
 * plus the first-class `notification.failed` event. Parses envelopes, prefers
 * structured `event.failure` payloads from retry exhaustion, and logs rich
 * context for on-call investigation.
 *
 * ## Handler strategy
 *
 * - **DLQ topics:** Each `ALL_DLQ_TOPICS[n]` has a dedicated `@EventPattern`
 *   method delegating to {@link handleDlqMessage} (required because Nest
 *   decorators need compile-time constant patterns).
 * - **notification.failed:** Separate handler with notification-specific log line.
 *
 * No retry runner is used — messages are assumed terminal; the service always
 * acknowledges after logging.
 *
 * @module dlq-service/kafka/dlq-events.consumer
 */
import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import {
  ALL_DLQ_TOPICS,
  EventType,
  extractEnvelope,
  isEventFailurePayload,
  type NotificationFailedPayload,
} from '@eventflow/shared';

/**
 * Kafka consumer that logs DLQ and notification failure events.
 */
@Controller()
export class DlqEventsConsumer {
  private readonly logger = new Logger(DlqEventsConsumer.name);

  /**
   * Shared DLQ message parser and logger.
   *
   * When the payload contains a structured `event.failure` (retry exhausted),
   * logs attempt counts, original event metadata, and error message. Otherwise
   * logs generic envelope fields. Unparseable payloads are logged as raw JSON.
   *
   * @param topic   - DLQ Kafka topic name (for log prefixing).
   * @param payload - Raw message value from Kafka.
   * @returns Acknowledgment object always marking the message as handled.
   */
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

  /**
   * Handles domain-level `notification.failed` (not a `*.dlq` topic).
   *
   * @param payload - Kafka message containing notification failure envelope.
   */
  @EventPattern(EventType.NOTIFICATION_FAILED)
  handleNotificationFailed(@Payload() payload: unknown) {
    const event = extractEnvelope<NotificationFailedPayload>(payload);
    this.logger.error(
      `[FAILED] notification ${event.payload.notificationId} order=${event.payload.orderId}: ${event.payload.reason}`,
    );
    return { acknowledged: true };
  }

  /** DLQ handler for `order.created.dlq` ({@link ALL_DLQ_TOPICS}[0]). */
  @EventPattern(ALL_DLQ_TOPICS[0])
  handleOrderCreatedDlq(@Payload() payload: unknown) {
    return this.handleDlqMessage(ALL_DLQ_TOPICS[0], payload);
  }

  /** DLQ handler for `order.cancelled.dlq` ({@link ALL_DLQ_TOPICS}[1]). */
  @EventPattern(ALL_DLQ_TOPICS[1])
  handleOrderCancelledDlq(@Payload() payload: unknown) {
    return this.handleDlqMessage(ALL_DLQ_TOPICS[1], payload);
  }

  /** DLQ handler for `payment.requested.dlq` ({@link ALL_DLQ_TOPICS}[2]). */
  @EventPattern(ALL_DLQ_TOPICS[2])
  handlePaymentRequestedDlq(@Payload() payload: unknown) {
    return this.handleDlqMessage(ALL_DLQ_TOPICS[2], payload);
  }

  /** DLQ handler for `payment.processed.dlq` ({@link ALL_DLQ_TOPICS}[3]). */
  @EventPattern(ALL_DLQ_TOPICS[3])
  handlePaymentProcessedDlq(@Payload() payload: unknown) {
    return this.handleDlqMessage(ALL_DLQ_TOPICS[3], payload);
  }

  /** DLQ handler for `payment.failed.dlq` ({@link ALL_DLQ_TOPICS}[4]). */
  @EventPattern(ALL_DLQ_TOPICS[4])
  handlePaymentFailedDlq(@Payload() payload: unknown) {
    return this.handleDlqMessage(ALL_DLQ_TOPICS[4], payload);
  }

  /** DLQ handler for `stock.reserved.dlq` ({@link ALL_DLQ_TOPICS}[5]). */
  @EventPattern(ALL_DLQ_TOPICS[5])
  handleStockReservedDlq(@Payload() payload: unknown) {
    return this.handleDlqMessage(ALL_DLQ_TOPICS[5], payload);
  }

  /** DLQ handler for `stock.released.dlq` ({@link ALL_DLQ_TOPICS}[6]). */
  @EventPattern(ALL_DLQ_TOPICS[6])
  handleStockReleasedDlq(@Payload() payload: unknown) {
    return this.handleDlqMessage(ALL_DLQ_TOPICS[6], payload);
  }

  /** DLQ handler for `stock.failed.dlq` ({@link ALL_DLQ_TOPICS}[7]). */
  @EventPattern(ALL_DLQ_TOPICS[7])
  handleStockFailedDlq(@Payload() payload: unknown) {
    return this.handleDlqMessage(ALL_DLQ_TOPICS[7], payload);
  }

  /** DLQ handler for `notification.send.dlq` ({@link ALL_DLQ_TOPICS}[8]). */
  @EventPattern(ALL_DLQ_TOPICS[8])
  handleNotificationSendDlq(@Payload() payload: unknown) {
    return this.handleDlqMessage(ALL_DLQ_TOPICS[8], payload);
  }

  /** DLQ handler for `notification.sent.dlq` ({@link ALL_DLQ_TOPICS}[9]). */
  @EventPattern(ALL_DLQ_TOPICS[9])
  handleNotificationSentDlq(@Payload() payload: unknown) {
    return this.handleDlqMessage(ALL_DLQ_TOPICS[9], payload);
  }

  /** DLQ handler for `notification.failed.dlq` ({@link ALL_DLQ_TOPICS}[10]). */
  @EventPattern(ALL_DLQ_TOPICS[10])
  handleNotificationFailedDlq(@Payload() payload: unknown) {
    return this.handleDlqMessage(ALL_DLQ_TOPICS[10], payload);
  }
}

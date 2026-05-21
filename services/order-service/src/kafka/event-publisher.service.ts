/**
 * @file event-publisher.service.ts
 * @module order-service — Kafka producer
 *
 * Implements {@link KafkaEventTransport} for saga initiation: primarily
 * `order.created` on `order.events`. Uses shared producer retry for transient
 * broker errors before surfacing failure to `OrdersService`.
 *
 * ## Message shape
 *
 * - Topic from `resolveKafkaTopic(eventType)`
 * - Partition key = `orderId` via `resolvePartitionKey` (per-order ordering)
 * - Headers from envelope + optional overrides
 *
 * ## DLQ
 *
 * `publishToDlq` supports consumer retry exhaustion paths from `KafkaRetryRunner`.
 *
 * @see OrdersService.createOrder
 */
import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import {
  type EventEnvelope,
  type EventFailurePayload,
  type EventPayloadMap,
  type EventTypeValue,
  type KafkaEventTransport,
  type PublishOptions,
  emitKafkaEvent,
  envelopeToKafkaHeaders,
  publishWithProducerRetry,
  resolveKafkaTopic,
  resolvePartitionKey,
  toDlqTopic,
} from '@eventflow/shared';

/** Nest DI token for the Kafka client proxy. */
export const KAFKA_CLIENT = 'KAFKA_CLIENT';

/**
 * Kafka producer with logging and shared publish retry wrapper.
 */
@Injectable()
export class EventPublisher implements OnModuleInit, KafkaEventTransport {
  private readonly logger = new Logger(EventPublisher.name);

  constructor(
    @Inject(KAFKA_CLIENT) private readonly kafkaClient: ClientKafka,
  ) {}

  /** Connects producer on module init. */
  async onModuleInit(): Promise<void> {
    await this.kafkaClient.connect();
  }

  /**
   * Publishes a domain envelope with producer-side retries.
   *
   * @typeParam T - Key of {@link EventPayloadMap}
   * @param eventType - e.g. `order.created`
   * @param envelope - Full event envelope
   * @param options - Optional extra headers
   */
  async publish<T extends keyof EventPayloadMap>(
    eventType: EventTypeValue,
    envelope: EventEnvelope<EventPayloadMap[T]>,
    options?: PublishOptions,
  ): Promise<void> {
    const kafkaTopic = resolveKafkaTopic(eventType);
    const partitionKey = resolvePartitionKey(envelope);

    const headers = {
      ...envelopeToKafkaHeaders(envelope),
      ...options?.headers,
    };

    this.logger.log(
      `Publishing ${eventType} → topic=${kafkaTopic} key=${partitionKey} eventId=${envelope.eventId}`,
    );

    try {
      await publishWithProducerRetry(
        () =>
          emitKafkaEvent(this.kafkaClient, kafkaTopic, {
            key: partitionKey,
            value: envelope,
            headers,
          }),
        {
          onRetry: (attempt, delayMs, error) => {
            this.logger.warn(
              `Kafka publish retry ${attempt} for ${eventType} in ${delayMs}ms (eventId=${envelope.eventId}): ${error instanceof Error ? error.message : String(error)}`,
            );
          },
        },
      );

      this.logger.log(
        `Kafka publish succeeded: topic=${kafkaTopic} key=${partitionKey} eventId=${envelope.eventId}`,
      );
    } catch (error) {
      const detail =
        error instanceof Error
          ? `${error.message}${error.stack ? `\n${error.stack}` : ''}`
          : String(error);
      this.logger.error(
        `Kafka publish failed after retries: topic=${kafkaTopic} eventId=${envelope.eventId} — ${detail}`,
      );
      throw error;
    }
  }

  /**
   * Sends failure envelope to `{topic}.dlq` keyed by `correlationId`.
   *
   * @param eventType - Source event that could not be processed
   * @param envelope - Failure metadata for operators
   */
  async publishToDlq(
    eventType: EventTypeValue,
    envelope: EventEnvelope<EventFailurePayload>,
    options?: PublishOptions,
  ): Promise<void> {
    const partitionKey = envelope.payload.correlationId;
    const dlqTopic = toDlqTopic(resolveKafkaTopic(eventType));
    const headers = {
      ...envelopeToKafkaHeaders(envelope),
      ...options?.headers,
    };

    await publishWithProducerRetry(() =>
      emitKafkaEvent(this.kafkaClient, dlqTopic, {
        key: partitionKey,
        value: envelope,
        headers,
      }),
    );
  }
}

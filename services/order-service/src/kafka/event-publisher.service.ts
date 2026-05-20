/**
 * Kafka producer adapter implementing {@link KafkaEventTransport}.
 * Sets message key from orderId, copies envelope headers, and retries publish on failure.
 */
import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { firstValueFrom } from 'rxjs';
import {
  type EventEnvelope,
  type EventFailurePayload,
  type EventPayloadMap,
  type EventTypeValue,
  type KafkaEventTransport,
  type PublishOptions,
  envelopeToKafkaHeaders,
  publishWithProducerRetry,
  resolveKafkaTopic,
  resolvePartitionKey,
  toDlqTopic,
} from '@eventflow/shared';

/** Injection token for the Nest Kafka client proxy. */
export const KAFKA_CLIENT = 'KAFKA_CLIENT';

@Injectable()
export class EventPublisher implements OnModuleInit, KafkaEventTransport {
  private readonly logger = new Logger(EventPublisher.name);

  constructor(
    @Inject(KAFKA_CLIENT) private readonly kafkaClient: ClientKafka,
  ) {}

  async onModuleInit(): Promise<void> {
    await this.kafkaClient.connect();
  }

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
          firstValueFrom(
            this.kafkaClient.emit(kafkaTopic, {
              key: partitionKey,
              value: envelope,
              headers,
            }),
          ),
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
      this.logger.error(
        `Kafka publish failed after retries: topic=${kafkaTopic} eventId=${envelope.eventId}`,
        error instanceof Error ? error.stack : String(error),
      );
      throw error;
    }
  }

  /** Routes exhausted-retry failures to `{kafkaTopic}.dlq` keyed by correlationId. */
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
      firstValueFrom(
        this.kafkaClient.emit(dlqTopic, {
          key: partitionKey,
          value: envelope,
          headers,
        }),
      ),
    );
  }
}

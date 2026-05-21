/**
 * Notification Service — Kafka Event Publisher
 *
 * NestJS implementation of {@link KafkaEventTransport}. Converts domain
 * envelopes into Kafka records with resolved topics, partition keys, and
 * standard metadata headers from `@eventflow/shared`.
 *
 * @module notification-service/kafka/event-publisher.service
 */
import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
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
  resolveKafkaTopic,
  resolvePartitionKey,
  toDlqTopic,
} from '@eventflow/shared';

/**
 * DI token for the Kafka client registered in {@link KafkaModule}.
 */
export const KAFKA_CLIENT = 'KAFKA_CLIENT';

/**
 * Producer adapter for notification domain and retry/DLQ flows.
 */
@Injectable()
export class EventPublisher implements OnModuleInit, KafkaEventTransport {
  /**
   * @param kafkaClient - Injected Nest `ClientKafka` instance.
   */
  constructor(
    @Inject(KAFKA_CLIENT) private readonly kafkaClient: ClientKafka,
  ) {}

  /** Connects the Kafka client when the Nest module initializes. */
  async onModuleInit(): Promise<void> {
    await this.kafkaClient.connect();
  }

  /**
   * Emits a domain event to its primary Kafka topic.
   *
   * @param eventType - Determines topic via shared resolver.
   * @param envelope  - Typed event envelope (message value).
   * @param options   - Optional header overrides.
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

    await firstValueFrom(
      this.kafkaClient.emit(kafkaTopic, {
        key: partitionKey,
        value: envelope,
        headers,
      }),
    );
  }

  /**
   * Emits an `event.failure` envelope to the DLQ topic derived from `eventType`.
   *
   * @param eventType - Original failed event type (for DLQ topic naming).
   * @param envelope  - Structured failure payload from retry exhaustion.
   * @param options   - Optional Kafka headers.
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

    await firstValueFrom(
      this.kafkaClient.emit(dlqTopic, {
        key: partitionKey,
        value: envelope,
        headers,
      }),
    );
  }
}

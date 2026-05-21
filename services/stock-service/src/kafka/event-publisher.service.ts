/**
 * Stock Service — Kafka Event Publisher
 *
 * NestJS adapter implementing {@link KafkaEventTransport} from `@eventflow/shared`.
 * Resolves domain event types to Kafka topics, derives partition keys from
 * envelope metadata, and serializes standard headers for downstream consumers.
 *
 * ## Responsibilities
 *
 * - Connect the injected `ClientKafka` on module init.
 * - `publish` — emit domain events to their primary topics.
 * - `publishToDlq` — route exhausted-retry failures to `*.dlq` companion topics.
 *
 * @module stock-service/kafka/event-publisher.service
 */
import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
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
  resolveKafkaTopic,
  resolvePartitionKey,
  toDlqTopic,
} from '@eventflow/shared';

/**
 * Injection token for the Nest `ClientsModule` Kafka client registered in {@link KafkaModule}.
 */
export const KAFKA_CLIENT = 'KAFKA_CLIENT';

/**
 * Kafka producer used by stock domain logic and the shared retry executor.
 */
@Injectable()
export class EventPublisher implements OnModuleInit, KafkaEventTransport {
  /**
   * @param kafkaClient - Nest microservices Kafka client bound to `KAFKA_CLIENT`.
   */
  constructor(
    @Inject(KAFKA_CLIENT) private readonly kafkaClient: ClientKafka,
  ) {}

  /** Establishes the producer connection before any publish calls. */
  async onModuleInit(): Promise<void> {
    await this.kafkaClient.connect();
  }

  /**
   * Publishes a typed domain envelope to the topic resolved from `eventType`.
   *
   * @param eventType - Catalog event type determining target topic.
   * @param envelope  - Full event envelope (value + metadata).
   * @param options   - Optional extra Kafka headers merged after envelope headers.
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

    await emitKafkaEvent(this.kafkaClient, kafkaTopic, {
      key: partitionKey,
      value: envelope,
      headers,
    });
  }

  /**
   * Publishes a structured `event.failure` envelope to the DLQ topic for `eventType`.
   *
   * Partition key uses `correlationId` from the failure payload so related
   * poison messages stay ordered on a single partition for inspection.
   *
   * @param eventType - Original event type (used to derive `*.dlq` topic name).
   * @param envelope  - Failure envelope produced by {@link KafkaRetryExecutor}.
   * @param options   - Optional extra Kafka headers.
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

    await emitKafkaEvent(this.kafkaClient, dlqTopic, {
      key: partitionKey,
      value: envelope,
      headers,
    });
  }
}

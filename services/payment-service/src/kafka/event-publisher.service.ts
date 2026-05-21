/**
 * @file event-publisher.service.ts
 * @module payment-service — Kafka producer
 *
 * Implements {@link KafkaEventTransport} for the payment leg of the saga.
 * Publishes typed event envelopes to domain topics resolved from `@eventflow/shared`.
 *
 * ## Publishing contract
 *
 * - **Topic**: `resolveKafkaTopic(eventType)` (e.g. `payment.events`)
 * - **Key**: `resolvePartitionKey(envelope)` — typically `orderId` for ordering
 * - **Headers**: `envelopeToKafkaHeaders(envelope)` plus optional overrides
 * - **Value**: full `EventEnvelope` JSON
 *
 * ## DLQ path
 *
 * `publishToDlq` targets `{originalTopic}.dlq` with key = `correlationId` so
 * the dlq-service can group failures by saga instance.
 *
 * @see PaymentService — calls `publish` for `payment.requested`, `payment.processed`, `payment.failed`
 * @see KafkaRetryRunner — calls `publishToDlq` on consumer retry exhaustion
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

/** Nest injection token for the registered `ClientKafka` proxy. */
export const KAFKA_CLIENT = 'KAFKA_CLIENT';

/**
 * Kafka producer adapter for payment-service outbound events.
 */
@Injectable()
export class EventPublisher implements OnModuleInit, KafkaEventTransport {
  constructor(
    @Inject(KAFKA_CLIENT) private readonly kafkaClient: ClientKafka,
  ) {}

  /** Establishes the producer connection before first publish. */
  async onModuleInit(): Promise<void> {
    await this.kafkaClient.connect();
  }

  /**
   * Emits a domain event to its resolved Kafka topic.
   *
   * @typeParam T - Event type key into {@link EventPayloadMap}
   * @param eventType - Canonical event name (e.g. `payment.processed`)
   * @param envelope - Full envelope including payload and correlation ids
   * @param options - Optional extra Kafka headers
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
   * Routes a failure envelope to the DLQ topic derived from the source event type.
   *
   * @param eventType - Original event that failed processing
   * @param envelope - Failure payload with correlation metadata
   * @param options - Optional header overrides
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

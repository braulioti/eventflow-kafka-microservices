/** Kafka producer implementing {@link KafkaEventTransport} for stock-service. */
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
  getOrderIdFromPayload,
  toDlqTopic,
} from '@eventflow/shared';

export const KAFKA_CLIENT = 'KAFKA_CLIENT';

@Injectable()
export class EventPublisher implements OnModuleInit, KafkaEventTransport {
  constructor(
    @Inject(KAFKA_CLIENT) private readonly kafkaClient: ClientKafka,
  ) {}

  async onModuleInit(): Promise<void> {
    await this.kafkaClient.connect();
  }

  async publish<T extends keyof EventPayloadMap>(
    topic: EventTypeValue,
    envelope: EventEnvelope<EventPayloadMap[T]>,
    options?: PublishOptions,
  ): Promise<void> {
    const partitionKey = getOrderIdFromPayload(
      envelope.payload as { orderId: string },
    );

    const headers = {
      ...envelopeToKafkaHeaders(envelope),
      ...options?.headers,
    };

    await firstValueFrom(
      this.kafkaClient.emit(topic, {
        key: partitionKey,
        value: envelope,
        headers,
      }),
    );
  }

  async publishToDlq(
    originalTopic: EventTypeValue,
    envelope: EventEnvelope<EventFailurePayload>,
    options?: PublishOptions,
  ): Promise<void> {
    const partitionKey = envelope.payload.correlationId;
    const dlqTopic = toDlqTopic(originalTopic);
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

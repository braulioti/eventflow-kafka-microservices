/**
 * @file kafka.module.ts
 * @module order-service — Kafka client wiring
 *
 * Provides the outbound `ClientKafka` used by `EventPublisher` to emit
 * `order.created` and the `KafkaRetryRunner` for inbound handler retries/DLQ.
 *
 * Consumer transport is connected in `main.ts`; this module is producer-focused.
 *
 * @see EventPublisher
 * @see KafkaRetryRunner
 */
import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { getKafkaClientConfig } from '@eventflow/shared';
import { EventPublisher, KAFKA_CLIENT } from './event-publisher.service';
import { KafkaRetryRunner } from './kafka-retry.runner';

/**
 * Kafka infrastructure: registered client + retry executor export.
 */
@Module({
  imports: [
    ClientsModule.register([
      {
        name: KAFKA_CLIENT,
        transport: Transport.KAFKA,
        options: getKafkaClientConfig('order-service'),
      },
    ]),
  ],
  providers: [EventPublisher, KafkaRetryRunner],
  exports: [EventPublisher, KafkaRetryRunner],
})
export class KafkaModule {}

/**
 * @file kafka.module.ts
 * @module payment-service — Kafka client wiring
 *
 * Registers the NestJS `ClientsModule` Kafka producer used by `EventPublisher`
 * and injects `KafkaRetryRunner` for consumer-side retry/DLQ orchestration.
 *
 * ## Producer configuration
 *
 * Broker settings, client id, and serialization come from shared
 * `getKafkaClientConfig('payment-service')` so all EventFlow services stay aligned.
 *
 * ## Consumers
 *
 * Consumer transport is **not** declared here — it is attached in `main.ts` via
 * `connectMicroservice`. This module only supplies the **outbound** client token
 * `KAFKA_CLIENT` for publishing `payment.*` and DLQ messages.
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
 * Kafka infrastructure module: producer client + retry executor adapter.
 */
@Module({
  imports: [
    ClientsModule.register([
      {
        name: KAFKA_CLIENT,
        transport: Transport.KAFKA,
        options: getKafkaClientConfig('payment-service'),
      },
    ]),
  ],
  providers: [EventPublisher, KafkaRetryRunner],
  exports: [EventPublisher, KafkaRetryRunner],
})
export class KafkaModule {}

/**
 * Notification Service — Kafka Infrastructure Module
 *
 * Registers the NestJS Kafka client, {@link EventPublisher}, and
 * {@link KafkaRetryRunner}. Configuration is sourced from
 * `getKafkaClientConfig('notification-service')` for consistent cluster settings.
 *
 * @module notification-service/kafka/kafka.module
 */
import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { getKafkaClientConfig } from '@eventflow/shared';
import { EventPublisher, KAFKA_CLIENT } from './event-publisher.service';
import { KafkaRetryRunner } from './kafka-retry.runner';

/**
 * Provides and exports Kafka publish and consumer-retry dependencies.
 */
@Module({
  imports: [
    ClientsModule.register([
      {
        name: KAFKA_CLIENT,
        transport: Transport.KAFKA,
        options: getKafkaClientConfig('notification-service'),
      },
    ]),
  ],
  providers: [EventPublisher, KafkaRetryRunner],
  exports: [EventPublisher, KafkaRetryRunner],
})
export class KafkaModule {}

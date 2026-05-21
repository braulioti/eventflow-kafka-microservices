/**
 * Stock Service — Kafka Infrastructure Module
 *
 * Registers the NestJS `ClientsModule` Kafka producer client and wires
 * {@link EventPublisher} plus {@link KafkaRetryRunner} for consumer-side retry
 * and DLQ publishing. Both providers are exported so domain and consumer
 * modules can inject them without duplicating client configuration.
 *
 * Client options come from `getKafkaClientConfig('stock-service')` in the
 * shared package, keeping broker addresses and client ids consistent cluster-wide.
 *
 * @module stock-service/kafka/kafka.module
 */
import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { getKafkaClientConfig } from '@eventflow/shared';
import { EventPublisher, KAFKA_CLIENT } from './event-publisher.service';
import { KafkaRetryRunner } from './kafka-retry.runner';

/**
 * Nest module providing Kafka publish and consumer-retry capabilities.
 */
@Module({
  imports: [
    ClientsModule.register([
      {
        name: KAFKA_CLIENT,
        transport: Transport.KAFKA,
        options: getKafkaClientConfig('stock-service'),
      },
    ]),
  ],
  providers: [EventPublisher, KafkaRetryRunner],
  exports: [EventPublisher, KafkaRetryRunner],
})
export class KafkaModule {}

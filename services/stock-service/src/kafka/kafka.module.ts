/** Registers Nest Kafka client, EventPublisher, and KafkaRetryRunner for stock-service. */
import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { getKafkaClientConfig } from '@eventflow/shared';
import { EventPublisher, KAFKA_CLIENT } from './event-publisher.service';
import { KafkaRetryRunner } from './kafka-retry.runner';

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

/**
 * DLQ Service — Application Entry Point
 *
 * Observability-focused microservice that consumes dead-letter queue (`*.dlq`)
 * topics and the `notification.failed` event type. It does not mutate saga
 * state; it centralizes structured logging for operators debugging retry
 * exhaustion and poison messages.
 *
 * ## Role in the EventFlow pipeline
 *
 * When consumer retry policies exhaust attempts, upstream services publish
 * `event.failure` envelopes to DLQ topics. This service subscribes to every
 * catalog DLQ topic (via {@link ALL_DLQ_TOPICS}) and emits human-readable
 * error logs with correlation and retry metadata.
 *
 * ## Runtime configuration
 *
 * | Variable              | Default | Description                          |
 * |-----------------------|---------|--------------------------------------|
 * | `PORT`                | —       | HTTP listen port (overrides below)   |
 * | `DLQ_SERVICE_PORT`    | `3005`  | Service-specific HTTP port fallback  |
 * | Kafka env (shared)    | —       | Consumer group `dlq-service`         |
 *
 * ## Startup sequence
 *
 * Same hybrid pattern as other EventFlow services: HTTP + Kafka microservice.
 *
 * @module dlq-service/main
 * @see {@link DlqEventsConsumer} Per-topic DLQ handlers
 */
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import {
  formatKafkaConsumerBootstrap,
  getKafkaConsumerConfig,
  getKafkaConsumerConnectionInfo,
} from '@eventflow/shared';
import { AppModule } from './app.module';

/**
 * Bootstraps HTTP and Kafka for centralized DLQ observability.
 *
 * Default HTTP port is `3005` when env vars are unset.
 */
async function bootstrap() {
  const port = Number(process.env.PORT ?? process.env.DLQ_SERVICE_PORT ?? 3005);
  const app = await NestFactory.create(AppModule);

  const consumerInfo = getKafkaConsumerConnectionInfo('dlq-service');
  Logger.log(formatKafkaConsumerBootstrap(consumerInfo), 'KafkaConsumer');

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: getKafkaConsumerConfig('dlq-service'),
  });

  await app.startAllMicroservices();
  Logger.log('Kafka consumer microservice started', 'KafkaConsumer');

  await app.listen(port);
  Logger.log(`HTTP listening on port ${port}`, 'Bootstrap');
}
bootstrap();

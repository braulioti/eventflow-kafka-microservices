/**
 * @file main.ts
 * @module payment-service — application bootstrap
 *
 * Entry point for the Payment microservice in the EventFlow Kafka saga.
 *
 * ## Runtime architecture
 *
 * The process runs **two transports** in a single NestJS application:
 *
 * 1. **HTTP** (default port `3002`, overridable via `PORT` or `PAYMENT_SERVICE_PORT`)
 *    - Health and service metadata (`AppController`)
 *    - Event catalog discovery (`GET /events/catalog`)
 *
 * 2. **Kafka consumer microservice** (connected via `app.connectMicroservice`)
 *    - Subscribes using shared `getKafkaConsumerConfig('payment-service')`
 *    - Handlers live in `PaymentEventsConsumer` (`@EventPattern`)
 *
 * ## Kafka flows started from this process
 *
 * | Direction | Topic / pattern        | Handler / publisher              |
 * |-----------|------------------------|----------------------------------|
 * | Consume   | `order.events`         | `PaymentEventsConsumer` → `order.created` |
 * | Consume   | `order.cancelled`      | `PaymentEventsConsumer` → skip/compensate   |
 * | Produce   | `payment.events`       | `PaymentService` → `payment.requested`, `payment.processed`, `payment.failed` |
 *
 * Downstream services (order, stock, notification) react to payment outcomes; this
 * service is the **payment leg** of the choreography after `order.created`.
 *
 * ## Related concerns (implemented elsewhere in `src/`)
 *
 * - **Idempotency**: SQLite `processed_events` via `ProcessedEventsService` — prevents
 *   double-charging on Kafka redelivery or duplicate `order.created` for the same order.
 * - **Retry / DLQ**: `KafkaRetryRunner` wraps handlers with shared `KafkaRetryExecutor`;
 *   exhausted retries publish to `{topic}.dlq`.
 * - **Payment simulation**: `payment-simulator.ts` models gateway success/failure rates.
 *
 * @see AppModule
 * @see PaymentEventsConsumer
 * @see PaymentService
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
 * Boots the Nest application: Kafka consumer first, then HTTP listener.
 *
 * Startup order matters for observability — consumer connection info is logged
 * before `startAllMicroservices()` so operators can verify broker/group settings.
 */
async function bootstrap() {
  const port = Number(
    process.env.PORT ?? process.env.PAYMENT_SERVICE_PORT ?? 3002,
  );
  const app = await NestFactory.create(AppModule);

  const consumerInfo = getKafkaConsumerConnectionInfo('payment-service');
  Logger.log(formatKafkaConsumerBootstrap(consumerInfo), 'KafkaConsumer');

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: getKafkaConsumerConfig('payment-service'),
  });

  await app.startAllMicroservices();
  Logger.log('Kafka consumer microservice started', 'KafkaConsumer');

  await app.listen(port);
  Logger.log(`HTTP listening on port ${port}`, 'Bootstrap');
}
bootstrap();

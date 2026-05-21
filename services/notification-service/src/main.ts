/**
 * Notification Service — Application Entry Point
 *
 * Bootstraps a hybrid NestJS application combining HTTP operational endpoints
 * with a Kafka consumer microservice for customer-notification workflows in the
 * EventFlow order saga.
 *
 * ## Role in the EventFlow pipeline
 *
 * Downstream of stock reservation: when `stock.reserved` is published, this
 * service emits `notification.send` followed by `notification.sent`. It also
 * listens for `payment.failed` and `order.cancelled` to log compensating
 * notification paths (demo implementation; extend to publish failure events).
 *
 * ## Runtime configuration
 *
 * | Variable                      | Default | Description                          |
 * |-------------------------------|---------|--------------------------------------|
 * | `PORT`                        | —       | HTTP listen port (overrides below)   |
 * | `NOTIFICATION_SERVICE_PORT`   | `3004`  | Service-specific HTTP port fallback  |
 * | Kafka env (shared)            | —       | Brokers, group id via `@eventflow/shared` |
 *
 * ## Startup sequence
 *
 * 1. Create the root `AppModule` HTTP application.
 * 2. Log Kafka consumer connection metadata.
 * 3. Attach Kafka microservice with `getKafkaConsumerConfig('notification-service')`.
 * 4. Start microservices, then bind HTTP.
 *
 * @module notification-service/main
 * @see {@link AppModule} Root dependency-injection graph
 * @see {@link NotificationEventsConsumer} Kafka event handlers
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
 * Initializes HTTP and Kafka transports for the notification service.
 *
 * Port resolution prefers `PORT`, then `NOTIFICATION_SERVICE_PORT`, then `3004`.
 */
async function bootstrap() {
  const port = Number(
    process.env.PORT ?? process.env.NOTIFICATION_SERVICE_PORT ?? 3004,
  );
  const app = await NestFactory.create(AppModule);

  const consumerInfo = getKafkaConsumerConnectionInfo('notification-service');
  Logger.log(formatKafkaConsumerBootstrap(consumerInfo), 'KafkaConsumer');

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: getKafkaConsumerConfig('notification-service'),
  });

  await app.startAllMicroservices();
  Logger.log('Kafka consumer microservice started', 'KafkaConsumer');

  await app.listen(port);
  Logger.log(`HTTP listening on port ${port}`, 'Bootstrap');
}
bootstrap();

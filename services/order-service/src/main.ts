/**
 * @file main.ts
 * @module order-service — application bootstrap
 *
 * Entry point for the Order microservice — **saga origin** for new purchases.
 *
 * ## Runtime architecture
 *
 * 1. **HTTP** (default port `3001`, `PORT` or `ORDER_SERVICE_PORT`)
 *    - `POST /orders` — creates order in SQLite, publishes `order.created`
 *    - Health, catalog, metadata endpoints
 *
 * 2. **Kafka consumer microservice** (`connectMicroservice` in bootstrap)
 *    - `OrderEventsConsumer` — reacts to payment/stock/notification outcomes
 *
 * ## Kafka flows
 *
 * | Direction | Topic / pattern           | Role in saga                          |
 * |-----------|---------------------------|---------------------------------------|
 * | Produce   | `order.events`            | `order.created` starts downstream     |
 * | Consume   | `payment.events`        | `payment.failed` → order `failed`     |
 * | Consume   | `stock.released` etc.     | Compensation / completion status      |
 *
 * Global `ValidationPipe` validates REST DTOs before `OrdersService` runs.
 *
 * @see OrdersService.createOrder
 * @see OrderEventsConsumer
 */
import { Logger, ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import {
  formatKafkaConsumerBootstrap,
  getKafkaConsumerConfig,
  getKafkaConsumerConnectionInfo,
} from '@eventflow/shared';
import { AppModule } from './app.module';

/**
 * Creates Nest app, attaches Kafka consumer, starts microservices, listens HTTP.
 */
async function bootstrap() {
  const port = Number(process.env.PORT ?? process.env.ORDER_SERVICE_PORT ?? 3001);
  const app = await NestFactory.create(AppModule);

  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
    }),
  );

  const consumerInfo = getKafkaConsumerConnectionInfo('order-service');
  Logger.log(formatKafkaConsumerBootstrap(consumerInfo), 'KafkaConsumer');

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: getKafkaConsumerConfig('order-service'),
  });

  await app.startAllMicroservices();
  Logger.log('Kafka consumer microservice started', 'KafkaConsumer');

  await app.listen(port);
  Logger.log(`HTTP listening on port ${port}`, 'Bootstrap');
}
bootstrap();

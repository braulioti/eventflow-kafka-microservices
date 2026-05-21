/**
 * Stock Service — Application Entry Point
 *
 * Bootstraps a hybrid NestJS application that exposes HTTP endpoints for
 * health checks and event catalog discovery, while simultaneously running a
 * Kafka consumer microservice for event-driven inventory workflows.
 *
 * ## Role in the EventFlow pipeline
 *
 * The stock service sits downstream of payment processing in the order saga:
 * when `payment.processed` arrives on `payment.events`, this service simulates
 * inventory reservation and publishes `stock.reserved`. When an order is
 * cancelled (`order.cancelled`), it releases the reservation via `stock.released`.
 *
 * ## Runtime configuration
 *
 * | Variable              | Default | Description                          |
 * |-----------------------|---------|--------------------------------------|
 * | `PORT`                | —       | HTTP listen port (overrides below)   |
 * | `STOCK_SERVICE_PORT`  | `3003`  | Service-specific HTTP port fallback  |
 * | Kafka env (shared)    | —       | Brokers, group id via `@eventflow/shared` |
 *
 * ## Startup sequence
 *
 * 1. Create the root `AppModule` HTTP application.
 * 2. Log Kafka consumer connection metadata for operators.
 * 3. Attach a Kafka transport microservice with `getKafkaConsumerConfig('stock-service')`.
 * 4. Start all microservices (Kafka listeners) before binding HTTP.
 *
 * @module stock-service/main
 * @see {@link AppModule} Root dependency-injection graph
 * @see {@link StockEventsConsumer} Kafka `@EventPattern` handlers
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
 * Initializes HTTP and Kafka transports, then blocks until the process exits.
 *
 * Port resolution prefers `PORT`, then `STOCK_SERVICE_PORT`, then `3003`.
 * Kafka consumer options are centralized in the shared package so all
 * services use consistent broker lists, group ids, and retry topic wiring.
 */
async function bootstrap() {
  const port = Number(process.env.PORT ?? process.env.STOCK_SERVICE_PORT ?? 3003);
  const app = await NestFactory.create(AppModule);

  const consumerInfo = getKafkaConsumerConnectionInfo('stock-service');
  Logger.log(formatKafkaConsumerBootstrap(consumerInfo), 'KafkaConsumer');

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: getKafkaConsumerConfig('stock-service'),
  });

  await app.startAllMicroservices();
  Logger.log('Kafka consumer microservice started', 'KafkaConsumer');

  await app.listen(port);
  Logger.log(`HTTP listening on port ${port}`, 'Bootstrap');
}
bootstrap();

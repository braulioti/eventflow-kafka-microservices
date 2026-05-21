/**
 * Stock Service — Root NestJS Module
 *
 * Composes the stock domain, Kafka infrastructure, HTTP controllers, and the
 * Kafka event consumer into a single deployable unit. This module is the
 * dependency-injection root imported by `main.ts`.
 *
 * ## Registered components
 *
 * - **Imports:** `KafkaModule` (producer + retry runner), `StockModule` (domain).
 * - **Controllers:** `AppController` (health), `EventsController` (catalog),
 *   `StockEventsConsumer` (Kafka `@EventPattern` handlers).
 * - **Providers:** `AppService` (service metadata for HTTP probes).
 *
 * @module stock-service/app.module
 */
import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { EventsController } from './events/events.controller';
import { StockEventsConsumer } from './kafka/stock-events.consumer';
import { KafkaModule } from './kafka/kafka.module';
import { StockModule } from './stock/stock.module';

/**
 * Root application module wiring HTTP, catalog, and Kafka consumer surfaces.
 */
@Module({
  imports: [KafkaModule, StockModule],
  controllers: [AppController, EventsController, StockEventsConsumer],
  providers: [AppService],
})
export class AppModule {}

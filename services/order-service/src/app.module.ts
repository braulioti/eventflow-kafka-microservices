/**
 * @file app.module.ts
 * @module order-service — root Nest module
 *
 * Composes the order bounded context with Kafka infrastructure:
 *
 * - **OrdersModule** — REST API, SQLite aggregates, `order.created` producer
 * - **KafkaModule** — `EventPublisher`, `KafkaRetryRunner`
 *
 * ## Controllers
 *
 * | Controller           | Transport | Responsibility                    |
 * |----------------------|-----------|-----------------------------------|
 * | `OrdersController`   | HTTP      | `POST /orders`                    |
 * | `EventsController`   | HTTP      | Event catalog discovery           |
 * | `OrderEventsConsumer`| Kafka     | Downstream saga status updates    |
 * | `AppController`      | HTTP      | Health / service info             |
 */
import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { EventsController } from './events/events.controller';
import { OrderEventsConsumer } from './kafka/order-events.consumer';
import { KafkaModule } from './kafka/kafka.module';
import { OrdersModule } from './orders/orders.module';

/** Root module for order-service HTTP + Kafka. */
@Module({
  imports: [KafkaModule, OrdersModule],
  controllers: [AppController, EventsController, OrderEventsConsumer],
  providers: [AppService],
})
export class AppModule {}

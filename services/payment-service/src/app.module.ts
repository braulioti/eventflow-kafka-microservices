/**
 * @file app.module.ts
 * @module payment-service — root Nest module
 *
 * Wires the payment bounded context into a runnable application:
 *
 * - **KafkaModule**: producer client (`EventPublisher`) and consumer retry (`KafkaRetryRunner`)
 * - **PaymentModule**: domain logic (`PaymentService`), SQLite idempotency (`IdempotencyModule`)
 *
 * ## Controllers registered here
 *
 * | Controller              | Role                                              |
 * |-------------------------|---------------------------------------------------|
 * | `AppController`         | HTTP `/` and `/health`                            |
 * | `EventsController`      | `GET /events/catalog` — saga discovery            |
 * | `PaymentEventsConsumer` | Kafka `@EventPattern` handlers (not REST)       |
 *
 * `PaymentEventsConsumer` is declared as a controller because NestJS microservices
 * route Kafka messages to `@EventPattern` methods on controller classes.
 */
import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { EventsController } from './events/events.controller';
import { PaymentEventsConsumer } from './kafka/payment-events.consumer';
import { KafkaModule } from './kafka/kafka.module';
import { PaymentModule } from './payment/payment.module';

/** Root module assembling HTTP, Kafka consume/produce, and payment domain providers. */
@Module({
  imports: [KafkaModule, PaymentModule],
  controllers: [AppController, EventsController, PaymentEventsConsumer],
  providers: [AppService],
})
export class AppModule {}

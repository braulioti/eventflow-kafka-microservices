/**
 * @file payment.module.ts
 * @module payment-service — payment domain composition
 *
 * Binds together:
 *
 * - **KafkaModule** — outbound `payment.*` and DLQ via `EventPublisher`
 * - **IdempotencyModule** — SQLite dedup for inbound `order.created`
 * - **PaymentService** — orchestrates rules, simulation, publish, and idempotency writes
 *
 * Exported so other modules could reuse `PaymentService` or idempotency APIs if extended.
 */
import { Module } from '@nestjs/common';
import { IdempotencyModule } from '../idempotency/idempotency.module';
import { KafkaModule } from '../kafka/kafka.module';
import { PaymentService } from './payment.service';

/**
 * Payment bounded-context Nest module.
 */
@Module({
  imports: [KafkaModule, IdempotencyModule],
  providers: [PaymentService],
  exports: [PaymentService, IdempotencyModule],
})
export class PaymentModule {}

/**
 * @file idempotency.module.ts
 * @module payment-service — Kafka consumer idempotency
 *
 * Composes SQLite (`DatabaseModule`) with the `ProcessedEventsService` repository
 * so inbound `order.created` handling can be safely retried by Kafka or the
 * shared retry executor without double publishing payment outcomes.
 *
 * ## Idempotency keys
 *
 * 1. **Inbound `eventId`** (envelope) — primary key in `processed_events`
 * 2. **`orderId` + outcome `approved`** — secondary guard against charging the same order twice
 *
 * Exported for injection into `PaymentEventsConsumer` and `PaymentService`.
 */
import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { DatabaseModule } from '../database/database.module';
import { ProcessedEventEntity } from './entities/processed-event.entity';
import { ProcessedEventsService } from './processed-events.service';

/**
 * Feature module encapsulating durable deduplication for payment consumers.
 */
@Module({
  imports: [DatabaseModule, TypeOrmModule.forFeature([ProcessedEventEntity])],
  providers: [ProcessedEventsService],
  exports: [ProcessedEventsService],
})
export class IdempotencyModule {}

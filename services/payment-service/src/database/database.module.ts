/**
 * @file database.module.ts
 * @module payment-service — SQLite persistence (TypeORM)
 *
 * Configures a **local SQLite** database for consumer-side idempotency only.
 * Payment outcomes are not stored as rich domain rows here — only deduplication
 * metadata in `processed_events` (see `ProcessedEventEntity`).
 *
 * ## Storage
 *
 * - Driver: `better-sqlite3` (embedded file, no separate DB server)
 * - Default path: `{cwd}/data/payments.sqlite`
 * - Override: `PAYMENT_DATABASE_PATH`
 * - `synchronize: true` — schema auto-migrated on startup (demo/dev friendly)
 *
 * ## Why SQLite in this service
 *
 * Kafka guarantees at-least-once delivery. Without a durable idempotency store,
 * redelivered `order.created` events could trigger duplicate charges. SQLite
 * gives a single-node, low-latency dedup table co-located with the consumer.
 *
 * @see ProcessedEventEntity
 * @see IdempotencyModule
 */
import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { join } from 'path';
import { ProcessedEventEntity } from '../idempotency/entities/processed-event.entity';

/**
 * Nest module that registers the TypeORM root connection for payment-service.
 */
@Module({
  imports: [
    TypeOrmModule.forRoot({
      type: 'better-sqlite3',
      database:
        process.env.PAYMENT_DATABASE_PATH ??
        join(process.cwd(), 'data', 'payments.sqlite'),
      entities: [ProcessedEventEntity],
      synchronize: true,
    }),
  ],
})
export class DatabaseModule {}

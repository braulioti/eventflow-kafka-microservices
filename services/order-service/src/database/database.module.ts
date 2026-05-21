/**
 * @file database.module.ts
 * @module order-service — SQLite persistence (TypeORM)
 *
 * Persists the **order aggregate** (header + line items) before Kafka publish.
 * This is the system of record for order status during the saga; Kafka events
 * notify other services but status transitions are written here on consume.
 *
 * ## Storage
 *
 * - Driver: `better-sqlite3`
 * - Default file: `{cwd}/data/orders.sqlite`
 * - Override: `ORDER_DATABASE_PATH`
 * - Entities: `OrderEntity`, `OrderItemEntity`
 * - `synchronize: true` — auto schema for local demos
 *
 * ## Idempotency field
 *
 * `OrderEntity.eventId` stores the `order.created` envelope id **before** publish
 * so HTTP retries or partial failures can avoid duplicate Kafka messages.
 *
 * @see OrderEntity
 * @see OrdersService.createOrder
 */
import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { join } from 'path';
import { OrderItemEntity } from '../orders/entities/order-item.entity';
import { OrderEntity } from '../orders/entities/order.entity';

/**
 * Registers TypeORM root connection for order-service SQLite database.
 */
@Module({
  imports: [
    TypeOrmModule.forRoot({
      type: 'better-sqlite3',
      database:
        process.env.ORDER_DATABASE_PATH ??
        join(process.cwd(), 'data', 'orders.sqlite'),
      entities: [OrderEntity, OrderItemEntity],
      synchronize: true,
    }),
  ],
})
export class DatabaseModule {}

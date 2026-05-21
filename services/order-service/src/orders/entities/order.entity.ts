/**
 * @file order.entity.ts
 * @module order-service — order aggregate root (TypeORM)
 *
 * SQLite-backed source of truth for order header fields and saga correlation.
 * Line items are modeled as `OrderItemEntity` children (cascade persist).
 *
 * ## Kafka-related columns
 *
 * - `eventId` — `order.created` envelope id; set **before** publish for idempotency
 * - `id` — used as `correlationId` and Kafka partition key for order events
 *
 * ## Status
 *
 * See {@link OrderStatus}; updated by `OrdersService` on create failure or
 * `OrderEventsConsumer` on downstream events.
 *
 * @see OrderItemEntity
 */
import {
  Column,
  CreateDateColumn,
  Entity,
  OneToMany,
  PrimaryColumn,
  UpdateDateColumn,
} from 'typeorm';
import { OrderItemEntity } from './order-item.entity';
import { OrderStatus } from './order-status.enum';

/**
 * Order aggregate root table `orders`.
 */
@Entity({ name: 'orders' })
export class OrderEntity {
  /** Primary key — also saga correlation id and Kafka message key. */
  @PrimaryColumn({ type: 'text' })
  id!: string;

  @Column({ name: 'customer_id' })
  customerId!: string;

  /** Lifecycle state — {@link OrderStatus} string value. */
  @Column({ type: 'text', default: OrderStatus.PENDING })
  status!: OrderStatus;

  @Column({ name: 'total_amount', type: 'real' })
  totalAmount!: number;

  @Column({ type: 'text', default: 'BRL' })
  currency!: string;

  /**
   * Envelope `eventId` for `order.created` — non-null after pre-publish save.
   * Enables HTTP-level idempotent publish (see shared `hasPublishedEventId`).
   */
  @Column({ name: 'event_id', type: 'text', nullable: true })
  eventId!: string | null;

  @OneToMany(() => OrderItemEntity, (item) => item.order, { cascade: true })
  items!: OrderItemEntity[];

  @CreateDateColumn({ name: 'created_at' })
  createdAt!: Date;

  @UpdateDateColumn({ name: 'updated_at' })
  updatedAt!: Date;
}

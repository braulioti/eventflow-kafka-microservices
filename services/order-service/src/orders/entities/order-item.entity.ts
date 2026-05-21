/**
 * @file order-item.entity.ts
 * @module order-service — order line item (TypeORM)
 *
 * Child entity of {@link OrderEntity}, stored in `order_items` SQLite table.
 * Quantities and prices are copied into the `order.created` Kafka payload for
 * payment-service business rules (amount vs line-item sum checks).
 *
 * Cascade delete removes items when parent order row is deleted.
 */
import {
  Column,
  Entity,
  JoinColumn,
  ManyToOne,
  PrimaryGeneratedColumn,
} from 'typeorm';
import { OrderEntity } from './order.entity';

/**
 * Line item row belonging to an order aggregate.
 */
@Entity({ name: 'order_items' })
export class OrderItemEntity {
  @PrimaryGeneratedColumn('uuid')
  id!: string;

  @Column({ name: 'order_id' })
  orderId!: string;

  @ManyToOne(() => OrderEntity, (order) => order.items, { onDelete: 'CASCADE' })
  @JoinColumn({ name: 'order_id' })
  order!: OrderEntity;

  @Column({ name: 'product_id' })
  productId!: string;

  @Column({ type: 'integer' })
  quantity!: number;

  @Column({ name: 'unit_price', type: 'real' })
  unitPrice!: number;
}

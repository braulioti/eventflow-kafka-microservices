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

/** Order aggregate root — source of truth before/outside Kafka delivery. */
@Entity({ name: 'orders' })
export class OrderEntity {
  @PrimaryColumn({ type: 'text' })
  id!: string;

  @Column({ name: 'customer_id' })
  customerId!: string;

  @Column({ type: 'text', default: OrderStatus.PENDING })
  status!: OrderStatus;

  @Column({ name: 'total_amount', type: 'real' })
  totalAmount!: number;

  @Column({ type: 'text', default: 'BRL' })
  currency!: string;

  /** Kafka envelope id for order.created (set after successful publish). */
  @Column({ name: 'event_id', type: 'text', nullable: true })
  eventId!: string | null;

  @OneToMany(() => OrderItemEntity, (item) => item.order, { cascade: true })
  items!: OrderItemEntity[];

  @CreateDateColumn({ name: 'created_at' })
  createdAt!: Date;

  @UpdateDateColumn({ name: 'updated_at' })
  updatedAt!: Date;
}

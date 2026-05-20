import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { join } from 'path';
import { OrderItemEntity } from '../orders/entities/order-item.entity';
import { OrderEntity } from '../orders/entities/order.entity';

/** SQLite persistence for order-service (file path via ORDER_DATABASE_PATH). */
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

/**
 * @file orders.module.ts
 * @module order-service — orders bounded context
 *
 * Wires SQLite persistence, REST controller, and Kafka producer for the
 * order aggregate lifecycle:
 *
 * - Create via HTTP → save → publish `order.created`
 * - Status updates via `OrderEventsConsumer` → `OrdersService.updateOrderStatus`
 *
 * @see DatabaseModule
 * @see KafkaModule
 */
import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { DatabaseModule } from '../database/database.module';
import { KafkaModule } from '../kafka/kafka.module';
import { OrderItemEntity } from './entities/order-item.entity';
import { OrderEntity } from './entities/order.entity';
import { OrdersController } from './orders.controller';
import { OrdersService } from './orders.service';

/**
 * Nest module for order REST API and domain service exports.
 */
@Module({
  imports: [
    DatabaseModule,
    TypeOrmModule.forFeature([OrderEntity, OrderItemEntity]),
    KafkaModule,
  ],
  controllers: [OrdersController],
  providers: [OrdersService],
  exports: [OrdersService],
})
export class OrdersModule {}

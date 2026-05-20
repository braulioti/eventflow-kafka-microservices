import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { DatabaseModule } from '../database/database.module';
import { KafkaModule } from '../kafka/kafka.module';
import { OrderItemEntity } from './entities/order-item.entity';
import { OrderEntity } from './entities/order.entity';
import { OrdersController } from './orders.controller';
import { OrdersService } from './orders.service';

/** Order bounded context: SQLite + Kafka producer for order.created. */
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

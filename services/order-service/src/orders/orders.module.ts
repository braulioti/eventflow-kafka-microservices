/** Order domain: publishes order.created and depends on KafkaModule for transport. */
import { Module } from '@nestjs/common';
import { KafkaModule } from '../kafka/kafka.module';
import { OrdersController } from './orders.controller';
import { OrdersService } from './orders.service';

@Module({
  imports: [KafkaModule],
  controllers: [OrdersController],
  providers: [OrdersService],
})
export class OrdersModule {}

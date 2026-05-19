/** Root module: REST orders API, event catalog endpoint, and Kafka consumers. */
import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { EventsController } from './events/events.controller';
import { OrderEventsConsumer } from './kafka/order-events.consumer';
import { KafkaModule } from './kafka/kafka.module';
import { OrdersModule } from './orders/orders.module';

@Module({
  imports: [KafkaModule, OrdersModule],
  controllers: [AppController, EventsController, OrderEventsConsumer],
  providers: [AppService],
})
export class AppModule {}

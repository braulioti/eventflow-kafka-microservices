/** Root module: notification domain, catalog endpoint, and Kafka consumers. */
import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { EventsController } from './events/events.controller';
import { NotificationEventsConsumer } from './kafka/notification-events.consumer';
import { KafkaModule } from './kafka/kafka.module';
import { NotificationModule } from './notification/notification.module';

@Module({
  imports: [KafkaModule, NotificationModule],
  controllers: [AppController, EventsController, NotificationEventsConsumer],
  providers: [AppService],
})
export class AppModule {}

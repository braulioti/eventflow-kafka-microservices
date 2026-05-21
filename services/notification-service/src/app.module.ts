/**
 * Notification Service — Root NestJS Module
 *
 * Wires the notification domain, Kafka producer/retry infrastructure, HTTP
 * controllers, and the Kafka consumer into one deployable application graph.
 *
 * ## Registered components
 *
 * - **Imports:** `KafkaModule`, `NotificationModule`.
 * - **Controllers:** `AppController`, `EventsController`, `NotificationEventsConsumer`.
 * - **Providers:** `AppService`.
 *
 * @module notification-service/app.module
 */
import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { EventsController } from './events/events.controller';
import { NotificationEventsConsumer } from './kafka/notification-events.consumer';
import { KafkaModule } from './kafka/kafka.module';
import { NotificationModule } from './notification/notification.module';

/**
 * Root module composing HTTP, catalog, Kafka, and notification domain layers.
 */
@Module({
  imports: [KafkaModule, NotificationModule],
  controllers: [AppController, EventsController, NotificationEventsConsumer],
  providers: [AppService],
})
export class AppModule {}

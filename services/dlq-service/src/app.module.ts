/** Subscribes to all *.dlq topics plus notification.failed for centralized logging. */
import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { EventsController } from './events/events.controller';
import { DlqEventsConsumer } from './kafka/dlq-events.consumer';

@Module({
  imports: [],
  controllers: [AppController, EventsController, DlqEventsConsumer],
  providers: [AppService],
})
export class AppModule {}

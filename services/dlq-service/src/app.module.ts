/**
 * DLQ Service — Root NestJS Module
 *
 * Lightweight composition: no domain modules or Kafka producer — only HTTP
 * probes, an expanded event catalog endpoint, and {@link DlqEventsConsumer}
 * subscribed to all platform DLQ topics plus `notification.failed`.
 *
 * ## Design note
 *
 * DLQ handling is intentionally read-only (log and acknowledge). Replaying
 * messages would belong in a separate recovery tool, not this observability service.
 *
 * @module dlq-service/app.module
 */
import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { EventsController } from './events/events.controller';
import { DlqEventsConsumer } from './kafka/dlq-events.consumer';

/**
 * Root module for DLQ logging and catalog HTTP endpoints.
 */
@Module({
  imports: [],
  controllers: [AppController, EventsController, DlqEventsConsumer],
  providers: [AppService],
})
export class AppModule {}

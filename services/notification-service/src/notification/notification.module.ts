/**
 * Notification Service — Domain Module
 *
 * Registers {@link NotificationService} and imports {@link KafkaModule} so
 * notification handlers can publish `notification.send` and `notification.sent`.
 * Exported for injection by the Kafka consumer controller.
 *
 * @module notification-service/notification/notification.module
 */
import { Module } from '@nestjs/common';
import { KafkaModule } from '../kafka/kafka.module';
import { NotificationService } from './notification.service';

/**
 * Nest module encapsulating customer notification domain logic.
 */
@Module({
  imports: [KafkaModule],
  providers: [NotificationService],
  exports: [NotificationService],
})
export class NotificationModule {}

/** Notification dispatch domain integrated with Kafka. */
import { Module } from '@nestjs/common';
import { KafkaModule } from '../kafka/kafka.module';
import { NotificationService } from './notification.service';

@Module({
  imports: [KafkaModule],
  providers: [NotificationService],
  exports: [NotificationService],
})
export class NotificationModule {}

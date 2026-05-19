/** Payment domain wired to Kafka publish/consume via KafkaModule. */
import { Module } from '@nestjs/common';
import { KafkaModule } from '../kafka/kafka.module';
import { PaymentService } from './payment.service';

@Module({
  imports: [KafkaModule],
  providers: [PaymentService],
  exports: [PaymentService],
})
export class PaymentModule {}

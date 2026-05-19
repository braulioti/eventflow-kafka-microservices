/** Root module: payment domain logic, catalog endpoint, and Kafka consumers. */
import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { EventsController } from './events/events.controller';
import { PaymentEventsConsumer } from './kafka/payment-events.consumer';
import { KafkaModule } from './kafka/kafka.module';
import { PaymentModule } from './payment/payment.module';

@Module({
  imports: [KafkaModule, PaymentModule],
  controllers: [AppController, EventsController, PaymentEventsConsumer],
  providers: [AppService],
})
export class AppModule {}

/** Root module: stock domain, catalog endpoint, and Kafka consumers. */
import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { EventsController } from './events/events.controller';
import { StockEventsConsumer } from './kafka/stock-events.consumer';
import { KafkaModule } from './kafka/kafka.module';
import { StockModule } from './stock/stock.module';

@Module({
  imports: [KafkaModule, StockModule],
  controllers: [AppController, EventsController, StockEventsConsumer],
  providers: [AppService],
})
export class AppModule {}

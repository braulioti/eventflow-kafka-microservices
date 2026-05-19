/** Stock reservation domain with KafkaModule for event-driven integration. */
import { Module } from '@nestjs/common';
import { KafkaModule } from '../kafka/kafka.module';
import { StockService } from './stock.service';

@Module({
  imports: [KafkaModule],
  providers: [StockService],
  exports: [StockService],
})
export class StockModule {}

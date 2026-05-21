/**
 * Stock Service — Domain Module
 *
 * Encapsulates inventory reservation logic (`StockService`) and imports
 * `KafkaModule` so the domain layer can publish `stock.reserved` and
 * `stock.released` events. Exported for potential reuse by other Nest modules
 * within this service (currently consumed only via `StockEventsConsumer`).
 *
 * @module stock-service/stock/stock.module
 */
import { Module } from '@nestjs/common';
import { KafkaModule } from '../kafka/kafka.module';
import { StockService } from './stock.service';

/**
 * Nest module registering and exporting the stock reservation domain service.
 */
@Module({
  imports: [KafkaModule],
  providers: [StockService],
  exports: [StockService],
})
export class StockModule {}

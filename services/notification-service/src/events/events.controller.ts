/**
 * Notification Service — Event Catalog HTTP Controller
 *
 * Exposes the produce/consume contract for `notification-service` using the
 * shared event catalog. Useful for documentation generation, contract tests,
 * and verifying that Kafka topic bindings match the catalog.
 *
 * ## Routes
 *
 * | Method | Path              | Purpose                                     |
 * |--------|-------------------|---------------------------------------------|
 * | GET    | `/events/catalog` | Events produced and consumed by this service |
 *
 * @module notification-service/events/events.controller
 */
import { Controller, Get } from '@nestjs/common';
import {
  getEventsByConsumer,
  getEventsByProducer,
  type ServiceName,
} from '@eventflow/shared';

/**
 * HTTP endpoint returning notification-service catalog entries.
 */
@Controller('events')
export class EventsController {
  /** Service key used when querying the shared catalog. */
  private readonly serviceName: ServiceName = 'notification-service';

  /**
   * Aggregates producer and consumer event lists from `@eventflow/shared`.
   *
   * @returns `service`, `produces`, and `consumes` arrays.
   */
  @Get('catalog')
  getCatalog() {
    return {
      service: this.serviceName,
      produces: getEventsByProducer(this.serviceName),
      consumes: getEventsByConsumer(this.serviceName),
    };
  }
}

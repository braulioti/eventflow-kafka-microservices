/**
 * @file events.controller.ts
 * @module order-service — event catalog HTTP API
 *
 * Exposes which Kafka events order-service **produces** vs **consumes**, sourced
 * from the shared catalog in `@eventflow/shared` (see repo `docs/EVENT_CATALOG.md`).
 *
 * ## order-service contract (summary)
 *
 * - **Produces**: `order.created` on `order.events`
 * - **Consumes**: `payment.failed`, `stock.released`, `stock.failed`, `notification.sent`
 */
import { Controller, Get } from '@nestjs/common';
import {
  getEventsByConsumer,
  getEventsByProducer,
  type ServiceName,
} from '@eventflow/shared';

/**
 * `GET /events/catalog` — discovery for integrators and demos.
 */
@Controller('events')
export class EventsController {
  private readonly serviceName: ServiceName = 'order-service';

  /**
   * @returns Producer and consumer slices of the shared event catalog
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

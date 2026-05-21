/**
 * @file events.controller.ts
 * @module payment-service — event catalog HTTP API
 *
 * Read-only discovery endpoint backed by the shared `EVENT_CATALOG` in
 * `@eventflow/shared`. Helps document which topics and event types this
 * service produces vs consumes without reading source code.
 *
 * ## payment-service in the saga
 *
 * **Produces**: `payment.requested`, `payment.processed`, `payment.failed`
 * **Consumes**: `order.created` (via `order.events`), `order.cancelled`
 */
import { Controller, Get } from '@nestjs/common';
import {
  getEventsByConsumer,
  getEventsByProducer,
  type ServiceName,
} from '@eventflow/shared';

/**
 * Exposes `GET /events/catalog` for payment-service Kafka contract discovery.
 */
@Controller('events')
export class EventsController {
  private readonly serviceName: ServiceName = 'payment-service';

  /**
   * Returns producer and consumer event definitions for this service.
   *
   * @returns Service name plus arrays from shared catalog helpers
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

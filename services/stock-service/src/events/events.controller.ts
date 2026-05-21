/**
 * Stock Service — Event Catalog HTTP Controller
 *
 * Surfaces the authoritative produce/consume contract for `stock-service` by
 * delegating to the shared `@eventflow/shared` event catalog. Operators and
 * integration tests use this endpoint to verify topic wiring without reading
 * source code.
 *
 * ## Routes
 *
 * | Method | Path              | Purpose                                      |
 * |--------|-------------------|----------------------------------------------|
 * | GET    | `/events/catalog` | Lists events this service produces/consumes  |
 *
 * @module stock-service/events/events.controller
 * @see {@link getEventsByProducer} Shared catalog lookup by producer
 * @see {@link getEventsByConsumer} Shared catalog lookup by consumer
 */
import { Controller, Get } from '@nestjs/common';
import {
  getEventsByConsumer,
  getEventsByProducer,
  type ServiceName,
} from '@eventflow/shared';

/**
 * HTTP controller exposing the stock-service slice of the system event catalog.
 */
@Controller('events')
export class EventsController {
  /** Fixed service identifier used for catalog queries. */
  private readonly serviceName: ServiceName = 'stock-service';

  /**
   * Returns produce and consume lists for this service from the shared catalog.
   *
   * Response shape:
   * - `service` — canonical service name
   * - `produces` — event types published by stock-service
   * - `consumes` — event types handled by stock-service Kafka consumers
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

/**
 * DLQ Service — Event Catalog HTTP Controller
 *
 * Extends the standard per-service catalog response with system-wide context:
 * the full {@link EVENT_CATALOG}, all {@link ALL_DLQ_TOPICS}, and this service's
 * produce/consume lists (DLQ service typically produces nothing).
 *
 * Intended for demos, runbooks, and operators tracing which DLQ topic corresponds
 * to which primary event type.
 *
 * ## Routes
 *
 * | Method | Path              | Purpose                                        |
 * |--------|-------------------|------------------------------------------------|
 * | GET    | `/events/catalog` | Service catalog + DLQ topics + full system map |
 *
 * @module dlq-service/events/events.controller
 */
import { Controller, Get } from '@nestjs/common';
import {
  ALL_DLQ_TOPICS,
  EVENT_CATALOG,
  getEventsByConsumer,
  getEventsByProducer,
  type ServiceName,
} from '@eventflow/shared';

/**
 * HTTP controller returning enriched catalog data for DLQ operations.
 */
@Controller('events')
export class EventsController {
  /** Service key for catalog slice lookups. */
  private readonly serviceName: ServiceName = 'dlq-service';

  /**
   * Returns service catalog entries plus global DLQ topic list and full event map.
   *
   * Response fields:
   * - `service`, `produces`, `consumes` — same as other services
   * - `dlqTopics` — ordered list of `*.dlq` Kafka topic names
   * - `systemCatalog` — complete cross-service event definitions
   */
  @Get('catalog')
  getCatalog() {
    return {
      service: this.serviceName,
      produces: getEventsByProducer(this.serviceName),
      consumes: getEventsByConsumer(this.serviceName),
      dlqTopics: ALL_DLQ_TOPICS,
      systemCatalog: EVENT_CATALOG,
    };
  }
}

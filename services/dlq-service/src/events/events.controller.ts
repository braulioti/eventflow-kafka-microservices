/** Full system catalog and DLQ topic list for operators and demos. */
import { Controller, Get } from '@nestjs/common';
import {
  ALL_DLQ_TOPICS,
  EVENT_CATALOG,
  getEventsByConsumer,
  getEventsByProducer,
  type ServiceName,
} from '@eventflow/shared';

@Controller('events')
export class EventsController {
  private readonly serviceName: ServiceName = 'dlq-service';

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

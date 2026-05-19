/** Exposes this service's slice of the shared {@link EVENT_CATALOG} for discovery. */
import { Controller, Get } from '@nestjs/common';
import {
  getEventsByConsumer,
  getEventsByProducer,
  type ServiceName,
} from '@eventflow/shared';

@Controller('events')
export class EventsController {
  private readonly serviceName: ServiceName = 'order-service';

  @Get('catalog')
  getCatalog() {
    return {
      service: this.serviceName,
      produces: getEventsByProducer(this.serviceName),
      consumes: getEventsByConsumer(this.serviceName),
    };
  }
}

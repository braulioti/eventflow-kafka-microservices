/** Exposes notification-service produce/consume catalog entries from shared package. */
import { Controller, Get } from '@nestjs/common';
import {
  getEventsByConsumer,
  getEventsByProducer,
  type ServiceName,
} from '@eventflow/shared';

@Controller('events')
export class EventsController {
  private readonly serviceName: ServiceName = 'notification-service';

  @Get('catalog')
  getCatalog() {
    return {
      service: this.serviceName,
      produces: getEventsByProducer(this.serviceName),
      consumes: getEventsByConsumer(this.serviceName),
    };
  }
}

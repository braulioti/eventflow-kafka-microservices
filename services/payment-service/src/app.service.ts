import { Injectable } from '@nestjs/common';

@Injectable()
export class AppService {
  private readonly serviceName = 'payment-service';

  getInfo() {
    return {
      service: this.serviceName,
      status: 'running',
    };
  }

  getHealth() {
    return {
      service: this.serviceName,
      status: 'ok',
    };
  }
}

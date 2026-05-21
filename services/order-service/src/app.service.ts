/**
 * @file app.service.ts
 * @module order-service — HTTP metadata provider
 *
 * Stateless helper for `AppController`; no database or Kafka access.
 */
import { Injectable } from '@nestjs/common';

/**
 * Supplies static identity strings for operational HTTP routes.
 */
@Injectable()
export class AppService {
  private readonly serviceName = 'order-service';

  /**
   * Descriptor for `GET /`.
   */
  getInfo() {
    return {
      service: this.serviceName,
      status: 'running',
    };
  }

  /**
   * Payload for `GET /health`.
   */
  getHealth() {
    return {
      service: this.serviceName,
      status: 'ok',
    };
  }
}

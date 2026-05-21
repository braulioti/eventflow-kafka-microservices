/**
 * @file app.service.ts
 * @module payment-service — HTTP metadata provider
 *
 * Stateless helper for `AppController`. Does not touch Kafka, SQLite, or payment rules.
 */
import { Injectable } from '@nestjs/common';

/**
 * Supplies static service identity for root and health HTTP routes.
 */
@Injectable()
export class AppService {
  private readonly serviceName = 'payment-service';

  /**
   * Basic service descriptor for `GET /`.
   *
   * @returns Service name and `running` status (process is accepting HTTP).
   */
  getInfo() {
    return {
      service: this.serviceName,
      status: 'running',
    };
  }

  /**
   * Health payload for `GET /health`.
   *
   * @returns Service name and `ok` — suitable for shallow probes.
   */
  getHealth() {
    return {
      service: this.serviceName,
      status: 'ok',
    };
  }
}

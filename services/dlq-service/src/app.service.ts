/**
 * DLQ Service — Application Metadata Provider
 *
 * Static responses for HTTP root and health routes. The DLQ service does not
 * expose business APIs beyond catalog discovery.
 *
 * @module dlq-service/app.service
 */
import { Injectable } from '@nestjs/common';

/**
 * Supplies service identity payloads for HTTP operational endpoints.
 */
@Injectable()
export class AppService {
  /** Fixed service identifier returned in HTTP JSON bodies. */
  private readonly serviceName = 'dlq-service';

  /**
   * Root endpoint metadata (`GET /`).
   *
   * @returns Service name and running status.
   */
  getInfo() {
    return {
      service: this.serviceName,
      status: 'running',
    };
  }

  /**
   * Health check metadata (`GET /health`).
   *
   * @returns Service name and ok status.
   */
  getHealth() {
    return {
      service: this.serviceName,
      status: 'ok',
    };
  }
}

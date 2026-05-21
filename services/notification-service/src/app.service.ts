/**
 * Notification Service — Application Metadata Provider
 *
 * Returns static JSON for HTTP root and health endpoints. Keeps Kafka paths
 * separate from probe traffic so orchestrators can health-check without
 * consuming topics.
 *
 * @module notification-service/app.service
 */
import { Injectable } from '@nestjs/common';

/**
 * Injectable provider for service identity and health check payloads.
 */
@Injectable()
export class AppService {
  /** Canonical service name included in every HTTP response body. */
  private readonly serviceName = 'notification-service';

  /**
   * Service discovery payload for `GET /`.
   *
   * @returns Service name and `running` status.
   */
  getInfo() {
    return {
      service: this.serviceName,
      status: 'running',
    };
  }

  /**
   * Health probe payload for `GET /health`.
   *
   * @returns Service name and `ok` status.
   */
  getHealth() {
    return {
      service: this.serviceName,
      status: 'ok',
    };
  }
}

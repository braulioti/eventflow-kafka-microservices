/**
 * Stock Service — Application Metadata Provider
 *
 * Supplies lightweight JSON payloads for the root (`GET /`) and health
 * (`GET /health`) HTTP endpoints. These endpoints support load balancers,
 * Kubernetes probes, and local development smoke checks without touching Kafka.
 *
 * @module stock-service/app.service
 */
import { Injectable } from '@nestjs/common';

/**
 * Injectable service returning static identity and health status for HTTP probes.
 */
@Injectable()
export class AppService {
  /** Canonical service name echoed in all HTTP responses. */
  private readonly serviceName = 'stock-service';

  /**
   * Returns basic service identity for the root endpoint.
   *
   * @returns Object with `service` name and `status: 'running'`.
   */
  getInfo() {
    return {
      service: this.serviceName,
      status: 'running',
    };
  }

  /**
   * Returns a minimal health payload for orchestration probes.
   *
   * @returns Object with `service` name and `status: 'ok'`.
   */
  getHealth() {
    return {
      service: this.serviceName,
      status: 'ok',
    };
  }
}

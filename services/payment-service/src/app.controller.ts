/**
 * @file app.controller.ts
 * @module payment-service — HTTP surface (non-domain)
 *
 * Exposes operational endpoints only. Payment business logic and Kafka handling
 * live in `PaymentService` and `PaymentEventsConsumer` respectively.
 */
import { Controller, Get } from '@nestjs/common';
import { AppService } from './app.service';

/**
 * Root HTTP controller for liveness and service identification.
 *
 * Used by load balancers, Docker health checks, and local development
 * to confirm the process is up — independent of Kafka broker availability.
 */
@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  /**
   * Returns service name and a coarse running status.
   *
   * @returns `{ service, status }` from {@link AppService.getInfo}
   */
  @Get()
  getRoot() {
    return this.appService.getInfo();
  }

  /**
   * Lightweight health probe (no dependency checks).
   *
   * @returns `{ service, status: 'ok' }` from {@link AppService.getHealth}
   */
  @Get('health')
  health() {
    return this.appService.getHealth();
  }
}

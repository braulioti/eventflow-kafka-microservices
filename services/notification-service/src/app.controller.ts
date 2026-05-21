/**
 * Notification Service — HTTP Root Controller
 *
 * Maps standard operational routes to {@link AppService}. The HTTP server runs
 * alongside the Kafka microservice started in `main.ts`.
 *
 * ## Routes
 *
 * | Method | Path      | Handler   | Purpose                 |
 * |--------|-----------|-----------|-------------------------|
 * | GET    | `/`       | `getRoot` | Service metadata        |
 * | GET    | `/health` | `health`  | Liveness/readiness      |
 *
 * @module notification-service/app.controller
 */
import { Controller, Get } from '@nestjs/common';
import { AppService } from './app.service';

/**
 * HTTP controller for root and health endpoints.
 */
@Controller()
export class AppController {
  /**
   * @param appService - Metadata provider for info and health responses.
   */
  constructor(private readonly appService: AppService) {}

  /** Returns service identity (`GET /`). */
  @Get()
  getRoot() {
    return this.appService.getInfo();
  }

  /** Returns health status (`GET /health`). */
  @Get('health')
  health() {
    return this.appService.getHealth();
  }
}

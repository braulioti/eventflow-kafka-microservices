/**
 * DLQ Service — HTTP Root Controller
 *
 * Standard NestJS controller for service discovery and health probes on the
 * DLQ observability process.
 *
 * ## Routes
 *
 * | Method | Path      | Handler   | Purpose              |
 * |--------|-----------|-----------|----------------------|
 * | GET    | `/`       | `getRoot` | Service metadata     |
 * | GET    | `/health` | `health`  | Health probe         |
 *
 * @module dlq-service/app.controller
 */
import { Controller, Get } from '@nestjs/common';
import { AppService } from './app.service';

/**
 * Maps HTTP GET routes to {@link AppService}.
 */
@Controller()
export class AppController {
  /**
   * @param appService - Provider for info and health JSON bodies.
   */
  constructor(private readonly appService: AppService) {}

  /** Service info (`GET /`). */
  @Get()
  getRoot() {
    return this.appService.getInfo();
  }

  /** Health status (`GET /health`). */
  @Get('health')
  health() {
    return this.appService.getHealth();
  }
}

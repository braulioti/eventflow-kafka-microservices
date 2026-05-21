/**
 * Stock Service — HTTP Root Controller
 *
 * Exposes operational endpoints on the main HTTP listener (distinct from the
 * Kafka microservice transport). Used for discovery, health checks, and quick
 * manual verification that the process is alive.
 *
 * ## Routes
 *
 * | Method | Path     | Handler    | Purpose                    |
 * |--------|----------|------------|----------------------------|
 * | GET    | `/`      | `getRoot`  | Service name and status    |
 * | GET    | `/health`| `health`   | Liveness/readiness probe   |
 *
 * @module stock-service/app.controller
 */
import { Controller, Get } from '@nestjs/common';
import { AppService } from './app.service';

/**
 * Maps HTTP GET routes to {@link AppService} metadata methods.
 */
@Controller()
export class AppController {
  /**
   * @param appService - Provider for service info and health payloads.
   */
  constructor(private readonly appService: AppService) {}

  /**
   * Root endpoint returning service identity (`GET /`).
   */
  @Get()
  getRoot() {
    return this.appService.getInfo();
  }

  /**
   * Health check endpoint for probes and monitoring (`GET /health`).
   */
  @Get('health')
  health() {
    return this.appService.getHealth();
  }
}

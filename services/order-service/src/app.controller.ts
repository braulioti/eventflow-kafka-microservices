/**
 * @file app.controller.ts
 * @module order-service — HTTP operational endpoints
 *
 * Non-domain routes for process health. Order creation lives under `OrdersController`.
 */
import { Controller, Get } from '@nestjs/common';
import { AppService } from './app.service';

/**
 * Root HTTP controller for service identification and health checks.
 */
@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  /**
   * @returns Service metadata from {@link AppService.getInfo}
   */
  @Get()
  getRoot() {
    return this.appService.getInfo();
  }

  /**
   * @returns Health payload from {@link AppService.getHealth}
   */
  @Get('health')
  health() {
    return this.appService.getHealth();
  }
}

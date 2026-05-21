/**
 * @file orders.controller.ts
 * @module order-service — orders REST API
 *
 * HTTP entry point that starts the EventFlow saga. `POST /orders` validates
 * the body, persists to SQLite, and triggers `order.created` publication.
 *
 * Downstream services (payment, stock, notification) subscribe to Kafka —
 * they are not invoked synchronously from this controller.
 *
 * @see CreateOrderDto
 * @see OrdersService.createOrder
 */
import { Body, Controller, HttpCode, HttpStatus, Post } from '@nestjs/common';
import { CreateOrderDto } from './dto/create-order.dto';
import { OrdersService } from './orders.service';

/**
 * REST surface for creating orders (`/orders`).
 */
@Controller('orders')
export class OrdersController {
  constructor(private readonly ordersService: OrdersService) {}

  /**
   * Creates a new order and publishes `order.created` when publish succeeds.
   *
   * @param dto - Validated request body (global ValidationPipe)
   * @returns Created order summary including `eventId` when published
   */
  @Post()
  @HttpCode(HttpStatus.CREATED)
  createOrder(@Body() dto: CreateOrderDto) {
    return this.ordersService.createOrder(dto);
  }
}

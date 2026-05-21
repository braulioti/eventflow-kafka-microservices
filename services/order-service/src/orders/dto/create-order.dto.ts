/**
 * @file create-order.dto.ts
 * @module order-service — REST request validation
 *
 * Defines the `POST /orders` body schema validated by the global
 * `ValidationPipe` in `main.ts` (`whitelist`, `forbidNonWhitelisted`, `transform`).
 *
 * Field rules mirror constraints expected in `order.created` Kafka payloads so
 * invalid orders are rejected before SQLite insert and publish.
 *
 * @see OrdersController.createOrder
 * @see OrdersService.createOrder
 */
import { Type } from 'class-transformer';
import {
  ArrayMinSize,
  IsArray,
  IsIn,
  IsInt,
  IsNumber,
  IsOptional,
  IsPositive,
  IsString,
  Min,
  MinLength,
  ValidateNested,
} from 'class-validator';

/** ISO currency codes allowed on create — aligned with shared `CurrencyCode`. */
const CURRENCIES = ['BRL', 'USD', 'EUR'] as const;

/**
 * One line item in the create-order request (maps to `OrderItemEntity`).
 */
export class OrderItemDto {
  /** Product sku/id — non-empty string. */
  @IsString()
  @MinLength(1)
  productId!: string;

  /** Units ordered — integer ≥ 1. */
  @IsInt()
  @Min(1)
  quantity!: number;

  /** Unit price — positive number, max 2 decimal places. */
  @IsNumber({ maxDecimalPlaces: 2 })
  @IsPositive()
  unitPrice!: number;
}

/**
 * Request body for `POST /orders`.
 *
 * Requires at least one nested `OrderItemDto`; optional `currency` defaults
 * to `BRL` in {@link OrdersService.createOrder}.
 */
export class CreateOrderDto {
  /** Customer identifier — opaque string from caller. */
  @IsString()
  @MinLength(1)
  customerId!: string;

  /** Non-empty array of line items. */
  @IsArray()
  @ArrayMinSize(1)
  @ValidateNested({ each: true })
  @Type(() => OrderItemDto)
  items!: OrderItemDto[];

  /** Optional currency; must be one of {@link CURRENCIES} when provided. */
  @IsOptional()
  @IsIn(CURRENCIES)
  currency?: (typeof CURRENCIES)[number];
}

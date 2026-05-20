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

/** Allowed currencies aligned with @eventflow/shared CurrencyCode. */
const CURRENCIES = ['BRL', 'USD', 'EUR'] as const;

/** Single line item in POST /orders body. */
export class OrderItemDto {
  @IsString()
  @MinLength(1)
  productId!: string;

  @IsInt()
  @Min(1)
  quantity!: number;

  @IsNumber({ maxDecimalPlaces: 2 })
  @IsPositive()
  unitPrice!: number;
}

/** Request body for POST /orders — validated by global ValidationPipe. */
export class CreateOrderDto {
  @IsString()
  @MinLength(1)
  customerId!: string;

  @IsArray()
  @ArrayMinSize(1)
  @ValidateNested({ each: true })
  @Type(() => OrderItemDto)
  items!: OrderItemDto[];

  @IsOptional()
  @IsIn(CURRENCIES)
  currency?: (typeof CURRENCIES)[number];
}

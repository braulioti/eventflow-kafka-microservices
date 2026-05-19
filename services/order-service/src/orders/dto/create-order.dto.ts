/** Request body for POST /orders (validation can be extended with class-validator). */
export class OrderItemDto {
  productId!: string;
  quantity!: number;
  unitPrice!: number;
}

export class CreateOrderDto {
  customerId!: string;
  items!: OrderItemDto[];
}

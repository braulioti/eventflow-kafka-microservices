/** Line item shared by order and stock events. */
export interface OrderItem {
  productId: string;
  quantity: number;
  unitPrice: number;
}

/** Supported ISO currency codes for monetary fields. */
export type CurrencyCode = 'BRL' | 'USD' | 'EUR';

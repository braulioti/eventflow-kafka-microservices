/**
 * Domain payload types barrel.
 *
 * Each file models one bounded context (order, payment, stock, notification); together
 * they populate {@link EventPayloadMap} for compile-time type-safe envelopes.
 */
export * from './common';
export * from './order.events';
export * from './payment.events';
export * from './stock.events';
export * from './notification.events';

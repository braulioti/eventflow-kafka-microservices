/**
 * Domain event module barrel.
 *
 * Re-exports envelope builders, validators, {@link EVENT_CATALOG}, typed payload map,
 * and idempotency helpers used by order, payment, stock, notification, and dlq services.
 */
export * from './event-types';
export * from './core-events';
export * from './envelope-spec';
export * from './domain-topics';
export * from './resolve-kafka-topic';
export * from './idempotency';
export * from './kafka-topics';
export * from './envelope';
export * from './create-envelope';
export * from './validate-envelope';
export * from './validate-order-created';
export * from './validate-payment-events';
export * from './event-failure';
export * from './event-map';
export * from './event-catalog';
export * from './payloads';

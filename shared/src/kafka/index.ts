/**
 * Kafka integration module barrel.
 *
 * Re-exports Nest/kafkajs configuration, wire-format envelope helpers, domain-topic parsers
 * (`parseOrderEventsMessage`, `parsePaymentProcessedMessage`), retry/DLQ executor, and
 * topic provisioning defaults shared across all EventFlow services.
 */
export * from './consumer-groups';
export * from './consumer-config';
export * from './kafka-brokers';
export * from './kafka-headers';
export * from './kafka-config';
export * from './deserialize-kafka-payload';
export * from './consume-order-events';
export * from './consume-payment-events';
export * from './envelope-kafka';
export * from './topic-config';
export * from './partition-key';
export * from './kafka-event-transport';
export * from './retry';
export * from './producer-retry-policy';
export * from './emit-kafka-event';

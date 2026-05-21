/**
 * @eventflow/shared — root public API for the EventFlow monorepo.
 *
 * **Purpose:** Single import surface (`@eventflow/shared`) so every microservice shares
 * the same event contracts, validation rules, and Kafka wiring without duplicating types.
 *
 * **Role in EventFlow:**
 * - **events/** — canonical `eventType` strings, envelope shape, domain payloads, catalog,
 *   runtime validators, and DLQ failure envelopes.
 * - **kafka/** — broker resolution, consumer groups, partition keys, deserialize/extract
 *   helpers, topic defaults, producer/consumer retry policies, and {@link KafkaRetryExecutor}.
 *
 * Services depend on this package for publish/consume consistency; infrastructure scripts
 * (topic creation, verification flows) reuse the same topic and catalog constants.
 */
export * from './events';
export * from './kafka';

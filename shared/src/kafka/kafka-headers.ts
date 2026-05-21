/**
 * Canonical Kafka record headers mirroring {@link EventEnvelope} metadata.
 *
 * Propagated on publish via {@link envelopeToKafkaHeaders} and preserved through
 * {@link mergeEnvelopeAndRetryHeaders} during consumer retries. Enables log correlation
 * and idempotent consumer dedupe without parsing the full JSON body.
 */

/** `x-*` header names attached to every EventFlow Kafka message. */
export const KafkaHeader = {
  EVENT_ID: 'x-event-id',
  EVENT_TYPE: 'x-event-type',
  CORRELATION_ID: 'x-correlation-id',
  CAUSATION_ID: 'x-causation-id',
  SOURCE: 'x-source',
  SCHEMA_VERSION: 'x-schema-version',
} as const;

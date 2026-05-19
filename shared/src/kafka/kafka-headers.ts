/** Standard Kafka headers for traceability (mirrors envelope metadata) */
export const KafkaHeader = {
  EVENT_ID: 'x-event-id',
  EVENT_TYPE: 'x-event-type',
  CORRELATION_ID: 'x-correlation-id',
  CAUSATION_ID: 'x-causation-id',
  SOURCE: 'x-source',
  SCHEMA_VERSION: 'x-schema-version',
} as const;

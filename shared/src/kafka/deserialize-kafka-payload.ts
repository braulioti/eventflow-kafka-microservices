/**
 * Normalizes raw Kafka message `value` into a parsed JavaScript value.
 *
 * Nest and kafkajs may deliver `Buffer`, UTF-8 string, or pre-parsed object depending on
 * serializer settings. Domain parsers ({@link parseOrderEventsMessage}) call this first
 * before {@link extractEnvelope}.
 */

/**
 * Parses message body to JSON (or returns already-parsed objects unchanged).
 * @param payload - Raw value from Kafka consumer / Nest context
 * @throws Error when value is empty or JSON is invalid
 */
export function deserializeKafkaPayload(payload: unknown): unknown {
  if (payload === null || payload === undefined) {
    throw new Error('Kafka message value is empty');
  }

  if (Buffer.isBuffer(payload)) {
    const text = payload.toString('utf8').trim();
    if (!text) {
      throw new Error('Kafka message Buffer is empty');
    }
    return JSON.parse(text) as unknown;
  }

  if (typeof payload === 'string') {
    const text = payload.trim();
    if (!text) {
      throw new Error('Kafka message string is empty');
    }
    return JSON.parse(text) as unknown;
  }

  return payload;
}

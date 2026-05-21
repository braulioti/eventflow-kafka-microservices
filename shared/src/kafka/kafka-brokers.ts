/**
 * Broker list resolution for all EventFlow Kafka clients.
 *
 * Reads `KAFKA_BOOTSTRAP_SERVERS` (comma-separated host:port). Defaults to local Docker
 * Compose (`localhost:9092`). Used by producer and consumer Nest modules and retry executor.
 */

/**
 * Returns trimmed broker host:port entries for kafkajs / `@nestjs/microservices` Kafka.
 * @returns Non-empty array of bootstrap servers
 */
export function resolveKafkaBrokers(): string[] {
  return (process.env.KAFKA_BOOTSTRAP_SERVERS ?? 'localhost:9092')
    .split(',')
    .map((broker) => broker.trim())
    .filter(Boolean);
}

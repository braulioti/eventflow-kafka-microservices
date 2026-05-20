import type { ServiceName } from '../events/event-catalog';
import { CONSUMER_GROUP_BY_SERVICE } from './consumer-groups';
import { resolveProducerRetryPolicy } from './producer-retry-policy';

/** Parses `KAFKA_BOOTSTRAP_SERVERS` (comma-separated) for kafkajs / Nest Kafka. */
export function resolveKafkaBrokers(): string[] {
  return (process.env.KAFKA_BOOTSTRAP_SERVERS ?? 'localhost:9092')
    .split(',')
    .map((broker) => broker.trim())
    .filter(Boolean);
}

/**
 * Producer-side KafkaJS options shared by Nest `ClientsModule` registrations.
 * `allowAutoTopicCreation` simplifies local/docker setups; disable in production.
 */
export function getKafkaClientConfig(clientId: string) {
  const producerRetry = resolveProducerRetryPolicy();

  return {
    client: {
      clientId,
      brokers: resolveKafkaBrokers(),
    },
    producer: {
      allowAutoTopicCreation: true,
      /** KafkaJS transport-level retries (backoff between broker send attempts). */
      retry: {
        retries: Math.max(producerRetry.maxAttempts - 1, 0),
        initialRetryTime: producerRetry.baseDelayMs,
        maxRetryTime: producerRetry.maxDelayMs,
        multiplier: producerRetry.backoffMultiplier,
      },
    },
  };
}

/**
 * Consumer microservice options: stable group id per service and optional replay.
 * Set `KAFKA_FROM_BEGINNING=true` only when reprocessing history intentionally.
 */
export function getKafkaConsumerConfig(service: ServiceName) {
  return {
    ...getKafkaClientConfig(service),
    consumer: {
      groupId: CONSUMER_GROUP_BY_SERVICE[service],
      allowAutoTopicCreation: true,
    },
    subscribe: {
      fromBeginning: process.env.KAFKA_FROM_BEGINNING === 'true',
    },
  };
}

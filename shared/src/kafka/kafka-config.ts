/**
 * NestJS Kafka microservice and client factory configuration.
 *
 * Builds kafkajs `client` / `consumer` / `producer` blocks from env vars and shared retry
 * policies. Each service calls {@link getKafkaConsumerConfig} or {@link getKafkaClientConfig}
 * in `main.ts` / `ClientsModule.register`.
 */
import type { ServiceName } from '../events/event-catalog';
import {
  getKafkaConsumerConnectionInfo,
  resolveConsumerClientId,
  resolveConsumerGroup,
} from './consumer-config';
import { resolveKafkaBrokers } from './kafka-brokers';
import { resolveProducerRetryPolicy } from './producer-retry-policy';

export { resolveKafkaBrokers } from './kafka-brokers';

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
 * Nest Kafka microservice options: broker connection + consumer group per service.
 *
 * - **Brokers:** `KAFKA_BOOTSTRAP_SERVERS` (comma-separated)
 * - **Group:** `resolveConsumerGroup()` — default `eventflow.<service>`
 * - **Replay:** `KAFKA_FROM_BEGINNING=true` only when reprocessing history
 */
export function getKafkaConsumerConfig(service: ServiceName) {
  const connection = getKafkaConsumerConnectionInfo(service);
  const producerRetry = resolveProducerRetryPolicy();

  return {
    client: {
      clientId: connection.clientId,
      brokers: connection.brokers,
      connectionTimeout: Number(process.env.KAFKA_CONNECTION_TIMEOUT_MS ?? 10000),
      retry: {
        retries: Math.max(producerRetry.maxAttempts, 3),
        initialRetryTime: producerRetry.baseDelayMs,
        maxRetryTime: producerRetry.maxDelayMs,
        multiplier: producerRetry.backoffMultiplier,
      },
    },
    consumer: {
      groupId: connection.groupId,
      sessionTimeout: connection.sessionTimeoutMs,
      heartbeatInterval: connection.heartbeatIntervalMs,
      allowAutoTopicCreation: true,
    },
    subscribe: {
      fromBeginning: connection.fromBeginning,
    },
    run: {
      autoCommit: true,
    },
  };
}

export { getKafkaConsumerConnectionInfo, resolveConsumerGroup } from './consumer-config';

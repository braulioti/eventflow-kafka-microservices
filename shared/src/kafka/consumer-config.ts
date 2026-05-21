/**
 * Consumer connection resolution — group ids, client ids, session/heartbeat tuning.
 *
 * Centralizes env-based overrides (`KAFKA_CONSUMER_GROUP_*`, `KAFKA_FROM_BEGINNING`) so
 * each Nest service logs the same bootstrap line via {@link formatKafkaConsumerBootstrap}.
 */
import type { ServiceName } from '../events/event-catalog';
import { CONSUMER_GROUP_BY_SERVICE } from './consumer-groups';
import { resolveKafkaBrokers } from './kafka-brokers';

/** Snapshot of how a service will connect its Kafka consumer (logging / tests). */
export interface KafkaConsumerConnectionInfo {
  service: ServiceName;
  clientId: string;
  brokers: string[];
  groupId: string;
  fromBeginning: boolean;
  sessionTimeoutMs: number;
  heartbeatIntervalMs: number;
}

/** Env key per service, e.g. `KAFKA_CONSUMER_GROUP_ORDER_SERVICE`. */
function consumerGroupEnvKey(service: ServiceName): string {
  return `KAFKA_CONSUMER_GROUP_${service.toUpperCase().replace(/-/g, '_')}`;
}

/**
 * Resolves the Kafka consumer group for a service.
 * Override via `KAFKA_CONSUMER_GROUP_<SERVICE>` or append `KAFKA_CONSUMER_GROUP_SUFFIX`.
 */
export function resolveConsumerGroup(service: ServiceName): string {
  const explicit = process.env[consumerGroupEnvKey(service)];
  if (explicit?.trim()) {
    return explicit.trim();
  }

  const base = CONSUMER_GROUP_BY_SERVICE[service];
  const suffix = process.env.KAFKA_CONSUMER_GROUP_SUFFIX?.trim();
  return suffix ? `${base}${suffix}` : base;
}

/**
 * Kafka client id for consumer connections (broker-side connection labeling).
 * Override with `KAFKA_CONSUMER_CLIENT_ID_<SERVICE>` or `KAFKA_CONSUMER_CLIENT_ID_SUFFIX`.
 * When scaling horizontally, Docker/Podman set `HOSTNAME` per container — we append it
 * so brokers can distinguish multiple consumers in the same group.
 */
export function resolveConsumerClientId(service: ServiceName): string {
  const explicit =
    process.env[`KAFKA_CONSUMER_CLIENT_ID_${service.toUpperCase().replace(/-/g, '_')}`]?.trim();
  if (explicit) {
    return explicit;
  }

  const base = `${service}-consumer`;
  const suffix =
    process.env.KAFKA_CONSUMER_CLIENT_ID_SUFFIX?.trim() ??
    process.env.HOSTNAME?.trim();
  return suffix ? `${base}-${suffix}` : base;
}

function resolveConsumerSessionTimeoutMs(): number {
  return Number(process.env.KAFKA_CONSUMER_SESSION_TIMEOUT_MS ?? 30000);
}

function resolveConsumerHeartbeatIntervalMs(): number {
  return Number(process.env.KAFKA_CONSUMER_HEARTBEAT_INTERVAL_MS ?? 3000);
}

/** Human-readable line for service startup logs. */
export function formatKafkaConsumerBootstrap(
  info: KafkaConsumerConnectionInfo,
): string {
  return [
    `Kafka consumer: service=${info.service}`,
    `brokers=[${info.brokers.join(', ')}]`,
    `groupId=${info.groupId}`,
    `clientId=${info.clientId}`,
    `fromBeginning=${info.fromBeginning}`,
  ].join(' ');
}

/** Snapshot of consumer wiring (brokers + group) for logging and tests. */
export function getKafkaConsumerConnectionInfo(
  service: ServiceName,
): KafkaConsumerConnectionInfo {
  const groupId = resolveConsumerGroup(service);
  const clientId = resolveConsumerClientId(service);
  const brokers = resolveKafkaBrokers();

  return {
    service,
    clientId,
    brokers,
    groupId,
    fromBeginning: process.env.KAFKA_FROM_BEGINNING === 'true',
    sessionTimeoutMs: resolveConsumerSessionTimeoutMs(),
    heartbeatIntervalMs: resolveConsumerHeartbeatIntervalMs(),
  };
}

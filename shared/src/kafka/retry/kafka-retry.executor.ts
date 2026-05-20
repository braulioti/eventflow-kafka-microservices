/**
 * Orchestrates consumer retries: backoff, republish to the same topic, then DLQ.
 *
 * Flow: handler throws → increment retry count → sleep/backoff → republish with
 * retry headers, or after max attempts wrap in {@link createFailureEnvelope} and
 * send to `{topic}.dlq` via {@link KafkaEventTransport.publishToDlq}.
 */
import type { ServiceName } from '../../events/event-catalog';
import { createFailureEnvelope } from '../../events/event-failure';
import type { EventEnvelope } from '../../events/envelope';
import { envelopeToKafkaHeaders } from '../envelope-kafka';
import type { KafkaEventTransport } from '../kafka-event-transport';
import type { EventTypeValue } from '../../events/event-types';
import { resolveKafkaTopic } from '../../events/resolve-kafka-topic';
import {
  buildRetryHeaders,
  getRetryAt,
  getRetryCount,
} from './retry-headers';
import {
  calculateBackoffMs,
  type RetryPolicyConfig,
  resolveRetryPolicy,
  shouldRetry,
  shouldSendToDlq,
  sleep,
} from './retry-policy';

/** Result of a single {@link KafkaRetryExecutor.execute} invocation. */
export type RetryOutcome = 'success' | 'retry' | 'dlq';

/** Parameters for running domain logic with shared retry/DLQ behavior. */
export interface ExecuteWithRetryParams {
  /** Canonical event type (used to resolve Kafka topic and DLQ routing). */
  eventType: EventTypeValue;
  envelope: EventEnvelope<unknown>;
  headers?: Record<string, string>;
  handler: () => Promise<void>;
}

/** Shared retry engine; each Nest service wraps this in {@link KafkaRetryRunner}. */
export class KafkaRetryExecutor {
  private readonly policy: RetryPolicyConfig;

  constructor(
    private readonly transport: KafkaEventTransport,
    private readonly service: ServiceName,
    policy?: RetryPolicyConfig,
  ) {
    this.policy = policy ?? resolveRetryPolicy();
  }

  async execute(params: ExecuteWithRetryParams): Promise<RetryOutcome> {
    const headers = params.headers ?? {};
    const retryAt = getRetryAt(headers);

    // Honor scheduled retry time from a previous republish (x-retry-at header).
    if (retryAt && Date.now() < retryAt) {
      await sleep(retryAt - Date.now());
    }

    try {
      await params.handler();
      return 'success';
    } catch (error) {
      const currentAttempt = getRetryCount(headers);
      const nextAttempt = currentAttempt + 1;

      if (shouldSendToDlq(nextAttempt, this.policy)) {
        await this.sendToDlq(params.eventType, params.envelope, error, nextAttempt);
        return 'dlq';
      }

      if (shouldRetry(nextAttempt, this.policy)) {
        await this.scheduleRetry(
          params.eventType,
          params.envelope,
          headers,
          nextAttempt,
        );
        return 'retry';
      }

      await this.sendToDlq(params.eventType, params.envelope, error, nextAttempt);
      return 'dlq';
    }
  }

  private async scheduleRetry(
    eventType: EventTypeValue,
    envelope: EventEnvelope<unknown>,
    headers: Record<string, string>,
    nextAttempt: number,
  ): Promise<void> {
    const kafkaTopic = resolveKafkaTopic(eventType);
    const backoffMs = calculateBackoffMs(nextAttempt, this.policy);
    const retryAt = Date.now() + backoffMs;

    await sleep(backoffMs);

    const envelopeHeaders = envelopeToKafkaHeaders(envelope);
    const retryHeaders = buildRetryHeaders({
      retryCount: nextAttempt,
      maxAttempts: this.policy.maxAttempts,
      retryAt,
      originalTopic: kafkaTopic,
      envelopeHeaders,
    });

    await this.transport.publish(eventType, envelope, { headers: retryHeaders });
  }

  private async sendToDlq(
    eventType: EventTypeValue,
    envelope: EventEnvelope<unknown>,
    error: unknown,
    attempt: number,
  ): Promise<void> {
    const kafkaTopic = resolveKafkaTopic(eventType);
    const failureEnvelope = createFailureEnvelope({
      original: envelope,
      originalTopic: kafkaTopic,
      service: this.service,
      error,
      attempt,
      policy: this.policy,
    });

    await this.transport.publishToDlq(eventType, failureEnvelope);
  }
}

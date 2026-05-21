/**
 * @file kafka-retry.runner.ts
 * @module payment-service — consumer retry adapter
 *
 * Thin NestJS injectable that delegates to the shared {@link KafkaRetryExecutor}.
 * Wraps payment consumer handlers so transient failures are retried with backoff
 * and exhausted attempts produce DLQ envelopes via `EventPublisher.publishToDlq`.
 *
 * ## Retry policy
 *
 * Resolved at construction from `resolveConsumerRetryPolicy()` (env-driven in
 * `@eventflow/shared`). Logged once on module init for operator visibility.
 *
 * ## Usage in payment flow
 *
 * `PaymentEventsConsumer` calls `execute()` around `PaymentService` methods so
 * `order.created` and `order.cancelled` share the same retry semantics as other
 * EventFlow services.
 *
 * @see PaymentEventsConsumer
 * @see EventPublisher.publishToDlq
 */
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import {
  ExecuteWithRetryParams,
  KafkaRetryExecutor,
  RetryOutcome,
  formatConsumerRetryPolicy,
  resolveConsumerRetryPolicy,
} from '@eventflow/shared';
import { EventPublisher } from './event-publisher.service';

/**
 * Service-scoped wrapper for shared Kafka consumer retry + DLQ publishing.
 */
@Injectable()
export class KafkaRetryRunner implements OnModuleInit {
  private readonly logger = new Logger(KafkaRetryRunner.name);
  private readonly executor: KafkaRetryExecutor;

  /**
   * Builds the executor with payment-service identity and Nest log sinks.
   *
   * @param publisher - Used to emit retry scheduling headers and DLQ payloads
   */
  constructor(publisher: EventPublisher) {
    const policy = resolveConsumerRetryPolicy();
    this.executor = new KafkaRetryExecutor(publisher, 'payment-service', {
      policy,
      log: (message) => this.logger.log(message),
      logWarn: (message) => this.logger.warn(message),
      logError: (message) => this.logger.error(message),
    });
  }

  /** Logs the effective consumer retry policy at startup. */
  onModuleInit(): void {
    this.logger.log(
      `[KAFKA CONSUMER RETRY] service=payment-service ${formatConsumerRetryPolicy(this.executor.getPolicy())}`,
    );
  }

  /**
   * Runs the handler with retry/DLQ behavior per shared executor rules.
   *
   * @param params - Event type, envelope, headers, and async handler
   * @returns Outcome describing success, retry scheduled, or DLQ sent
   */
  execute(params: ExecuteWithRetryParams): Promise<RetryOutcome> {
    return this.executor.execute(params);
  }
}

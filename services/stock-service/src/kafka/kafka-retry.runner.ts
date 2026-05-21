/**
 * Stock Service — Kafka Consumer Retry Runner
 *
 * Thin NestJS façade over the shared {@link KafkaRetryExecutor}. Consumer
 * handlers delegate transient failures to this runner, which applies the
 * environment-driven retry policy (backoff, max attempts) and publishes to DLQ
 * when retries are exhausted.
 *
 * Logs the resolved policy at startup so operators can confirm configuration
 * without inspecting environment variables directly.
 *
 * @module stock-service/kafka/kafka-retry.runner
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
 * Injectable wrapper exposing `execute` for Kafka event handlers in stock-service.
 */
@Injectable()
export class KafkaRetryRunner implements OnModuleInit {
  private readonly logger = new Logger(KafkaRetryRunner.name);
  private readonly executor: KafkaRetryExecutor;

  /**
   * Builds a {@link KafkaRetryExecutor} bound to this service's {@link EventPublisher}.
   *
   * @param publisher - DLQ-capable transport used when retries are exhausted.
   */
  constructor(publisher: EventPublisher) {
    const policy = resolveConsumerRetryPolicy();
    this.executor = new KafkaRetryExecutor(publisher, 'stock-service', {
      policy,
      log: (message) => this.logger.log(message),
      logWarn: (message) => this.logger.warn(message),
      logError: (message) => this.logger.error(message),
    });
  }

  /** Logs the active consumer retry policy once the module is initialized. */
  onModuleInit(): void {
    this.logger.log(
      `[KAFKA CONSUMER RETRY] service=stock-service ${formatConsumerRetryPolicy(this.executor.getPolicy())}`,
    );
  }

  /**
   * Runs a handler with retry/DLQ semantics defined by the shared executor.
   *
   * @param params - Event type, envelope, headers, and async handler callback.
   * @returns Outcome describing success, retry scheduled, DLQ, or skip.
   */
  execute(params: ExecuteWithRetryParams): Promise<RetryOutcome> {
    return this.executor.execute(params);
  }
}

/**
 * Notification Service — Kafka Consumer Retry Runner
 *
 * Delegates consumer handler execution to the shared {@link KafkaRetryExecutor}
 * with notification-service identity and logging hooks. Ensures transient errors
 * follow the platform retry policy before messages land on DLQ topics.
 *
 * @module notification-service/kafka/kafka-retry.runner
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
 * Nest injectable exposing retry/DLQ execution for Kafka consumers.
 */
@Injectable()
export class KafkaRetryRunner implements OnModuleInit {
  private readonly logger = new Logger(KafkaRetryRunner.name);
  private readonly executor: KafkaRetryExecutor;

  /**
   * @param publisher - Event publisher used for DLQ emission on retry exhaustion.
   */
  constructor(publisher: EventPublisher) {
    const policy = resolveConsumerRetryPolicy();
    this.executor = new KafkaRetryExecutor(publisher, 'notification-service', {
      policy,
      log: (message) => this.logger.log(message),
      logWarn: (message) => this.logger.warn(message),
      logError: (message) => this.logger.error(message),
    });
  }

  /** Logs resolved retry policy at startup for operational visibility. */
  onModuleInit(): void {
    this.logger.log(
      `[KAFKA CONSUMER RETRY] service=notification-service ${formatConsumerRetryPolicy(this.executor.getPolicy())}`,
    );
  }

  /**
   * Executes a handler with platform retry and DLQ semantics.
   *
   * @param params - Event metadata, headers, and async handler.
   * @returns {@link RetryOutcome} describing how the message was handled.
   */
  execute(params: ExecuteWithRetryParams): Promise<RetryOutcome> {
    return this.executor.execute(params);
  }
}

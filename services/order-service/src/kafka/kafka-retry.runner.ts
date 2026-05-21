/**
 * @file kafka-retry.runner.ts
 * @module order-service — consumer retry adapter
 *
 * Nest injectable wrapping shared {@link KafkaRetryExecutor} with order-service
 * identity. Used by `OrderEventsConsumer` when updating order status from
 * payment, stock, or notification events.
 *
 * On retry exhaustion, failure envelopes go to DLQ via `EventPublisher.publishToDlq`.
 *
 * @see OrderEventsConsumer
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
 * order-service scoped consumer retry + DLQ helper.
 */
@Injectable()
export class KafkaRetryRunner implements OnModuleInit {
  private readonly logger = new Logger(KafkaRetryRunner.name);
  private readonly executor: KafkaRetryExecutor;

  /**
   * @param publisher - DLQ and retry header publisher
   */
  constructor(publisher: EventPublisher) {
    const policy = resolveConsumerRetryPolicy();
    this.executor = new KafkaRetryExecutor(publisher, 'order-service', {
      policy,
      log: (message) => this.logger.log(message),
      logWarn: (message) => this.logger.warn(message),
      logError: (message) => this.logger.error(message),
    });
  }

  /** Logs resolved retry policy at startup. */
  onModuleInit(): void {
    this.logger.log(
      `[KAFKA CONSUMER RETRY] service=order-service ${formatConsumerRetryPolicy(this.executor.getPolicy())}`,
    );
  }

  /**
   * Executes handler with shared retry semantics.
   *
   * @param params - Event type, envelope, headers, handler
   */
  execute(params: ExecuteWithRetryParams): Promise<RetryOutcome> {
    return this.executor.execute(params);
  }
}

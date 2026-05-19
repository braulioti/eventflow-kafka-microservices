/** Nest wrapper around shared {@link KafkaRetryExecutor} bound to order-service. */
import { Injectable } from '@nestjs/common';
import {
  ExecuteWithRetryParams,
  KafkaRetryExecutor,
  RetryOutcome,
} from '@eventflow/shared';
import { EventPublisher } from './event-publisher.service';

@Injectable()
export class KafkaRetryRunner {
  private readonly executor: KafkaRetryExecutor;

  constructor(publisher: EventPublisher) {
    this.executor = new KafkaRetryExecutor(publisher, 'order-service');
  }

  execute(params: ExecuteWithRetryParams): Promise<RetryOutcome> {
    return this.executor.execute(params);
  }
}

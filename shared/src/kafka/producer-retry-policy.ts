import {
  type RetryPolicyConfig,
  calculateBackoffMs,
  resolveRetryPolicy,
  sleep,
} from './retry/retry-policy';

/** Producer publish retry policy (env: `KAFKA_PRODUCER_RETRY_*`, falls back to `KAFKA_RETRY_*`). */
export function resolveProducerRetryPolicy(
  overrides?: Partial<RetryPolicyConfig>,
): RetryPolicyConfig {
  return resolveRetryPolicy({
    maxAttempts: Number(
      process.env.KAFKA_PRODUCER_RETRY_MAX_ATTEMPTS ??
        process.env.KAFKA_RETRY_MAX_ATTEMPTS,
    ),
    baseDelayMs: Number(
      process.env.KAFKA_PRODUCER_RETRY_BASE_DELAY_MS ??
        process.env.KAFKA_RETRY_BASE_DELAY_MS,
    ),
    maxDelayMs: Number(
      process.env.KAFKA_PRODUCER_RETRY_MAX_DELAY_MS ??
        process.env.KAFKA_RETRY_MAX_DELAY_MS,
    ),
    backoffMultiplier: Number(
      process.env.KAFKA_PRODUCER_RETRY_BACKOFF_MULTIPLIER ??
        process.env.KAFKA_RETRY_BACKOFF_MULTIPLIER,
    ),
    ...overrides,
  });
}

export interface PublishWithRetryOptions {
  policy?: RetryPolicyConfig;
  /** Called before sleeping between attempts (attempt is 1-based for the *next* try). */
  onRetry?: (attempt: number, delayMs: number, error: unknown) => void;
}

/**
 * Application-level producer retry with exponential backoff.
 * Complements KafkaJS built-in producer retries in {@link getKafkaClientConfig}.
 */
export async function publishWithProducerRetry(
  publish: () => Promise<void>,
  options?: PublishWithRetryOptions,
): Promise<void> {
  const policy = options?.policy ?? resolveProducerRetryPolicy();
  let lastError: unknown;

  for (let attempt = 1; attempt <= policy.maxAttempts; attempt++) {
    try {
      await publish();
      return;
    } catch (error) {
      lastError = error;
      if (attempt >= policy.maxAttempts) {
        break;
      }

      const delayMs = calculateBackoffMs(attempt, policy);
      options?.onRetry?.(attempt + 1, delayMs, error);
      await sleep(delayMs);
    }
  }

  throw lastError;
}

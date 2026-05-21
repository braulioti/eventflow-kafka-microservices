/**
 * Application-level producer retry wrapper (above KafkaJS transport retries).
 *
 * {@link publishWithProducerRetry} sleeps with exponential backoff between publish attempts
 * when the domain publisher throws. Distinct from consumer {@link KafkaRetryExecutor} which
 * republishes to the same topic with headers.
 */
import {
  type RetryPolicyConfig,
  calculateBackoffMs,
  resolveRetryPolicy,
  sleep,
} from './retry/retry-policy';

/**
 * Resolves retry limits for outbound publish operations.
 * Env: `KAFKA_PRODUCER_RETRY_*`, falling back to `KAFKA_RETRY_*`.
 */
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

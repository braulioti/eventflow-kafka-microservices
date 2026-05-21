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

function resolveProducerRetryNumber(
  producerEnv: string | undefined,
  sharedEnv: string | undefined,
  fallback: number,
): number {
  const raw = producerEnv ?? sharedEnv;
  if (raw === undefined || raw.trim() === '') {
    return fallback;
  }
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : fallback;
}

/**
 * Resolves retry limits for outbound publish operations.
 * Env: `KAFKA_PRODUCER_RETRY_*`, falling back to `KAFKA_RETRY_*`.
 */
export function resolveProducerRetryPolicy(
  overrides?: Partial<RetryPolicyConfig>,
): RetryPolicyConfig {
  const fallback = resolveRetryPolicy();
  return {
    maxAttempts: resolveProducerRetryNumber(
      process.env.KAFKA_PRODUCER_RETRY_MAX_ATTEMPTS,
      process.env.KAFKA_RETRY_MAX_ATTEMPTS,
      fallback.maxAttempts,
    ),
    baseDelayMs: resolveProducerRetryNumber(
      process.env.KAFKA_PRODUCER_RETRY_BASE_DELAY_MS,
      process.env.KAFKA_RETRY_BASE_DELAY_MS,
      fallback.baseDelayMs,
    ),
    maxDelayMs: resolveProducerRetryNumber(
      process.env.KAFKA_PRODUCER_RETRY_MAX_DELAY_MS,
      process.env.KAFKA_RETRY_MAX_DELAY_MS,
      fallback.maxDelayMs,
    ),
    backoffMultiplier: resolveProducerRetryNumber(
      process.env.KAFKA_PRODUCER_RETRY_BACKOFF_MULTIPLIER,
      process.env.KAFKA_RETRY_BACKOFF_MULTIPLIER,
      fallback.backoffMultiplier,
    ),
    ...overrides,
  };
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

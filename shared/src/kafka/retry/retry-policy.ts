export interface RetryPolicyConfig {
  /** Total processing attempts before sending to DLQ (default: 3) */
  maxAttempts: number;
  /** Initial backoff delay in ms (default: 1000) */
  baseDelayMs: number;
  /** Maximum backoff cap in ms (default: 30000) */
  maxDelayMs: number;
  /** Exponential multiplier (default: 2 → 1s, 2s, 4s, …) */
  backoffMultiplier: number;
}

export const DEFAULT_RETRY_POLICY: RetryPolicyConfig = {
  maxAttempts: 3,
  baseDelayMs: 1000,
  maxDelayMs: 30000,
  backoffMultiplier: 2,
};

/** Merges env vars (KAFKA_RETRY_*) with optional overrides. */
export function resolveRetryPolicy(
  overrides?: Partial<RetryPolicyConfig>,
): RetryPolicyConfig {
  return {
    maxAttempts: Number(
      process.env.KAFKA_RETRY_MAX_ATTEMPTS ?? DEFAULT_RETRY_POLICY.maxAttempts,
    ),
    baseDelayMs: Number(
      process.env.KAFKA_RETRY_BASE_DELAY_MS ?? DEFAULT_RETRY_POLICY.baseDelayMs,
    ),
    maxDelayMs: Number(
      process.env.KAFKA_RETRY_MAX_DELAY_MS ?? DEFAULT_RETRY_POLICY.maxDelayMs,
    ),
    backoffMultiplier: Number(
      process.env.KAFKA_RETRY_BACKOFF_MULTIPLIER ??
        DEFAULT_RETRY_POLICY.backoffMultiplier,
    ),
    ...overrides,
  };
}

/** Exponential backoff: baseDelay * multiplier^(attempt - 1), capped at maxDelay */
export function calculateBackoffMs(
  attempt: number,
  config: RetryPolicyConfig = DEFAULT_RETRY_POLICY,
): number {
  if (attempt <= 0) {
    return 0;
  }

  const delay =
    config.baseDelayMs * Math.pow(config.backoffMultiplier, attempt - 1);

  return Math.min(delay, config.maxDelayMs);
}

export function shouldRetry(
  nextAttempt: number,
  config: RetryPolicyConfig = DEFAULT_RETRY_POLICY,
): boolean {
  return nextAttempt < config.maxAttempts;
}

export function shouldSendToDlq(
  nextAttempt: number,
  config: RetryPolicyConfig = DEFAULT_RETRY_POLICY,
): boolean {
  return nextAttempt >= config.maxAttempts;
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Exponential backoff retry policy shared by producers and consumers.
 *
 * Consumer handlers use {@link resolveConsumerRetryPolicy}; producers use
 * {@link resolveProducerRetryPolicy}. {@link KafkaRetryExecutor} calls
 * {@link shouldRetry} / {@link shouldSendToDlq} after each handler failure.
 */

/** Tunable retry/backoff parameters (env-overridable). */
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

/** Fallback when no `KAFKA_RETRY_*` environment variables are set. */
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

/**
 * Consumer handler retry policy (`KAFKA_CONSUMER_RETRY_*`, falls back to `KAFKA_RETRY_*`).
 * Used by {@link KafkaRetryExecutor} in all `@EventPattern` consumers.
 */
export function resolveConsumerRetryPolicy(
  overrides?: Partial<RetryPolicyConfig>,
): RetryPolicyConfig {
  const fallback = resolveRetryPolicy();
  return {
    maxAttempts: Number(
      process.env.KAFKA_CONSUMER_RETRY_MAX_ATTEMPTS ??
        process.env.KAFKA_RETRY_MAX_ATTEMPTS ??
        fallback.maxAttempts,
    ),
    baseDelayMs: Number(
      process.env.KAFKA_CONSUMER_RETRY_BASE_DELAY_MS ??
        process.env.KAFKA_RETRY_BASE_DELAY_MS ??
        fallback.baseDelayMs,
    ),
    maxDelayMs: Number(
      process.env.KAFKA_CONSUMER_RETRY_MAX_DELAY_MS ??
        process.env.KAFKA_RETRY_MAX_DELAY_MS ??
        fallback.maxDelayMs,
    ),
    backoffMultiplier: Number(
      process.env.KAFKA_CONSUMER_RETRY_BACKOFF_MULTIPLIER ??
        process.env.KAFKA_RETRY_BACKOFF_MULTIPLIER ??
        fallback.backoffMultiplier,
    ),
    ...overrides,
  };
}

/** Backoff delays per attempt (ms) until max attempts — useful for docs and tests. */
export function getRetryBackoffSchedule(
  policy: RetryPolicyConfig = resolveConsumerRetryPolicy(),
): number[] {
  const schedule: number[] = [];
  for (let attempt = 1; attempt < policy.maxAttempts; attempt++) {
    schedule.push(calculateBackoffMs(attempt, policy));
  }
  return schedule;
}

/** One-line summary for service startup logs. */
export function formatConsumerRetryPolicy(
  policy: RetryPolicyConfig = resolveConsumerRetryPolicy(),
): string {
  const schedule = getRetryBackoffSchedule(policy).join('ms, ');
  return [
    `maxAttempts=${policy.maxAttempts}`,
    `baseDelayMs=${policy.baseDelayMs}`,
    `maxDelayMs=${policy.maxDelayMs}`,
    `backoffMultiplier=${policy.backoffMultiplier}`,
    `backoffSchedule=[${schedule}ms]`,
  ].join(' ');
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

/**
 * Whether another in-process/republish attempt is allowed before DLQ.
 * @param nextAttempt - 1-based attempt count after incrementing `x-retry-count`
 */
export function shouldRetry(
  nextAttempt: number,
  config: RetryPolicyConfig = DEFAULT_RETRY_POLICY,
): boolean {
  return nextAttempt < config.maxAttempts;
}

/**
 * Whether the next failure should route to DLQ (attempts exhausted).
 * @param nextAttempt - 1-based attempt count after incrementing `x-retry-count`
 */
export function shouldSendToDlq(
  nextAttempt: number,
  config: RetryPolicyConfig = DEFAULT_RETRY_POLICY,
): boolean {
  return nextAttempt >= config.maxAttempts;
}

/** Promise-based delay used by consumer retry and producer republish loops. */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

import {
  calculateBackoffMs,
  formatConsumerRetryPolicy,
  getRetryBackoffSchedule,
  resolveConsumerRetryPolicy,
  shouldRetry,
  shouldSendToDlq,
} from '@eventflow/shared';

describe('Consumer retry policy', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    process.env = { ...originalEnv };
  });

  afterAll(() => {
    process.env = originalEnv;
  });

  it('defaults to 3 attempts with exponential backoff', () => {
    delete process.env.KAFKA_CONSUMER_RETRY_MAX_ATTEMPTS;
    const policy = resolveConsumerRetryPolicy();

    expect(policy.maxAttempts).toBe(3);
    expect(getRetryBackoffSchedule(policy)).toEqual([1000, 2000]);
    expect(calculateBackoffMs(1, policy)).toBe(1000);
    expect(calculateBackoffMs(2, policy)).toBe(2000);
  });

  it('allows consumer-specific env overrides', () => {
    process.env.KAFKA_CONSUMER_RETRY_MAX_ATTEMPTS = '5';
    process.env.KAFKA_CONSUMER_RETRY_BASE_DELAY_MS = '500';

    const policy = resolveConsumerRetryPolicy();
    expect(policy.maxAttempts).toBe(5);
    expect(policy.baseDelayMs).toBe(500);
  });

  it('controls retry vs DLQ by attempt count', () => {
    const policy = { maxAttempts: 3, baseDelayMs: 100, maxDelayMs: 1000, backoffMultiplier: 2 };

    expect(shouldRetry(1, policy)).toBe(true);
    expect(shouldRetry(2, policy)).toBe(true);
    expect(shouldRetry(3, policy)).toBe(false);
    expect(shouldSendToDlq(3, policy)).toBe(true);
  });

  it('formats policy for startup logs', () => {
    const line = formatConsumerRetryPolicy({
      maxAttempts: 3,
      baseDelayMs: 1000,
      maxDelayMs: 30000,
      backoffMultiplier: 2,
    });
    expect(line).toContain('maxAttempts=3');
    expect(line).toContain('backoffSchedule');
  });
});

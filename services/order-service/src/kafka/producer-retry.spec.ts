import { calculateBackoffMs, publishWithProducerRetry } from '@eventflow/shared';

const testPolicy = {
  maxAttempts: 3,
  baseDelayMs: 10,
  maxDelayMs: 50,
  backoffMultiplier: 2,
};

describe('publishWithProducerRetry', () => {
  it('succeeds on first attempt', async () => {
    const fn = jest.fn().mockResolvedValue(undefined);
    await publishWithProducerRetry(fn, { policy: testPolicy });
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('retries with backoff then succeeds', async () => {
    const fn = jest
      .fn()
      .mockRejectedValueOnce(new Error('broker down'))
      .mockRejectedValueOnce(new Error('broker down'))
      .mockResolvedValue(undefined);

    const onRetry = jest.fn();

    await publishWithProducerRetry(fn, { policy: testPolicy, onRetry });

    expect(fn).toHaveBeenCalledTimes(3);
    expect(onRetry).toHaveBeenCalledTimes(2);
    expect(onRetry.mock.calls[0][1]).toBe(calculateBackoffMs(1, testPolicy));
  });

  it('throws after max attempts', async () => {
    const fn = jest.fn().mockRejectedValue(new Error('permanent failure'));

    await expect(
      publishWithProducerRetry(fn, {
        policy: { maxAttempts: 2, baseDelayMs: 1, maxDelayMs: 5, backoffMultiplier: 2 },
      }),
    ).rejects.toThrow('permanent failure');

    expect(fn).toHaveBeenCalledTimes(2);
  });
});

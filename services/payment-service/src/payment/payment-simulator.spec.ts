import {
  isPaymentForceFailureEnabled,
  resolvePaymentFailureRate,
  simulatePaymentGateway,
} from './payment-simulator';

describe('payment simulator', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    process.env = { ...originalEnv };
  });

  afterAll(() => {
    process.env = originalEnv;
  });

  it('approves when failure rate is 0', () => {
    const result = simulatePaymentGateway('order-1', {
      failureRate: 0,
      forceFailure: false,
    });
    expect(result.outcome).toBe('approved');
    expect(result.paymentId).toBeDefined();
  });

  it('declines when force failure is enabled', () => {
    const result = simulatePaymentGateway('order-1', { forceFailure: true });
    expect(result.outcome).toBe('failed');
    expect(result.status).toBe('failed');
    expect(result.failureReason).toBeDefined();
  });

  it('always declines at failure rate 1', () => {
    const result = simulatePaymentGateway('order-1', { failureRate: 1 });
    expect(result.outcome).toBe('failed');
  });

  it('defaults to 20% failure rate (80% approval)', () => {
    delete process.env.PAYMENT_FAILURE_RATE;
    expect(resolvePaymentFailureRate()).toBe(0.2);
  });

  it('reads failure rate from env', () => {
    process.env.PAYMENT_FAILURE_RATE = '0.5';
    expect(resolvePaymentFailureRate()).toBe(0.5);
  });

  it('picks realistic failure reasons', () => {
    const result = simulatePaymentGateway('order-1', { failureRate: 1 });
    expect(['timeout', 'gateway_unavailable', 'card_declined', 'connection_reset']).toContain(
      result.failureType,
    );
  });

  it('reads force failure flag from env', () => {
    process.env.PAYMENT_FORCE_FAILURE = 'true';
    expect(isPaymentForceFailureEnabled()).toBe(true);
  });
});

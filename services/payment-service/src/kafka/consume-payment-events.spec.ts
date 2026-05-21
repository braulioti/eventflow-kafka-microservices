import {
  EventType,
  createEventEnvelope,
  parsePaymentFailedMessage,
  parsePaymentProcessedMessage,
} from '@eventflow/shared';

describe('parsePaymentEventsMessage', () => {
  const processedEnvelope = createEventEnvelope({
    eventType: EventType.PAYMENT_PROCESSED,
    source: 'payment-service',
    correlationId: 'order-1',
    payload: {
      paymentId: 'pay-1',
      orderId: 'order-1',
      transactionId: 'txn-1',
      processedAt: new Date().toISOString(),
    },
  });

  const failedEnvelope = createEventEnvelope({
    eventType: EventType.PAYMENT_FAILED,
    source: 'payment-service',
    correlationId: 'order-2',
    payload: {
      paymentId: 'pay-2',
      orderId: 'order-2',
      reason: 'Payment gateway timeout',
      failedAt: new Date().toISOString(),
    },
  });

  it('parses payment.processed from payment.events stream', () => {
    const result = parsePaymentProcessedMessage(processedEnvelope);
    expect(result.kind).toBe('payment.processed');
  });

  it('parses payment.failed from payment.events stream', () => {
    const result = parsePaymentFailedMessage(failedEnvelope);
    expect(result.kind).toBe('payment.failed');
  });

  it('skips payment.failed when filtering for processed', () => {
    const result = parsePaymentProcessedMessage(failedEnvelope);
    expect(result.kind).toBe('skipped');
  });

  it('skips payment.processed when filtering for failed', () => {
    const result = parsePaymentFailedMessage(processedEnvelope);
    expect(result.kind).toBe('skipped');
  });
});

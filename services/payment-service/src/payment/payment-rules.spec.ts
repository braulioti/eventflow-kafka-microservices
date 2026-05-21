import {
  evaluatePaymentRules,
  formatRuleViolations,
  resolvePaymentBusinessRules,
} from './payment-rules';

const baseOrder = {
  orderId: 'order-1',
  customerId: 'customer-1',
  items: [{ productId: 'sku-1', quantity: 2, unitPrice: 49.9 }],
  totalAmount: 99.8,
  currency: 'BRL' as const,
};

describe('evaluatePaymentRules', () => {
  it('passes valid orders', () => {
    expect(evaluatePaymentRules(baseOrder)).toHaveLength(0);
  });

  it('rejects amount below minimum', () => {
    const violations = evaluatePaymentRules(
      { ...baseOrder, totalAmount: 0 },
      { ...resolvePaymentBusinessRules(), minAmount: 1 },
    );
    expect(violations[0]?.rule).toBe('MIN_AMOUNT');
  });

  it('rejects amount above maximum', () => {
    const violations = evaluatePaymentRules(
      { ...baseOrder, totalAmount: 99_999 },
      { ...resolvePaymentBusinessRules(), maxAmount: 100 },
    );
    expect(violations[0]?.rule).toBe('MAX_AMOUNT');
  });

  it('rejects when total does not match items', () => {
    const violations = evaluatePaymentRules({
      ...baseOrder,
      totalAmount: 50,
    });
    expect(violations.some((v) => v.rule === 'AMOUNT_MATCHES_ITEMS')).toBe(true);
  });

  it('formats violations for decline reason', () => {
    const text = formatRuleViolations([
      { rule: 'MIN_AMOUNT', message: 'too low' },
    ]);
    expect(text).toContain('MIN_AMOUNT');
  });
});

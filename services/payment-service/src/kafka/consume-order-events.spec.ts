import {
  EventType,
  EventValidationError,
  createEventEnvelope,
  parseOrderEventsMessage,
} from '@eventflow/shared';

describe('parseOrderEventsMessage', () => {
  const validEnvelope = createEventEnvelope({
    eventType: EventType.ORDER_CREATED,
    source: 'order-service',
    correlationId: 'order-123',
    payload: {
      orderId: 'order-123',
      customerId: 'customer-1',
      items: [{ productId: 'sku-1', quantity: 2, unitPrice: 49.9 }],
      totalAmount: 99.8,
      currency: 'BRL',
    },
  });

  it('deserializes Buffer and validates order.created', () => {
    const buffer = Buffer.from(JSON.stringify(validEnvelope), 'utf8');
    const result = parseOrderEventsMessage(buffer);

    expect(result.kind).toBe('order.created');
    if (result.kind === 'order.created') {
      expect(result.envelope.payload.orderId).toBe('order-123');
      expect(result.envelope.eventType).toBe(EventType.ORDER_CREATED);
    }
  });

  it('filters non order.created events on order.events', () => {
    const other = createEventEnvelope({
      eventType: EventType.ORDER_CANCELLED,
      source: 'order-service',
      correlationId: 'order-456',
      payload: {
        orderId: 'order-456',
        reason: 'test',
        cancelledBy: 'system',
      },
    });

    const result = parseOrderEventsMessage(other);
    expect(result).toEqual({
      kind: 'skipped',
      reason: 'unsupported-event-type',
      eventType: EventType.ORDER_CANCELLED,
    });
  });

  it('rejects invalid envelope structure', () => {
    expect(() =>
      parseOrderEventsMessage({ eventType: 'order.created', payload: {} }),
    ).toThrow(EventValidationError);
  });

  it('rejects invalid order.created payload', () => {
    const bad = {
      ...validEnvelope,
      payload: { orderId: 'order-123', customerId: '', items: [], totalAmount: -1, currency: 'XXX' },
    };

    expect(() => parseOrderEventsMessage(bad)).toThrow(EventValidationError);
  });

  it('requires correlationId to match orderId', () => {
    const mismatch = {
      ...validEnvelope,
      correlationId: 'other-id',
    };

    expect(() => parseOrderEventsMessage(mismatch)).toThrow(EventValidationError);
  });
});

import {
  ConsumerGroup,
  getKafkaConsumerConnectionInfo,
  resolveConsumerGroup,
} from '@eventflow/shared';

describe('Kafka consumer config', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    process.env = { ...originalEnv };
  });

  afterAll(() => {
    process.env = originalEnv;
  });

  it('uses default consumer group per service', () => {
    expect(resolveConsumerGroup('payment-service')).toBe(ConsumerGroup.PAYMENT);
  });

  it('allows per-service group override', () => {
    process.env.KAFKA_CONSUMER_GROUP_PAYMENT_SERVICE = 'custom.payment.group';
    expect(resolveConsumerGroup('payment-service')).toBe('custom.payment.group');
  });

  it('includes brokers and group in connection info', () => {
    process.env.KAFKA_BOOTSTRAP_SERVERS = 'kafka:29092,localhost:9092';
    const info = getKafkaConsumerConnectionInfo('order-service');

    expect(info.brokers).toEqual(['kafka:29092', 'localhost:9092']);
    expect(info.groupId).toBe(ConsumerGroup.ORDER);
    expect(info.clientId).toBe('order-service-consumer');
  });
});

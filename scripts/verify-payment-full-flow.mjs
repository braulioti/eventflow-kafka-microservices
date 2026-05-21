#!/usr/bin/env node
/**
 * Full saga slice: POST /orders → order.events → payment.events (processed | failed)
 *
 * Usage:
 *   PAYMENT_FAILURE_RATE=0 node scripts/verify-payment-full-flow.mjs
 *   ORDER_SERVICE_URL=http://localhost:3001 KAFKA_BOOTSTRAP_SERVERS=localhost:9092 node scripts/verify-payment-full-flow.mjs
 */
import { Kafka, logLevel } from 'kafkajs';

const ORDER_SERVICE_URL = process.env.ORDER_SERVICE_URL ?? 'http://localhost:3001';
const KAFKA_BROKERS = (process.env.KAFKA_BOOTSTRAP_SERVERS ?? 'localhost:9092')
  .split(',')
  .map((b) => b.trim());
const ORDER_TOPIC = 'order.events';
const PAYMENT_TOPIC = 'payment.events';
const TIMEOUT_MS = 60000;

const body = {
  customerId: 'customer-flow',
  currency: 'BRL',
  items: [{ productId: 'sku-flow', quantity: 2, unitPrice: 49.9 }],
};

const kafka = new Kafka({
  clientId: 'verify-payment-full-flow',
  brokers: KAFKA_BROKERS,
  logLevel: logLevel.ERROR,
});

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

async function waitForMessage(consumer, topic, filterFn) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`Timeout waiting for ${topic}`)),
      TIMEOUT_MS,
    );

    consumer
      .run({
        eachMessage: async ({ message }) => {
          if (!message.value) return;
          const envelope = JSON.parse(message.value.toString());
          if (!filterFn(envelope, message)) return;
          clearTimeout(timer);
          resolve({
            envelope,
            key: message.key?.toString(),
            topic,
          });
        },
      })
      .catch(reject);
  });
}

async function main() {
  const orderConsumer = kafka.consumer({ groupId: `verify-order-${Date.now()}` });
  const paymentConsumer = kafka.consumer({ groupId: `verify-payment-${Date.now()}` });

  await orderConsumer.connect();
  await paymentConsumer.connect();
  await orderConsumer.subscribe({ topic: ORDER_TOPIC, fromBeginning: false });
  await paymentConsumer.subscribe({ topic: PAYMENT_TOPIC, fromBeginning: false });

  const orderPromise = waitForMessage(
    orderConsumer,
    ORDER_TOPIC,
    (e) => e.eventType === 'order.created',
  );

  const paymentPromise = waitForMessage(
    paymentConsumer,
    PAYMENT_TOPIC,
    (e, _m) =>
      e.eventType === 'payment.processed' || e.eventType === 'payment.failed',
  );

  console.log(`→ POST ${ORDER_SERVICE_URL}/orders`);
  const response = await fetch(`${ORDER_SERVICE_URL}/orders`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    throw new Error(`POST /orders failed: ${response.status} ${await response.text()}`);
  }

  const order = await response.json();
  console.log('← Order created:', JSON.stringify(order, null, 2));

  const orderMsg = await orderPromise;
  assert(
    orderMsg.envelope.eventId === order.eventId,
    'order.events eventId mismatch',
  );
  assert(orderMsg.key === order.orderId, 'order.events key must be orderId');
  console.log('\n✓ Step 1–2: order.created published and consumed (validated)');

  const paymentMsg = await paymentPromise;
  assert(
    paymentMsg.envelope.payload.orderId === order.orderId,
    'payment event orderId mismatch',
  );
  assert(
    paymentMsg.key === order.orderId,
    'payment.events key must be orderId',
  );

  console.log(`\n✓ Step 3–5: payment simulated → ${paymentMsg.envelope.eventType}`);
  console.log('  topic:', PAYMENT_TOPIC);
  console.log('  key:', paymentMsg.key);
  console.log('  paymentId:', paymentMsg.envelope.payload.paymentId);

  if (paymentMsg.envelope.eventType === 'payment.processed') {
    assert(paymentMsg.envelope.payload.transactionId, 'missing transactionId');
    console.log('  transactionId:', paymentMsg.envelope.payload.transactionId);
  } else {
    console.log('  reason:', paymentMsg.envelope.payload.reason);
  }

  console.log('\n→ Kafka UI: Topics → order.events + payment.events');
  console.log('→ payment-service logs: [EVENT RECEIVED] → [PAYMENT SUCCESS|FAILED]');

  await orderConsumer.disconnect();
  await paymentConsumer.disconnect();
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error('✗', error.message);
    process.exit(1);
  });

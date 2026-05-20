#!/usr/bin/env node
/**
 * Manual / CI helper: POST /orders and validate the Kafka record on order.events.
 *
 * Usage:
 *   node scripts/verify-order-kafka-flow.mjs
 *   ORDER_SERVICE_URL=http://localhost:3001 KAFKA_BOOTSTRAP_SERVERS=localhost:9092 node scripts/verify-order-kafka-flow.mjs
 */
import { Kafka, logLevel } from 'kafkajs';

const ORDER_SERVICE_URL = process.env.ORDER_SERVICE_URL ?? 'http://localhost:3001';
const KAFKA_BROKERS = (process.env.KAFKA_BOOTSTRAP_SERVERS ?? 'localhost:9092')
  .split(',')
  .map((b) => b.trim());
const TOPIC = 'order.events';
const TIMEOUT_MS = 45000;

const body = {
  customerId: 'customer-verify',
  currency: 'BRL',
  items: [{ productId: 'sku-verify', quantity: 2, unitPrice: 49.9 }],
};

const kafka = new Kafka({
  clientId: 'verify-order-flow',
  brokers: KAFKA_BROKERS,
  logLevel: logLevel.ERROR,
});

const consumer = kafka.consumer({ groupId: `verify-flow-${Date.now()}` });

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function main() {
  console.log(`→ Subscribing to ${TOPIC} (${KAFKA_BROKERS.join(',')})`);
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

  const messagePromise = new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`No message on ${TOPIC} within ${TIMEOUT_MS}ms`)),
      TIMEOUT_MS,
    );

    consumer.run({
      eachMessage: async ({ message }) => {
        if (!message.value) return;

        const envelope = JSON.parse(message.value.toString());
        if (envelope.eventType !== 'order.created') return;

        clearTimeout(timer);
        resolve({ envelope, key: message.key?.toString(), headers: message.headers });
      },
    }).catch(reject);
  });

  console.log(`→ POST ${ORDER_SERVICE_URL}/orders`);
  const response = await fetch(`${ORDER_SERVICE_URL}/orders`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`POST /orders failed: ${response.status} ${text}`);
  }

  const created = await response.json();
  console.log('← Order API response:', JSON.stringify(created, null, 2));

  const { envelope, key } = await messagePromise;
  await consumer.disconnect();

  assert(key === created.orderId, `Kafka key ${key} !== orderId ${created.orderId}`);
  assert(
    envelope.eventId === created.eventId,
    `eventId mismatch: kafka=${envelope.eventId} api=${created.eventId}`,
  );
  assert(
    envelope.correlationId === created.orderId,
    `correlationId must equal orderId`,
  );
  assert(envelope.source === 'order-service', 'source must be order-service');
  assert(envelope.payload.orderId === created.orderId, 'payload.orderId mismatch');
  assert(envelope.payload.totalAmount === 99.8, 'payload.totalAmount mismatch');

  console.log('\n✓ Kafka message validated');
  console.log(`  topic: ${TOPIC}`);
  console.log(`  key: ${key}`);
  console.log(`  eventType: ${envelope.eventType}`);
  console.log(`  eventId: ${envelope.eventId}`);
  console.log(`  correlationId: ${envelope.correlationId}`);
  console.log('\n→ Open Kafka UI → Topics → order.events to inspect the same record.');
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error('✗', error.message);
    process.exit(1);
  })
  .finally(() => consumer.disconnect().catch(() => {}));

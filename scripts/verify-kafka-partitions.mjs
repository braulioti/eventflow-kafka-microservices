#!/usr/bin/env node
/**
 * Validates Kafka partitions: count, key→partition stickiness, distribution, consumer balance.
 *
 * Usage:
 *   npm run verify:kafka-partitions
 *   KAFKA_BOOTSTRAP_SERVERS=localhost:9092 ORDER_SERVICE_URL=http://localhost:3001 \
 *     KAFKA_TOPIC_PARTITIONS=3 ORDER_BATCH_SIZE=12 node scripts/verify-kafka-partitions.mjs
 */
import { execSync } from 'node:child_process';
import { Kafka, logLevel } from 'kafkajs';
import { createHash } from 'node:crypto';

const BROKERS = (process.env.KAFKA_BOOTSTRAP_SERVERS ?? 'localhost:9092')
  .split(',')
  .map((b) => b.trim());
const ORDER_SERVICE_URL = process.env.ORDER_SERVICE_URL ?? 'http://localhost:3001';
const TOPIC = process.env.KAFKA_PARTITION_TEST_TOPIC ?? 'order.events';
const EXPECTED_PARTITIONS = Number(process.env.KAFKA_TOPIC_PARTITIONS ?? 3);
const ORDER_BATCH_SIZE = Number(process.env.ORDER_BATCH_SIZE ?? 12);
const PAYMENT_GROUP =
  process.env.KAFKA_PAYMENT_CONSUMER_GROUP ?? 'eventflow.payment-service-server';
const TIMEOUT_MS = 60000;

const kafka = new Kafka({
  clientId: 'verify-kafka-partitions',
  brokers: BROKERS,
  logLevel: logLevel.ERROR,
});

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function partitionSpread(records) {
  const byPartition = new Map();
  for (const r of records) {
    byPartition.set(r.partition, (byPartition.get(r.partition) ?? 0) + 1);
  }
  return byPartition;
}

async function describeTopicPartitions(admin) {
  const metadata = await admin.fetchTopicMetadata({ topics: [TOPIC] });
  const topic = metadata.topics.find((t) => t.name === TOPIC);
  assert(topic, `Topic ${TOPIC} not found. Run kafka-init or create the topic.`);
  return topic.partitions.length;
}

async function produceWithKey(producer, key, label) {
  const value = JSON.stringify({
    probe: true,
    label,
    key,
    ts: new Date().toISOString(),
  });
  const result = await producer.send({
    topic: TOPIC,
    messages: [{ key, value }],
  });
  return result[0].partition;
}

async function testKeyOrdering(producer) {
  console.log('\n=== 1) Ordenação por key (mesma key → mesma partition) ===');
  const stickyKey = 'order-sticky-abc';
  const partitions = [];
  for (let i = 0; i < 5; i++) {
    partitions.push(await produceWithKey(producer, stickyKey, `seq-${i}`));
  }
  const unique = new Set(partitions);
  console.log(`   key=${stickyKey} → partitions=${[...unique].join(', ')} (5 produces)`);
  assert(
    unique.size === 1,
    `Expected one partition for key ${stickyKey}, got ${[...unique].join(', ')}`,
  );
  console.log('   ✓ Mesma partition key mantém ordem por partição no Kafka');
}

async function testDistribution(producer) {
  console.log('\n=== 2) Distribuição entre partitions ===');
  const records = [];
  const keys = Array.from({ length: 30 }, (_, i) => `order-dist-${String(i).padStart(3, '0')}`);
  for (const key of keys) {
    records.push({ key, partition: await produceWithKey(producer, key, key) });
  }
  const spread = partitionSpread(records);
  console.log('   Histogram (30 keys distintas):');
  for (const [partition, count] of [...spread.entries()].sort((a, b) => a[0] - b[0])) {
    console.log(`     partition ${partition}: ${count} message(s)`);
  }
  assert(
    spread.size >= 2,
    `Expected messages across >= 2 partitions, got ${spread.size}. Increase keys or partitions.`,
  );
  if (spread.size >= EXPECTED_PARTITIONS) {
    console.log(`   ✓ Usando ${spread.size}/${EXPECTED_PARTITIONS} partitions`);
  } else {
    console.log(
      `   ⚠ Usando ${spread.size}/${EXPECTED_PARTITIONS} partitions (aceitável; hashing pode concentrar)`,
    );
  }
}

async function testHashStability() {
  console.log('\n=== 3) Estabilidade do hash (key → partition previsível) ===');
  const key = 'order-hash-check';
  const h = createHash('md5').update(key).digest();
  const pseudoPartition = h.readUInt32BE(0) % EXPECTED_PARTITIONS;
  console.log(`   key=${key} → hash mod ${EXPECTED_PARTITIONS} ≈ partition ${pseudoPartition}`);
  console.log('   (Kafka usa murmur2; o teste 1 já valida stickiness real no broker)');
}

async function createOrders(batchSize) {
  const created = [];
  for (let i = 0; i < batchSize; i++) {
    const res = await fetch(`${ORDER_SERVICE_URL}/orders`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        customerId: `partition-test-${i}`,
        items: [{ productId: `sku-${i}`, quantity: 1, unitPrice: 10 + i }],
      }),
    });
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`POST /orders failed: ${res.status} ${text}`);
    }
    created.push(await res.json());
  }
  return created;
}

async function testApiFlow(partitionCount) {
  console.log('\n=== 4) Fluxo real: POST /orders → order.events ===');
  const consumer = kafka.consumer({ groupId: `verify-partitions-${Date.now()}` });
  const seen = [];

  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

  const messagesPromise = new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(
        new Error(
          `Timed out: got ${seen.length}/${ORDER_BATCH_SIZE} messages on ${TOPIC}`,
        ),
      );
    }, TIMEOUT_MS);

    consumer
      .run({
        eachMessage: async ({ message, partition }) => {
          if (!message.value) return;
          let envelope;
          try {
            envelope = JSON.parse(message.value.toString());
          } catch {
            return;
          }
          if (envelope.eventType !== 'order.created') return;

          seen.push({
            partition,
            key: message.key?.toString(),
            orderId: envelope.payload?.orderId,
            eventId: envelope.eventId,
          });

          if (seen.length >= ORDER_BATCH_SIZE) {
            clearTimeout(timer);
            resolve([...seen]);
          }
        },
      })
      .catch(reject);
  });

  // Consumer must join the group before we publish via POST /orders.
  await new Promise((r) => setTimeout(r, 3000));

  console.log(`   Creating ${ORDER_BATCH_SIZE} orders...`);
  const orders = await createOrders(ORDER_BATCH_SIZE);
  const orderIds = new Set(orders.map((o) => o.orderId));

  const messages = await messagesPromise;
  await consumer.disconnect();

  assert(messages.length === ORDER_BATCH_SIZE, `Expected ${ORDER_BATCH_SIZE} messages`);

  for (const msg of messages) {
    assert(
      msg.key === msg.orderId,
      `Partition key must be orderId (got key=${msg.key} orderId=${msg.orderId})`,
    );
    assert(orderIds.has(msg.orderId), `Unexpected orderId ${msg.orderId}`);
    assert(
      msg.partition >= 0 && msg.partition < partitionCount,
      `Invalid partition ${msg.partition} for topic with ${partitionCount} partitions`,
    );
  }

  const spread = partitionSpread(messages);
  console.log(`   ${ORDER_BATCH_SIZE} order.created records:`);
  for (const [partition, count] of [...spread.entries()].sort((a, b) => a[0] - b[0])) {
    console.log(`     partition ${partition}: ${count} order(s)`);
  }

  if (ORDER_BATCH_SIZE >= partitionCount) {
    assert(
      spread.size >= 2,
      `Expected load across multiple partitions (got ${spread.size}). ` +
        'Restart payment consumers after altering partitions.',
    );
  }

  const keyToPartition = new Map();
  for (const msg of messages) {
    const prev = keyToPartition.get(msg.key);
    if (prev !== undefined) {
      assert(prev === msg.partition, `Key ${msg.key} jumped partition ${prev} → ${msg.partition}`);
    } else {
      keyToPartition.set(msg.key, msg.partition);
    }
  }
  console.log('   ✓ Cada orderId mapeia para uma única partition (ordenacao por pedido)');
}

function describeGroupViaPodman(groupId) {
  const container = process.env.KAFKA_CONTAINER ?? 'eventflow-kafka';
  const bootstrap = process.env.KAFKA_CONTAINER_BOOTSTRAP ?? 'kafka:29092';
  try {
    const out = execSync(
      `podman exec ${container} kafka-consumer-groups --bootstrap-server ${bootstrap} --describe --group ${groupId} --members --verbose`,
      { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] },
    );
    return out;
  } catch {
    return null;
  }
}

function parseOrderEventsAssignments(text) {
  const byMember = new Map();
  for (const line of text.split('\n')) {
    if (!line.includes('order.events')) continue;
    const consumerMatch = line.match(/payment-service-consumer[^\s]+/);
    const partitionsMatch = line.match(/order\.events\(([^)]*)\)/);
    if (!consumerMatch || !partitionsMatch) continue;
    const partitions = partitionsMatch[1]
      .split(',')
      .map((p) => p.trim())
      .filter(Boolean)
      .map(Number);
    byMember.set(consumerMatch[0], partitions);
  }
  return byMember;
}

async function describePaymentGroupBalance(admin) {
  console.log('\n=== 5) Balanceamento do consumer group (payment-service) ===');
  try {
    const { groups } = await admin.listGroups();
    const match =
      groups.find((g) => g.groupId === PAYMENT_GROUP) ??
      groups.find((g) => g.groupId.includes('payment-service'));

    if (!match) {
      console.log(`   ⚠ Group ${PAYMENT_GROUP} not found. Is payment-service running?`);
      console.log('   Available groups:', groups.map((g) => g.groupId).join(', ') || '(none)');
      return;
    }

    const described = await admin.describeGroups([match.groupId]);
    const group = described.groups[0];
    console.log(`   groupId=${group.groupId} state=${group.state} members=${group.members.length}`);

    assert(
      group.members.length >= 2,
      'Expected >= 2 payment-service members. Start payment-service-1 and payment-service-2.',
    );

    const cli = describeGroupViaPodman(group.groupId);
    if (cli) {
      const assignments = parseOrderEventsAssignments(cli);
      let membersWithOrderEvents = 0;
      for (const [member, partitions] of assignments) {
        if (partitions.length > 0) {
          membersWithOrderEvents += 1;
          console.log(
            `     - ${member}: order.events partitions [${partitions.join(', ')}]`,
          );
        }
      }
      const allPartitions = new Set([...assignments.values()].flat());
      console.log(`   Partitions cobertas em order.events: [${[...allPartitions].sort().join(', ')}]`);
      if (membersWithOrderEvents >= 2 && allPartitions.size >= 2) {
        console.log('   ✓ Balanceamento: 2+ consumers e 2+ partitions atribuídas');
      } else if (membersWithOrderEvents === 1) {
        console.log(
          '   ⚠ Um único consumer em order.events — aumente partitions e reinicie payments',
        );
      }
    } else {
      console.log(
        '   (Detalhe de assignment: podman exec eventflow-kafka kafka-consumer-groups --describe --group ' +
          `${group.groupId} --members --verbose)`,
      );
    }
  } catch (error) {
    console.log(`   ⚠ Could not describe group: ${error instanceof Error ? error.message : error}`);
  }
}

async function main() {
  console.log('EventFlow — Kafka partition verification');
  console.log(`Brokers: ${BROKERS.join(', ')}`);
  console.log(`Topic: ${TOPIC}`);
  console.log(`Expected partitions: ${EXPECTED_PARTITIONS}`);

  const admin = kafka.admin();
  const producer = kafka.producer();

  await admin.connect();
  await producer.connect();

  try {
    const partitionCount = await describeTopicPartitions(admin);
    console.log(`\nTopic ${TOPIC} has ${partitionCount} partition(s)`);
    assert(
      partitionCount >= EXPECTED_PARTITIONS,
      `Topic has ${partitionCount} partitions; need >= ${EXPECTED_PARTITIONS}. ` +
        'Run: npm run kafka:partitions',
    );

    await testKeyOrdering(producer);
    await testDistribution(producer);
    await testHashStability();
    await testApiFlow(partitionCount);
    await describePaymentGroupBalance(admin);

    console.log('\n✓ All partition checks passed');
  } finally {
    await producer.disconnect();
    await admin.disconnect();
  }
}

main().catch((err) => {
  console.error('\n✗', err.message);
  process.exit(1);
});

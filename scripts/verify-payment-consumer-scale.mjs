#!/usr/bin/env node
/**
 * Validates horizontal scale: payment-service replicas share groupId eventflow.payment-service.
 *
 * Usage:
 *   npm run verify:payment-scale
 *   KAFKA_BOOTSTRAP_SERVERS=localhost:9092 PAYMENT_SCALE_MIN_MEMBERS=2 node scripts/verify-payment-consumer-scale.mjs
 */
import { Kafka, logLevel } from 'kafkajs';

const BROKERS = (process.env.KAFKA_BOOTSTRAP_SERVERS ?? 'localhost:9092')
  .split(',')
  .map((b) => b.trim());
const GROUP_ID =
  process.env.KAFKA_CONSUMER_GROUP_PAYMENT_SERVICE ?? 'eventflow.payment-service';
const MIN_MEMBERS = Number(process.env.PAYMENT_SCALE_MIN_MEMBERS ?? 2);
const TIMEOUT_MS = 15000;

const kafka = new Kafka({
  clientId: 'verify-payment-scale',
  brokers: BROKERS,
  logLevel: logLevel.ERROR,
});

const admin = kafka.admin();

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

async function describeGroup() {
  await admin.connect();
  try {
    const { groups } = await admin.listGroups();
    const exists = groups.some((g) => g.groupId === GROUP_ID);
    if (!exists) {
      return { memberCount: 0, members: [], state: 'EmptyGroup' };
    }

    const descriptions = await admin.describeGroups([GROUP_ID]);
    const group = descriptions.groups[0];
    const members = group.members ?? [];
    return {
      memberCount: members.length,
      members: members.map((m) => ({
        clientId: m.clientId,
        memberId: m.memberId,
      })),
      state: group.state,
    };
  } finally {
    await admin.disconnect();
  }
}

async function main() {
  console.log(`→ Brokers: ${BROKERS.join(', ')}`);
  console.log(`→ Expected consumer group: ${GROUP_ID}`);
  console.log(`→ Minimum members: ${MIN_MEMBERS}`);

  const deadline = Date.now() + TIMEOUT_MS;
  let last = { memberCount: 0, members: [], state: 'unknown' };

  while (Date.now() < deadline) {
    last = await describeGroup();
    if (last.memberCount >= MIN_MEMBERS) break;
    await new Promise((r) => setTimeout(r, 2000));
  }

  console.log(`← Group state: ${last.state}`);
  console.log(`← Members (${last.memberCount}):`);
  for (const m of last.members) {
    console.log(`   - clientId=${m.clientId ?? '(pending)'}`);
  }

  assert(
    last.memberCount >= MIN_MEMBERS,
    `Group ${GROUP_ID} has ${last.memberCount} member(s); expected >= ${MIN_MEMBERS}. ` +
      'Start replicas: npm run docker:payment-instances',
  );

  const clientIds = last.members.map((m) => m.clientId).filter(Boolean);
  assert(
    new Set(clientIds).size === clientIds.length,
    `Duplicate clientIds in group: ${clientIds.join(', ')}`,
  );

  console.log('✓ Horizontal scale OK — same groupId, distinct consumers');
}

main().catch((err) => {
  console.error('✗', err.message);
  process.exit(1);
});

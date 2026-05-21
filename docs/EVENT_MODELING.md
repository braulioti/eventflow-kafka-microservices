# Event Modeling

Design reference for EventFlow domain events, Kafka topics, and message standards.

> **Complete rules (single source of truth):** [RULES.md](./RULES.md) — flow, aggregate topics, partitions, scalability, producer/consumer, Docker, and troubleshooting.

## Core system events

These five events define the primary business flow:

| Order | Event | Producer | Consumer(s) |
|-------|-------|----------|-------------|
| 1 | `order.created` | order-service | payment-service |
| 2 | `payment.processed` | payment-service | stock-service |
| 3 | `stock.reserved` | stock-service | notification-service |
| 4 | `notification.sent` | notification-service | order-service |

**Failure branch:** `payment.failed` (payment-service → order-service, notification-service)

```
order.created → payment.processed → stock.reserved → notification.sent
                      ↓ (on failure)
               payment.failed
```

Code: `CORE_SYSTEM_EVENTS` and `CORE_EVENT_FLOW` in `shared/src/events/core-events.ts`.

Additional events (`order.cancelled`, `payment.requested`, etc.) remain in the full catalog for extended flows.

---

## Order API (`POST /orders`)

Implemented in **order-service** only.

| Layer | Detail |
|-------|--------|
| Endpoint | `POST /orders` → HTTP **201** |
| DTO | `CreateOrderDto`, nested `OrderItemDto` |
| Validation | `class-validator` + global `ValidationPipe` (`whitelist`, `transform`) |
| Database | SQLite via TypeORM — tables `orders`, `order_items` |
| Env | `ORDER_DATABASE_PATH` (default `./services/order-service/data/orders.sqlite`) |

**Request example:**

```json
{
  "customerId": "customer-1",
  "currency": "BRL",
  "items": [{ "productId": "sku-1", "quantity": 2, "unitPrice": 49.9 }]
}
```

**Flow:** validate → persist `pending` → build envelope → persist `eventId` → publish to **`order.events`** → return JSON.

Code: `services/order-service/src/orders/`.

### Checklist — `order.created`

| Item | Implementation |
|------|----------------|
| Create event envelope | `createEventEnvelope()` in `OrdersService.createOrder` |
| Generate `eventId` | UUID via `createEventEnvelope` (stored on `orders.event_id`) |
| Generate `correlationId` | `orderId` (same value for the whole saga) |
| Add `timestamp` | ISO-8601 UTC default in `createEventEnvelope` |
| Publish to topic `order.events` | `resolveKafkaTopic(EventType.ORDER_CREATED)` → `order.events` |

### Checklist — Retry (producer)

| Item | Implementation |
|------|----------------|
| Configure producer retry | KafkaJS `producer.retry` in `getKafkaClientConfig()` |
| Configure retry backoff | `publishWithProducerRetry()` + `KAFKA_PRODUCER_RETRY_*` / `KAFKA_RETRY_*` |
| Reliable publishing | `emitKafkaEvent()` — do not use `firstValueFrom(emit())` |
| Producer-only client | `producerOnlyMode: true` in `getKafkaClientConfig()` |
| Handle publish failure | `OrdersService` marks order `failed`; logs error; HTTP 500 |
| Empty env vars | **Forbidden** `KAFKA_RETRY_*=` empty → `maxAttempts: NaN` (see [RULES.md §6](./RULES.md#6-producer-kafka-publishing)) |

Env: `KAFKA_PRODUCER_RETRY_MAX_ATTEMPTS`, `KAFKA_PRODUCER_RETRY_BASE_DELAY_MS`, `KAFKA_PRODUCER_RETRY_MAX_DELAY_MS`, `KAFKA_PRODUCER_RETRY_BACKOFF_MULTIPLIER`.

### Checklist — Logs

| Item | Implementation |
|------|----------------|
| Log order creation | `OrdersService` — `Order created: orderId=...` |
| Log Kafka publish | `EventPublisher` — publish start + success |
| Log failures | `EventPublisher` + `OrdersService` — `logger.error` with stack |

### Checklist — Tests (full flow)

| Step | Command |
|------|---------|
| Start Kafka | `podman-compose -f docker/docker-compose.yml up -d` |
| Start order-service | `npm run start:order` (or container) |
| POST /orders | `curl -X POST http://localhost:3001/orders -H 'Content-Type: application/json' -d '{...}'` |
| Validate in Kafka UI | Topic `order.events` — key = `orderId`, value = envelope JSON |
| Automate validation | `npm run verify:order-kafka` or `KAFKA_INTEGRATION_TEST=true npm run test:integration -w order-service` |

### Checklist — PaymentService (distributed simulation)

| Item | Implementation |
|------|----------------|
| Create PaymentService | `payment.service.ts` — `processPayment()` |
| Simulate approval (~80%) | `payment.processed` + log `[PAYMENT SUCCESS]` |
| Simulate random failure (~20%) | `Math.random() < PAYMENT_FAILURE_RATE` → `payment.failed` |
| Business rules | `payment-rules.ts` — rejection before simulated gateway |
| Failure types | `timeout`, `gateway_unavailable`, `card_declined`, `connection_reset` |

**Scenario 1 — success:** `payment.processed`  
**Scenario 2 — failure:** `payment.failed`

**Structured logs (grep in terminal):**

```
[PAYMENT SUCCESS] orderId=... paymentId=... status=approved
[PAYMENT FAILED]  orderId=... reason=Payment gateway timeout
```

**Kafka UI:** topic `payment.events` with `eventType` = `payment.processed` or `payment.failed`.

### Checklist — Result publishing (payment.events)

| Item | Implementation |
|------|----------------|
| `payment.processed` — create event | `createEventEnvelope` in `publishPaymentProcessed()` |
| Publish success to `payment.events` | `resolveKafkaTopic(PAYMENT_PROCESSED)` → `payment.events` |
| `payment.failed` — create event | `createEventEnvelope` in `publishPaymentFailed()` |
| Publish failure to `payment.events` | `resolveKafkaTopic(PAYMENT_FAILED)` → `payment.events` |

Consumers: **stock-service** filters `payment.processed`; **order-service** and **notification-service** filter `payment.failed`.

### Checklist — Retry (consumer)

| Item | Implementation |
|------|----------------|
| Configure consumer retry | `KafkaRetryRunner` + `KafkaRetryExecutor` on each `@EventPattern` handler |
| Configure backoff | Exponential: `min(baseDelay × multiplier^(attempt-1), maxDelay)` |
| Control attempts | `KAFKA_CONSUMER_RETRY_MAX_ATTEMPTS` (default **3**) |

**Flow:** handler fails → sleep(backoff) → republish to same topic with `x-retry-count` → after max attempts → `{topic}.dlq`

**Env:** `KAFKA_CONSUMER_RETRY_MAX_ATTEMPTS`, `KAFKA_CONSUMER_RETRY_BASE_DELAY_MS`, `KAFKA_CONSUMER_RETRY_MAX_DELAY_MS`, `KAFKA_CONSUMER_RETRY_BACKOFF_MULTIPLIER` (or `KAFKA_RETRY_*`).

**Logs:** `[KAFKA CONSUMER RETRY]` at startup · `[KAFKA RETRY]` on each retry · `[KAFKA DLQ]` when attempts are exhausted.

Example with defaults: attempts **1s → 2s → DLQ** (3 attempts total).

Env: `PAYMENT_FAILURE_RATE=0.2`, `PAYMENT_FORCE_FAILURE`, `PAYMENT_TIMEOUT_DELAY_MS`, `PAYMENT_MIN_AMOUNT`, `PAYMENT_MAX_AMOUNT`.

### Checklist — Idempotency (payment-service)

| Item | Implementation |
|------|----------------|
| Single processing | `processed_events.inbound_event_id` (PK) |
| Avoid duplicate payment | blocks 2nd `approved` for the same `orderId` |
| Persist processed events | SQLite `PAYMENT_DATABASE_PATH` — `ProcessedEventsService` |

### Checklist — Logs (payment-service)

| Log | Tag |
|-----|-----|
| Event received | `[EVENT RECEIVED]` |
| Approval | `[PAYMENT SUCCESS]` |
| Failure | `[PAYMENT FAILED]` |
| Consumer retry | `[KAFKA RETRY]` / `[PAYMENT RETRY]` |
| Idempotency | `[IDEMPOTENCY SKIP]` |

### Checklist — Tests (full flow)

| Step | Command / validation |
|------|---------------------|
| Publish order.created | `POST /orders` or `npm run verify:order-kafka` |
| Consume + simulate payment | `npm run start:payment` (logs `[EVENT RECEIVED]`) |
| Publish payment.processed/failed | automatic on `payment.events` |
| Validate Kafka UI | `npm run verify:payment-flow` (with `PAYMENT_FAILURE_RATE=0` for success) |

### Checklist — Consume `order.events` (payment-service)

| Item | Implementation |
|------|----------------|
| Consume topic `order.events` | `@EventPattern(OrderKafkaTopic.ORDER_EVENTS)` |
| Filter `order.created` | `parseOrderEventsMessage()` → `kind: 'skipped'` for other types |
| Deserialize payload | `deserializeKafkaPayload()` (Buffer / string / object) + `extractEnvelope()` |
| Validate event structure | `validateEventEnvelope()` + `validateOrderCreatedPayload()` |

Code: `shared/src/kafka/consume-order-events.ts`, `payment-events.consumer.ts`.

### Checklist — Kafka consumer

| Item | Implementation |
|------|----------------|
| Configure Kafka consumer | `app.connectMicroservice({ transport: KAFKA, options: getKafkaConsumerConfig(service) })` |
| Configure consumer group | `resolveConsumerGroup()` → default `eventflow.<service>` (`shared/src/kafka/consumer-groups.ts`) |
| Connect to Kafka broker | `KAFKA_BOOTSTRAP_SERVERS` → `client.brokers` in `getKafkaConsumerConfig()` |

**Consumer groups (default):**

| Service | Group ID |
|---------|----------|
| order-service | `eventflow.order-service` |
| payment-service | `eventflow.payment-service` |
| stock-service | `eventflow.stock-service` |
| notification-service | `eventflow.notification-service` |
| dlq-service | `eventflow.dlq-service` |

Startup log example: `Kafka consumer: service=payment-service brokers=[localhost:9092] groupId=eventflow.payment-service ...`

**NestJS on broker:** the group appears as `eventflow.<service>-server` (e.g. `eventflow.payment-service-server`). See [RULES.md §4](./RULES.md#4-consumer-groups-and-horizontal-scaling).

### Checklist — Horizontal scaling (payment-service consumers)

| Item | Status | Implementation |
|------|--------|----------------|
| Run multiple instances | ✅ | `payment-service-1`, `payment-service-2` (+ optional `payment-service-3`) in `docker/services/docker-compose.yml` |
| Configure same `groupId` | ✅ | All replicas: `KAFKA_CONSUMER_GROUP_PAYMENT_SERVICE=eventflow.payment-service` (default in `consumer-groups.ts`) |
| Partition balancing | ✅ | Kafka assigns `order.events` partitions among group members (max = topic partition count, default **3**) |
| Distinct `clientId` per instance | ✅ | `resolveConsumerClientId()` appends container `HOSTNAME` (`payment-service-consumer-<id>`) |
| Idempotency across replicas | ✅ | Docker volume `payment-idempotency` → shared `/data/payments.sqlite` |

**Rule:** replicas of the **same** service share **one** `groupId`. Never use a different `groupId` per instance (that would duplicate processing).

**Start (Docker):**

```bash
podman-compose -f docker/services/docker-compose.yml up -d --build payment-service-1 payment-service-2
# or
npm run docker:payment-instances
```

**Validate in Kafka UI:** Consumers → `eventflow.payment-service` → **2+ members**.

**Validate via script:**

```bash
npm run verify:payment-scale
```

### Checklist — Partitions, distribution, and balancing

| Item | Command / validation |
|------|---------------------|
| Create multiple partitions | `npm run kafka:partitions` (or `kafka-init` with `KAFKA_TOPIC_PARTITIONS=3`) |
| Test distribution | `npm run verify:kafka-partitions` — histogram per partition |
| Validate ordering by key | same script — 5 produces with same key → same partition; `orderId` = Kafka key |
| Test balancing | same script — consumer group with 2+ members on `order.events` |

```bash
# 1) Increase partitions (host)
npm run kafka:partitions

# 2) Rebalance consumers
podman-compose -f docker/services/docker-compose.yml restart payment-service-1 payment-service-2

# 3) Validate
npm run verify:kafka-partitions
```

**Rule:** horizontal throughput ≤ number of partitions; **per-order ordering** = always `key = orderId`.

**Expected logs (each instance):**

```
Kafka consumer: service=payment-service ... groupId=eventflow.payment-service clientId=payment-service-consumer-<hostname>
```

**Local development (2 terminals, same group):**

```bash
# Terminal 1
PORT=3002 npm run start:payment

# Terminal 2 — same groupId (default), different clientId
PORT=3006 KAFKA_CONSUMER_CLIENT_ID_SUFFIX=instance-2 npm run start:payment
```

### Checklist — Idempotency

| Item | Implementation |
|------|----------------|
| Strategy | `IdempotencyStrategy.EVENT_ID_PER_AGGREGATE` — one `eventId` per order |
| Do not duplicate events | Skip publish if `orders.event_id` already set; payment consumer dedupes by `eventId` |
| Key `orderId` | Kafka message key = `orderId` (`resolvePartitionKey`) |

---

## Event envelope (standard structure)

Every published message uses `EventEnvelope<T>`:

| Field | Rule |
|-------|------|
| `eventId` | UUID v4; unique per message; idempotency key |
| `eventType` | Canonical event name (e.g. `order.created`) |
| Kafka topic | Physical topic (e.g. `order.events` for `order.created`) — see `resolveKafkaTopic()` |
| `version` | Schema version (`1.0`) |
| `timestamp` | ISO-8601 UTC at publish time |
| `correlationId` | Business flow id — **use `orderId`** for order flows |
| `causationId` | Optional; `eventId` of the causing event |
| `source` | Producing service (e.g. `order-service`) |
| `payload` | Domain data; **must include `orderId`** |

Factory: `createEventEnvelope()` — auto-generates `eventId`, `timestamp`, and default `version`.

```json
{
  "eventId": "550e8400-e29b-41d4-a716-446655440000",
  "eventType": "order.created",
  "version": "1.0",
  "timestamp": "2026-05-19T12:00:00.000Z",
  "correlationId": "order-uuid",
  "causationId": "optional-parent-event-uuid",
  "source": "order-service",
  "payload": { "orderId": "order-uuid", "...": "..." }
}
```

---

## Kafka topics

### Base topics

Aggregate streams (`order.events`, `payment.events`) plus one topic per other event type. Created by `kafka-init`; `order.events` / `payment.events` are altered to **3 partitions** if they already existed with fewer. See [RULES.md §3](./RULES.md#3-partitions-key-and-ordering).

| Setting | Default | Env variable |
|---------|---------|--------------|
| Partitions | 3 | `KAFKA_TOPIC_PARTITIONS` |
| Replication | 1 | `KAFKA_TOPIC_REPLICATION_FACTOR` |
| Retention | 7 days | `KAFKA_TOPIC_RETENTION_MS` |
| Cleanup | delete | `KAFKA_TOPIC_CLEANUP_POLICY` |

### Partition strategy

- **Message key:** `orderId` (field `PARTITION_KEY_FIELD` in code)
- **Partitions:** 3 per topic (configurable)
- **Effect:** All events for the same order hash to the same partition → **per-order ordering**
- **Scaling:** Consumer groups can run up to N consumers where N = partition count

Code: `resolvePartitionKey(envelope)` in `shared/src/kafka/partition-key.ts`.

### Data retention

- `retention.ms` — how long messages are kept (default: 604800000 = 7 days)
- `cleanup.policy=delete` — old segments are removed after retention
- DLQ topics use the same retention policy

Adjust via `.env` or Docker environment variables before running `kafka-init`.

---

## Related docs

- [RULES.md](./RULES.md) — **all system rules** (consolidated reference)
- [EVENT_CATALOG.md](./EVENT_CATALOG.md) — full event list and service ownership
- [RETRY_DLQ.md](./RETRY_DLQ.md) — retry and dead-letter handling

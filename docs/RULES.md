# EventFlow System Rules

Consolidated reference for all architecture, Kafka, Docker, and operations rules defined for the project.  
Related documents: [EVENT_MODELING.md](./EVENT_MODELING.md), [EVENT_CATALOG.md](./EVENT_CATALOG.md), [RETRY_DLQ.md](./RETRY_DLQ.md).

---

## 1. Business flow (core)

| Order | `eventType` | Kafka topic | Producer | Consumer(s) |
|-------|-------------|--------------|----------|-------------|
| 1 | `order.created` | **`order.events`** | order-service | payment-service |
| 2 | `payment.processed` | **`payment.events`** | payment-service | stock-service |
| 3 | `stock.reserved` | `stock.reserved` | stock-service | notification-service |
| 4 | `notification.sent` | `notification.sent` | notification-service | order-service |

**Failure branch:** `payment.failed` → topic **`payment.events`** → order-service, notification-service.

```
order.created → payment.processed → stock.reserved → notification.sent
                      ↓
               payment.failed
```

**Rule:** the `eventType` field in the envelope stays canonical (`order.created`, `payment.processed`, …). The **physical topic** is resolved by `resolveKafkaTopic()` in `shared/src/events/resolve-kafka-topic.ts`.

| `eventType` | Physical topic (override) |
|-------------|-------------------------|
| `order.created` | `order.events` |
| `payment.processed` | `payment.events` |
| `payment.failed` | `payment.events` |
| Others | same as `eventType` (e.g. `stock.reserved`) |

**Legacy:** old messages may exist on topic `order.created`. payment-service **only** consumes `order.events`. After a model change, create new orders or reprocess manually.

---

## 2. Envelope and correlation

| Field | Rule |
|-------|--------|
| `eventId` | UUID v4; unique per message; idempotency key |
| `correlationId` | **`orderId`** in order flows |
| `causationId` | Optional; `eventId` of the causing event |
| `timestamp` | ISO-8601 UTC at publish |
| `version` | `1.0` (default) |
| `source` | Producing service name (`order-service`, …) |
| `payload.orderId` | Required in order-flow events |

Factory: `createEventEnvelope()` in `shared/src/events/create-envelope.ts`.

---

## 3. Partitions, key, and ordering

| Rule | Detail |
|-------|---------|
| **Partition key** | Always `orderId` (`resolvePartitionKey`) |
| **Default partitions** | **3** on topics created by `kafka-init` |
| **Aggregated topics with alter** | `order.events`, `payment.events` (script ensures ≥ 3 partitions) |
| **Ordering** | Events for the **same order** → **same partition** → order preserved within the partition |
| **Horizontal throughput** | Max useful consumers in the group = **number of partitions** on the topic |

**Commands:**

```bash
npm run kafka:partitions          # alter → 3 partitions
npm run verify:kafka-partitions   # distribution + key + balancing
```

After altering partitions: `restart payment-service-1 payment-service-2` to rebalance.

---

## 4. Consumer groups and horizontal scaling

### 4.1 Group IDs (configuration)

| Service | Configured `groupId` (`resolveConsumerGroup`) |
|---------|-----------------------------------------------|
| order-service | `eventflow.order-service` |
| payment-service | `eventflow.payment-service` |
| stock-service | `eventflow.stock-service` |
| notification-service | `eventflow.notification-service` |
| dlq-service | `eventflow.dlq-service` |

Per-service override: `KAFKA_CONSUMER_GROUP_<SERVICE>` (e.g. `KAFKA_CONSUMER_GROUP_PAYMENT_SERVICE`).

### 4.2 NestJS — `-server` suffix

The Nest Kafka microservice registers the consumer with a **`-server`** suffix on the broker.

| Config / app log | Group on broker (Kafka UI / CLI) |
|------------------|----------------------------------|
| `eventflow.payment-service` | **`eventflow.payment-service-server`** |

Balance verification scripts use `eventflow.payment-service-server`.

### 4.3 Multiple payment-service instances

| Rule | Value |
|-------|--------|
| Same `groupId` on all replicas | **Required** — otherwise the same event is processed more than once |
| Distinct `clientId` per instance | `payment-service-consumer-<HOSTNAME>` (automatic via container `HOSTNAME`) |
| **Do not** mix Docker modes | Do not run `payment-service-1/2` **and** `payment-service` with `--scale` at the same time |

**Docker (default):**

| Container | HTTP host | Consumer |
|-----------|-----------|----------|
| `payment-service-1` | port **3002** | yes |
| `payment-service-2` | no published port | yes |
| `payment-service-3` | profile `payment-scale-3` | yes |

```bash
npm run docker:payment-instances
npm run verify:payment-scale
```

**Balancing (example with 3 partitions on `order.events`):**

```text
payment-service-1 → order.events(2), order.cancelled(0,1)
payment-service-2 → order.events(0,1), order.cancelled(2)
```

Order consumption logs: check **`payment-service-2`** (or any instance assigned an `order.events` partition).

```bash
podman exec eventflow-kafka kafka-consumer-groups \
  --bootstrap-server kafka:29092 \
  --describe --group eventflow.payment-service-server --members --verbose
```

### 4.4 Idempotency across replicas (payment)

| Item | Rule |
|------|--------|
| Storage | SQLite at `PAYMENT_DATABASE_PATH` |
| Docker | Shared volume **`payment-idempotency`** → `/data/payments.sqlite` |
| Key | `processed_events.inbound_event_id` (PK) |
| Same `orderId` | Blocks a second `approved` payment |

**Demo:** a shared volume is acceptable for learning; in production use Postgres/Redis for many replicas.

---

## 5. Idempotency (order-service)

| Rule | Implementation |
|-------|----------------|
| Before publishing | Persist `orders.event_id`; if it already exists, do not republish |
| Business key | `orderId` + inbound `eventId` on payment |

---

## 6. Producer (Kafka publishing)

| Rule | Implementation |
|-------|----------------|
| Topic | `resolveKafkaTopic(eventType)` |
| Key | `resolvePartitionKey(envelope)` → `orderId` |
| App-level retry | `publishWithProducerRetry()` |
| Transport retry | `producer.retry` in `getKafkaClientConfig()` |
| Reliable publish | **`emitKafkaEvent()`** — `lastValueFrom(emit().pipe(defaultIfEmpty))`; **do not** use `firstValueFrom(emit())` |
| Producer client | **`producerOnlyMode: true`** — avoids an extra consumer on `ClientKafka` |
| Failure after retries | order-service marks order `failed`; HTTP **500** |

### 6.1 Retry variables — empty values

| Rule | Detail |
|-------|---------|
| **Forbidden** | `KAFKA_RETRY_MAX_ATTEMPTS=` (empty) in `.env` |
| Effect of invalid value | `maxAttempts: NaN` → publish fails with error `undefined` |
| Fix | Use numbers (`3`, `1000`, …) or **omit** the variable |

Variables: `KAFKA_PRODUCER_RETRY_*` with fallback to `KAFKA_RETRY_*` (see [RETRY_DLQ.md](./RETRY_DLQ.md)).

---

## 7. Consumer (retry and DLQ)

| Rule | Default |
|-------|----------------|
| Max attempts | **3** |
| Backoff | exponential: `1s → 2s → DLQ` |
| After exhaustion | publish to `{topic}.dlq` |

Env: `KAFKA_CONSUMER_RETRY_*` or `KAFKA_RETRY_*`.

**Logs:** `[KAFKA CONSUMER RETRY]`, `[KAFKA RETRY]`, `[KAFKA DLQ]`, `[EVENT RECEIVED]`, `[IDEMPOTENCY SKIP]`.

Invalid handlers (invalid envelope): ack without retry (avoids poison loop).

---

## 8. Payment-service — simulation

| Variable | Default | Effect |
|----------|--------|--------|
| `PAYMENT_FAILURE_RATE` | `0.2` | ~80% `payment.processed`, ~20% `payment.failed` |
| `PAYMENT_FORCE_FAILURE` | — | forces failure |
| Business rules | `payment-rules.ts` | validation before simulated gateway |

Simulated failure types: `timeout`, `gateway_unavailable`, `card_declined`, `connection_reset`.

Deterministic success test: `PAYMENT_FAILURE_RATE=0 npm run verify:payment-flow`.

---

## 9. Application bootstrap (HTTP + Kafka)

Every domain service follows the hybrid pattern in `main.ts`:

1. `NestFactory.create(AppModule)`
2. `app.connectMicroservice({ transport: KAFKA, options: getKafkaConsumerConfig(service) })`
3. `await app.startAllMicroservices()` — **before** `app.listen()`
4. Log: `formatKafkaConsumerBootstrap(consumerInfo)`

**Docker rule:** after changing code, **image rebuild** is mandatory. An old payment-service image without `connectMicroservice` only starts HTTP and **does not consume** Kafka.

```bash
npm run docker:rebuild-services
```

---

## 10. Docker and network

| Context | `KAFKA_BOOTSTRAP_SERVERS` |
|----------|---------------------------|
| Apps on host | `localhost:9092` |
| Apps on `eventflow-network` | `kafka:29092` |

| Infra service | Host port |
|---------------|------------|
| Kafka | 9092 |
| Kafka UI | 8080 |
| order-service | 3001 |
| payment-service-1 | 3002 |

`kafka-init`: creates topics with `--config retention.ms=…` and `--config cleanup.policy=…` (separate flags, not CSV in a single `--config`).

---

## 11. API `POST /orders`

**Endpoint:** `POST http://localhost:3001/orders` → **201**

```bash
curl -sS -X POST http://localhost:3001/orders \
  -H 'Content-Type: application/json' \
  -d '{
    "customerId": "customer-demo",
    "currency": "BRL",
    "items": [{ "productId": "sku-1", "quantity": 1, "unitPrice": 49.9 }]
  }'
```

| Field | Rule |
|-------|--------|
| `customerId` | non-empty string |
| `items` | array with ≥ 1 item |
| `items[].quantity` | integer ≥ 1 |
| `items[].unitPrice` | positive number, up to 2 decimal places |
| `currency` | optional: `BRL`, `USD`, `EUR` (default `BRL`) |

**Response:** `orderId`, `status`, `totalAmount`, `currency`, `eventId`, `correlationId`.

---

## 12. npm validation scripts

| Script | Purpose |
|--------|--------|
| `npm run verify:order-kafka` | POST /orders + message on `order.events` |
| `npm run verify:payment-flow` | Flow order → payment → Kafka |
| `npm run verify:payment-scale` | ≥ 2 members in consumer group |
| `npm run verify:kafka-partitions` | Partitions, key, distribution, balancing |
| `npm run kafka:partitions` | Alters `order.events` / `payment.events` to 3 partitions |
| `npm run docker:payment-instances` | Starts payment replicas (named or scale) |
| `npm run docker:rebuild-services` | Rebuild + recreate containers |

---

## 13. Troubleshooting

| Symptom | Likely cause | Action |
|---------|----------------|------|
| Payment with no consumption logs | Old Docker image (no Kafka consumer) | `npm run docker:rebuild-services` |
| Payment without `[EVENT RECEIVED]` | Logs on wrong instance or 1 partition | `kafka-consumer-groups --describe`; `npm run kafka:partitions` + restart |
| `Published` on order but payment does not react | Messages on `order.created` (legacy) | New orders after rebuild; correct topic is `order.events` |
| `Kafka publish failed … undefined` | Empty `KAFKA_RETRY_*` → NaN | Fix `.env` or omit variables |
| `order.events` with 1 partition | Topic auto-created before init | `npm run kafka:partitions` |
| Two consumers duplicate events | Different `groupId` per instance | Same `KAFKA_CONSUMER_GROUP_PAYMENT_SERVICE` |

**Healthy startup (payment):**

```text
Kafka consumer: service=payment-service ... groupId=eventflow.payment-service
Kafka consumer microservice started
```

---

## 14. Structured logs (grep)

| Service | Tags |
|---------|------|
| order | `Order created`, `Publishing order.created`, `Kafka publish succeeded` |
| payment | `[EVENT RECEIVED]`, `[PAYMENT STARTED]`, `[PAYMENT SUCCESS]`, `[PAYMENT FAILED]`, `[IDEMPOTENCY SKIP]` |
| kafka | `[KAFKA RETRY]`, `[KAFKA DLQ]`, `[KAFKA CONSUMER RETRY]` |

# Event Modeling

Design reference for EventFlow domain events, Kafka topics, and message standards.

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
| Criar event envelope | `createEventEnvelope()` in `OrdersService.createOrder` |
| Gerar `eventId` | UUID via `createEventEnvelope` (stored on `orders.event_id`) |
| Gerar `correlationId` | `orderId` (same value for the whole saga) |
| Adicionar `timestamp` | ISO-8601 UTC default in `createEventEnvelope` |
| Publicar no tópico `order.events` | `resolveKafkaTopic(EventType.ORDER_CREATED)` → `order.events` |

### Checklist — Retry (producer)

| Item | Implementation |
|------|----------------|
| Configurar retry do producer | KafkaJS `producer.retry` in `getKafkaClientConfig()` |
| Configurar retry backoff | `publishWithProducerRetry()` + `KAFKA_PRODUCER_RETRY_*` / `KAFKA_RETRY_*` |
| Tratar falha de publicação | `OrdersService` marks order `failed`; logs error; HTTP 500 |

Env: `KAFKA_PRODUCER_RETRY_MAX_ATTEMPTS`, `KAFKA_PRODUCER_RETRY_BASE_DELAY_MS`, `KAFKA_PRODUCER_RETRY_MAX_DELAY_MS`, `KAFKA_PRODUCER_RETRY_BACKOFF_MULTIPLIER`.

### Checklist — Logs

| Item | Implementation |
|------|----------------|
| Logar criação de pedido | `OrdersService` — `Order created: orderId=...` |
| Logar publicação Kafka | `EventPublisher` — publish start + success |
| Logar falhas | `EventPublisher` + `OrdersService` — `logger.error` with stack |

### Checklist — Testes (fluxo completo)

| Step | Command |
|------|---------|
| Subir Kafka | `podman-compose -f docker/docker-compose.yml up -d` |
| Subir order-service | `npm run start:order` (ou container) |
| POST /orders | `curl -X POST http://localhost:3001/orders -H 'Content-Type: application/json' -d '{...}'` |
| Validar no Kafka UI | Tópico `order.events` — key = `orderId`, value = envelope JSON |
| Automatizar validação | `npm run verify:order-kafka` ou `KAFKA_INTEGRATION_TEST=true npm run test:integration -w order-service` |

### Checklist — Idempotência

| Item | Implementation |
|------|----------------|
| Strategy | `IdempotencyStrategy.EVENT_ID_PER_AGGREGATE` — one `eventId` per order |
| Não duplicar eventos | Skip publish if `orders.event_id` already set; payment consumer dedupes by `eventId` |
| Chave `orderId` | Kafka message key = `orderId` (`resolvePartitionKey`) |

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

One topic per event type (11 base + 11 DLQ). Created automatically by `kafka-init` on `docker compose up`.

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

- [EVENT_CATALOG.md](./EVENT_CATALOG.md) — full event list and service ownership
- [RETRY_DLQ.md](./RETRY_DLQ.md) — retry and dead-letter handling

# Event Modeling

Design reference for EventFlow domain events, Kafka topics, and message standards.

> **Regras completas (fonte única):** [RULES.md](./RULES.md) — fluxo, tópicos agregados, partições, escalabilidade, producer/consumer, Docker e troubleshooting.

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
| Publicação confiável | `emitKafkaEvent()` — não usar `firstValueFrom(emit())` |
| Producer-only client | `producerOnlyMode: true` em `getKafkaClientConfig()` |
| Tratar falha de publicação | `OrdersService` marks order `failed`; logs error; HTTP 500 |
| Env vazias | **Proibido** `KAFKA_RETRY_*=` vazio → `maxAttempts: NaN` (ver [RULES.md §6](./RULES.md#6-producer-publicação-kafka)) |

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

### Checklist — PaymentService (simulação distribuída)

| Item | Implementation |
|------|----------------|
| Criar PaymentService | `payment.service.ts` — `processPayment()` |
| Simular aprovação (~80%) | `payment.processed` + log `[PAYMENT SUCCESS]` |
| Simular falha aleatória (~20%) | `Math.random() < PAYMENT_FAILURE_RATE` → `payment.failed` |
| Regras de negócio | `payment-rules.ts` — recusa antes do gateway simulado |
| Tipos de falha | `timeout`, `gateway_unavailable`, `card_declined`, `connection_reset` |

**Cenário 1 — sucesso:** `payment.processed`  
**Cenário 2 — falha:** `payment.failed`

**Logs estruturados (grep no terminal):**

```
[PAYMENT SUCCESS] orderId=... paymentId=... status=approved
[PAYMENT FAILED]  orderId=... reason=Payment gateway timeout
```

**Kafka UI:** tópico `payment.events` com `eventType` = `payment.processed` ou `payment.failed`.

### Checklist — Publicação de resultado (payment.events)

| Item | Implementation |
|------|----------------|
| `payment.processed` — criar evento | `createEventEnvelope` em `publishPaymentProcessed()` |
| Publicar sucesso em `payment.events` | `resolveKafkaTopic(PAYMENT_PROCESSED)` → `payment.events` |
| `payment.failed` — criar evento | `createEventEnvelope` em `publishPaymentFailed()` |
| Publicar falha em `payment.events` | `resolveKafkaTopic(PAYMENT_FAILED)` → `payment.events` |

Consumidores: **stock-service** filtra `payment.processed`; **order-service** e **notification-service** filtram `payment.failed`.

### Checklist — Retry (consumer)

| Item | Implementation |
|------|----------------|
| Configurar retry do consumer | `KafkaRetryRunner` + `KafkaRetryExecutor` em cada `@EventPattern` handler |
| Configurar backoff | Exponencial: `min(baseDelay × multiplier^(attempt-1), maxDelay)` |
| Controlar tentativas | `KAFKA_CONSUMER_RETRY_MAX_ATTEMPTS` (default **3**) |

**Fluxo:** handler falha → sleep(backoff) → republica no mesmo tópico com `x-retry-count` → após max tentativas → `{topic}.dlq`

**Env:** `KAFKA_CONSUMER_RETRY_MAX_ATTEMPTS`, `KAFKA_CONSUMER_RETRY_BASE_DELAY_MS`, `KAFKA_CONSUMER_RETRY_MAX_DELAY_MS`, `KAFKA_CONSUMER_RETRY_BACKOFF_MULTIPLIER` (ou `KAFKA_RETRY_*`).

**Logs:** `[KAFKA CONSUMER RETRY]` no startup · `[KAFKA RETRY]` em cada retry · `[KAFKA DLQ]` ao esgotar tentativas.

Exemplo com defaults: tentativas **1s → 2s → DLQ** (3 tentativas totais).

Env: `PAYMENT_FAILURE_RATE=0.2`, `PAYMENT_FORCE_FAILURE`, `PAYMENT_TIMEOUT_DELAY_MS`, `PAYMENT_MIN_AMOUNT`, `PAYMENT_MAX_AMOUNT`.

### Checklist — Idempotência (payment-service)

| Item | Implementation |
|------|----------------|
| Processamento único | `processed_events.inbound_event_id` (PK) |
| Evitar pagamento duplicado | bloqueia 2º `approved` para mesmo `orderId` |
| Persistir eventos processados | SQLite `PAYMENT_DATABASE_PATH` — `ProcessedEventsService` |

### Checklist — Logs (payment-service)

| Log | Tag |
|-----|-----|
| Recebimento do evento | `[EVENT RECEIVED]` |
| Aprovação | `[PAYMENT SUCCESS]` |
| Falha | `[PAYMENT FAILED]` |
| Retry consumer | `[KAFKA RETRY]` / `[PAYMENT RETRY]` |
| Idempotência | `[IDEMPOTENCY SKIP]` |

### Checklist — Testes (fluxo completo)

| Step | Command / validação |
|------|---------------------|
| Publicar order.created | `POST /orders` ou `npm run verify:order-kafka` |
| Consumir + simular pagamento | `npm run start:payment` (logs `[EVENT RECEIVED]`) |
| Publicar payment.processed/failed | automático em `payment.events` |
| Validar Kafka UI | `npm run verify:payment-flow` (com `PAYMENT_FAILURE_RATE=0` para sucesso) |

### Checklist — Consumir `order.events` (payment-service)

| Item | Implementation |
|------|----------------|
| Consumir tópico `order.events` | `@EventPattern(OrderKafkaTopic.ORDER_EVENTS)` |
| Filtrar `order.created` | `parseOrderEventsMessage()` → `kind: 'skipped'` para outros tipos |
| Deserializar payload | `deserializeKafkaPayload()` (Buffer / string / object) + `extractEnvelope()` |
| Validar estrutura do evento | `validateEventEnvelope()` + `validateOrderCreatedPayload()` |

Código: `shared/src/kafka/consume-order-events.ts`, `payment-events.consumer.ts`.

### Checklist — Kafka consumer

| Item | Implementation |
|------|----------------|
| Configurar Kafka consumer | `app.connectMicroservice({ transport: KAFKA, options: getKafkaConsumerConfig(service) })` |
| Configurar consumer group | `resolveConsumerGroup()` → default `eventflow.<service>` (`shared/src/kafka/consumer-groups.ts`) |
| Conectar ao broker Kafka | `KAFKA_BOOTSTRAP_SERVERS` → `client.brokers` in `getKafkaConsumerConfig()` |

**Consumer groups (default):**

| Service | Group ID |
|---------|----------|
| order-service | `eventflow.order-service` |
| payment-service | `eventflow.payment-service` |
| stock-service | `eventflow.stock-service` |
| notification-service | `eventflow.notification-service` |
| dlq-service | `eventflow.dlq-service` |

Startup log example: `Kafka consumer: service=payment-service brokers=[localhost:9092] groupId=eventflow.payment-service ...`

**NestJS no broker:** o grupo aparece como `eventflow.<service>-server` (ex.: `eventflow.payment-service-server`). Ver [RULES.md §4](./RULES.md#4-consumer-groups-e-escalabilidade-horizontal).

### Checklist — Escalabilidade horizontal (payment-service consumers)

| Item | Status | Implementation |
|------|--------|----------------|
| Subir múltiplas instâncias | ✅ | `payment-service-1`, `payment-service-2` (+ opcional `payment-service-3`) em `docker/services/docker-compose.yml` |
| Configurar mesmo `groupId` | ✅ | Todas as réplicas: `KAFKA_CONSUMER_GROUP_PAYMENT_SERVICE=eventflow.payment-service` (default em `consumer-groups.ts`) |
| Balanceamento de partições | ✅ | Kafka atribui partições de `order.events` entre membros do grupo (máx. = número de partições do tópico, default **3**) |
| `clientId` distinto por instância | ✅ | `resolveConsumerClientId()` acrescenta `HOSTNAME` do container (`payment-service-consumer-<id>`) |
| Idempotência entre réplicas | ✅ | Volume Docker `payment-idempotency` → `/data/payments.sqlite` compartilhado |

**Regra:** réplicas do **mesmo** serviço compartilham **um** `groupId`. Nunca use `groupId` diferente por instância (isso duplicaria o processamento).

**Subir (Docker):**

```bash
podman-compose -f docker/services/docker-compose.yml up -d --build payment-service-1 payment-service-2
# ou
npm run docker:payment-instances
```

**Validar no Kafka UI:** Consumers → `eventflow.payment-service` → **2+ members**.

**Validar via script:**

```bash
npm run verify:payment-scale
```

### Checklist — Partições, distribuição e balanceamento

| Item | Comando / validação |
|------|---------------------|
| Criar múltiplas partitions | `npm run kafka:partitions` (ou `kafka-init` com `KAFKA_TOPIC_PARTITIONS=3`) |
| Testar distribuição | `npm run verify:kafka-partitions` — histograma por partition |
| Validar ordenação por key | mesmo script — 5 produces com mesma key → mesma partition; `orderId` = Kafka key |
| Testar balanceamento | mesmo script — consumer group com 2+ members em `order.events` |

```bash
# 1) Aumentar partitions (host)
npm run kafka:partitions

# 2) Rebalancear consumers
podman-compose -f docker/services/docker-compose.yml restart payment-service-1 payment-service-2

# 3) Validar
npm run verify:kafka-partitions
```

**Regra:** throughput horizontal ≤ número de partitions; **ordenacao por pedido** = sempre `key = orderId`.

**Logs esperados (cada instância):**

```
Kafka consumer: service=payment-service ... groupId=eventflow.payment-service clientId=payment-service-consumer-<hostname>
```

**Desenvolvimento local (2 terminais, mesmo grupo):**

```bash
# Terminal 1
PORT=3002 npm run start:payment

# Terminal 2 — mesmo groupId (default), clientId diferente
PORT=3006 KAFKA_CONSUMER_CLIENT_ID_SUFFIX=instance-2 npm run start:payment
```

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

Aggregate streams (`order.events`, `payment.events`) plus one topic per other event type. Created by `kafka-init`; `order.events` / `payment.events` are altered to **3 partitions** if they already existed with fewer. See [RULES.md §3](./RULES.md#3-partições-chave-e-ordenação).

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

- [RULES.md](./RULES.md) — **todas as regras** do sistema (referência consolidada)
- [EVENT_CATALOG.md](./EVENT_CATALOG.md) — full event list and service ownership
- [RETRY_DLQ.md](./RETRY_DLQ.md) — retry and dead-letter handling

# Regras do sistema EventFlow

Referência consolidada de todas as regras de arquitetura, Kafka, Docker e operação definidas para o projeto.  
Documentos relacionados: [EVENT_MODELING.md](./EVENT_MODELING.md), [EVENT_CATALOG.md](./EVENT_CATALOG.md), [RETRY_DLQ.md](./RETRY_DLQ.md).

---

## 1. Fluxo de negócio (core)

| Ordem | `eventType` | Tópico Kafka | Producer | Consumer(s) |
|-------|-------------|--------------|----------|-------------|
| 1 | `order.created` | **`order.events`** | order-service | payment-service |
| 2 | `payment.processed` | **`payment.events`** | payment-service | stock-service |
| 3 | `stock.reserved` | `stock.reserved` | stock-service | notification-service |
| 4 | `notification.sent` | `notification.sent` | notification-service | order-service |

**Ramificação de falha:** `payment.failed` → tópico **`payment.events`** → order-service, notification-service.

```
order.created → payment.processed → stock.reserved → notification.sent
                      ↓
               payment.failed
```

**Regra:** o campo `eventType` no envelope permanece canônico (`order.created`, `payment.processed`, …). O **tópico físico** é resolvido por `resolveKafkaTopic()` em `shared/src/events/resolve-kafka-topic.ts`.

| `eventType` | Tópico físico (override) |
|-------------|-------------------------|
| `order.created` | `order.events` |
| `payment.processed` | `payment.events` |
| `payment.failed` | `payment.events` |
| Demais | igual ao `eventType` (ex.: `stock.reserved`) |

**Legado:** mensagens antigas podem existir no tópico `order.created`. O payment-service **só** consome `order.events`. Após mudança de modelo, crie pedidos novos ou reprocesse manualmente.

---

## 2. Envelope e correlação

| Campo | Regra |
|-------|--------|
| `eventId` | UUID v4; único por mensagem; chave de idempotência |
| `correlationId` | **`orderId`** em fluxos de pedido |
| `causationId` | Opcional; `eventId` do evento causador |
| `timestamp` | ISO-8601 UTC no publish |
| `version` | `1.0` (padrão) |
| `source` | Nome do serviço produtor (`order-service`, …) |
| `payload.orderId` | Obrigatório em eventos do fluxo de pedido |

Factory: `createEventEnvelope()` em `shared/src/events/create-envelope.ts`.

---

## 3. Partições, chave e ordenação

| Regra | Detalhe |
|-------|---------|
| **Partition key** | Sempre `orderId` (`resolvePartitionKey`) |
| **Partições padrão** | **3** em tópicos criados pelo `kafka-init` |
| **Tópicos agregados com alter** | `order.events`, `payment.events` (script garante ≥ 3 partições) |
| **Ordenação** | Eventos do **mesmo pedido** → **mesma partition** → ordem preservada na partition |
| **Throughput horizontal** | Máximo de consumidores úteis no grupo = **número de partitions** do tópico |

**Comandos:**

```bash
npm run kafka:partitions          # alter → 3 partitions
npm run verify:kafka-partitions   # distribuição + key + balanceamento
```

Após alterar partitions: `restart payment-service-1 payment-service-2` para rebalancear.

---

## 4. Consumer groups e escalabilidade horizontal

### 4.1 Group IDs (configuração)

| Serviço | `groupId` configurado (`resolveConsumerGroup`) |
|---------|-----------------------------------------------|
| order-service | `eventflow.order-service` |
| payment-service | `eventflow.payment-service` |
| stock-service | `eventflow.stock-service` |
| notification-service | `eventflow.notification-service` |
| dlq-service | `eventflow.dlq-service` |

Override por serviço: `KAFKA_CONSUMER_GROUP_<SERVICE>` (ex.: `KAFKA_CONSUMER_GROUP_PAYMENT_SERVICE`).

### 4.2 NestJS — sufixo `-server`

O microserviço Kafka do Nest registra o consumer com sufixo **`-server`** no broker.

| Config / log app | Grupo no broker (Kafka UI / CLI) |
|------------------|----------------------------------|
| `eventflow.payment-service` | **`eventflow.payment-service-server`** |

Scripts de verificação de balanceamento usam `eventflow.payment-service-server`.

### 4.3 Múltiplas instâncias do payment-service

| Regra | Valor |
|-------|--------|
| Mesmo `groupId` em todas as réplicas | **Obrigatório** — senão o mesmo evento é processado mais de uma vez |
| `clientId` distinto por instância | `payment-service-consumer-<HOSTNAME>` (automático via `HOSTNAME` do container) |
| **Não** misturar modos Docker | Não subir `payment-service-1/2` **e** `payment-service` com `--scale` ao mesmo tempo |

**Docker (padrão):**

| Container | HTTP host | Consumer |
|-----------|-----------|----------|
| `payment-service-1` | porta **3002** | sim |
| `payment-service-2` | sem porta publicada | sim |
| `payment-service-3` | profile `payment-scale-3` | sim |

```bash
npm run docker:payment-instances
npm run verify:payment-scale
```

**Balanceamento (exemplo com 3 partitions em `order.events`):**

```text
payment-service-1 → order.events(2), order.cancelled(0,1)
payment-service-2 → order.events(0,1), order.cancelled(2)
```

Logs de consumo de pedido: ver **`payment-service-2`** (ou qualquer instância que tenha partition de `order.events` no assignment).

```bash
podman exec eventflow-kafka kafka-consumer-groups \
  --bootstrap-server kafka:29092 \
  --describe --group eventflow.payment-service-server --members --verbose
```

### 4.4 Idempotência entre réplicas (payment)

| Item | Regra |
|------|--------|
| Armazenamento | SQLite em `PAYMENT_DATABASE_PATH` |
| Docker | Volume compartilhado **`payment-idempotency`** → `/data/payments.sqlite` |
| Chave | `processed_events.inbound_event_id` (PK) |
| Mesmo `orderId` | Bloqueia segundo pagamento `approved` |

**Demo:** volume compartilhado é aceitável para aprendizado; em produção use Postgres/Redis para muitas réplicas.

---

## 5. Idempotência (order-service)

| Regra | Implementação |
|-------|----------------|
| Antes de publicar | Persistir `orders.event_id`; se já existir, não republicar |
| Chave de negócio | `orderId` + `eventId` inbound no payment |

---

## 6. Producer (publicação Kafka)

| Regra | Implementação |
|-------|----------------|
| Tópico | `resolveKafkaTopic(eventType)` |
| Key | `resolvePartitionKey(envelope)` → `orderId` |
| Retry app-level | `publishWithProducerRetry()` |
| Retry transporte | `producer.retry` em `getKafkaClientConfig()` |
| Publish confiável | **`emitKafkaEvent()`** — `lastValueFrom(emit().pipe(defaultIfEmpty))`; **não** usar `firstValueFrom(emit())` |
| Client producer | **`producerOnlyMode: true`** — evita consumer extra no `ClientKafka` |
| Falha após retries | order-service marca pedido `failed`; HTTP **500** |

### 6.1 Variáveis de retry — valores vazios

| Regra | Detalhe |
|-------|---------|
| **Proibido** | `KAFKA_RETRY_MAX_ATTEMPTS=` (vazio) no `.env` |
| Efeito de valor inválido | `maxAttempts: NaN` → publish falha com erro `undefined` |
| Correção | Usar números (`3`, `1000`, …) ou **omitir** a variável |

Variáveis: `KAFKA_PRODUCER_RETRY_*` com fallback em `KAFKA_RETRY_*` (ver [RETRY_DLQ.md](./RETRY_DLQ.md)).

---

## 7. Consumer (retry e DLQ)

| Regra | Valor padrão |
|-------|----------------|
| Tentativas máximas | **3** |
| Backoff | exponencial: `1s → 2s → DLQ` |
| Após esgotar | publicar em `{topic}.dlq` |

Env: `KAFKA_CONSUMER_RETRY_*` ou `KAFKA_RETRY_*`.

**Logs:** `[KAFKA CONSUMER RETRY]`, `[KAFKA RETRY]`, `[KAFKA DLQ]`, `[EVENT RECEIVED]`, `[IDEMPOTENCY SKIP]`.

Handlers inválidos (envelope inválido): ack sem retry (evita poison loop).

---

## 8. Payment-service — simulação

| Variável | Padrão | Efeito |
|----------|--------|--------|
| `PAYMENT_FAILURE_RATE` | `0.2` | ~80% `payment.processed`, ~20% `payment.failed` |
| `PAYMENT_FORCE_FAILURE` | — | força falha |
| Regras de negócio | `payment-rules.ts` | validação antes do gateway simulado |

Tipos de falha simulados: `timeout`, `gateway_unavailable`, `card_declined`, `connection_reset`.

Teste determinístico de sucesso: `PAYMENT_FAILURE_RATE=0 npm run verify:payment-flow`.

---

## 9. Bootstrap da aplicação (HTTP + Kafka)

Todo serviço de domínio segue o padrão híbrido em `main.ts`:

1. `NestFactory.create(AppModule)`
2. `app.connectMicroservice({ transport: KAFKA, options: getKafkaConsumerConfig(service) })`
3. `await app.startAllMicroservices()` — **antes** de `app.listen()`
4. Log: `formatKafkaConsumerBootstrap(consumerInfo)`

**Regra Docker:** após alterar código, **rebuild da imagem** é obrigatório. Imagem antiga do payment-service sem `connectMicroservice` só sobe HTTP e **não consome** Kafka.

```bash
npm run docker:rebuild-services
```

---

## 10. Docker e rede

| Contexto | `KAFKA_BOOTSTRAP_SERVERS` |
|----------|---------------------------|
| Apps no host | `localhost:9092` |
| Apps na rede `eventflow-network` | `kafka:29092` |

| Serviço infra | Porta host |
|---------------|------------|
| Kafka | 9092 |
| Kafka UI | 8080 |
| order-service | 3001 |
| payment-service-1 | 3002 |

`kafka-init`: cria tópicos com `--config retention.ms=…` e `--config cleanup.policy=…` (formato separado, não CSV em um único `--config`).

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

| Campo | Regra |
|-------|--------|
| `customerId` | string não vazia |
| `items` | array ≥ 1 item |
| `items[].quantity` | inteiro ≥ 1 |
| `items[].unitPrice` | número positivo, até 2 casas decimais |
| `currency` | opcional: `BRL`, `USD`, `EUR` (padrão `BRL`) |

**Resposta:** `orderId`, `status`, `totalAmount`, `currency`, `eventId`, `correlationId`.

---

## 12. Scripts npm de validação

| Script | Função |
|--------|--------|
| `npm run verify:order-kafka` | POST /orders + mensagem em `order.events` |
| `npm run verify:payment-flow` | Fluxo order → payment → Kafka |
| `npm run verify:payment-scale` | ≥ 2 members no consumer group |
| `npm run verify:kafka-partitions` | Partitions, key, distribuição, balanceamento |
| `npm run kafka:partitions` | Altera `order.events` / `payment.events` para 3 partitions |
| `npm run docker:payment-instances` | Sobe réplicas payment (named ou scale) |
| `npm run docker:rebuild-services` | Rebuild + recreate containers |

---

## 13. Troubleshooting

| Sintoma | Causa provável | Ação |
|---------|----------------|------|
| Payment sem logs de consumo | Imagem Docker antiga (sem Kafka consumer) | `npm run docker:rebuild-services` |
| Payment sem `[EVENT RECEIVED]` | Logs na instância errada ou 1 partition | `kafka-consumer-groups --describe`; `npm run kafka:partitions` + restart |
| `Published` no order mas payment não reage | Mensagens em `order.created` (legado) | Pedidos novos após rebuild; tópico correto é `order.events` |
| `Kafka publish failed … undefined` | `KAFKA_RETRY_*` vazio → NaN | Corrigir `.env` ou omitir variáveis |
| `order.events` com 1 partition | Tópico auto-criado antes do init | `npm run kafka:partitions` |
| Dois consumers duplicam evento | `groupId` diferente por instância | Mesmo `KAFKA_CONSUMER_GROUP_PAYMENT_SERVICE` |

**Startup saudável (payment):**

```text
Kafka consumer: service=payment-service ... groupId=eventflow.payment-service
Kafka consumer microservice started
```

---

## 14. Logs estruturados (grep)

| Serviço | Tags |
|---------|------|
| order | `Order created`, `Publishing order.created`, `Kafka publish succeeded` |
| payment | `[EVENT RECEIVED]`, `[PAYMENT STARTED]`, `[PAYMENT SUCCESS]`, `[PAYMENT FAILED]`, `[IDEMPOTENCY SKIP]` |
| kafka | `[KAFKA RETRY]`, `[KAFKA DLQ]`, `[KAFKA CONSUMER RETRY]` |

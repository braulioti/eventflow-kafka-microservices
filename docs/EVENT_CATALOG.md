# Event Catalog

Canonical reference for EventFlow domain events, Kafka topics, and service ownership.

See [RULES.md](./RULES.md) for all system rules, [EVENT_MODELING.md](./EVENT_MODELING.md) for modeling details.

## Core system events

| Event | Role in flow |
|-------|----------------|
| `order.created` | Starts the order pipeline |
| `payment.processed` | Payment succeeded → triggers stock |
| `payment.failed` | Payment failure branch |
| `stock.reserved` | Inventory reserved → triggers notification |
| `notification.sent` | Flow completed |

Happy path: `order.created → payment.processed → stock.reserved → notification.sent`

## Design principles

| Concern | Approach |
|---------|----------|
| **Consistency** | Shared TypeScript contracts in `@eventflow/shared` |
| **Traceability** | `EventEnvelope` with `eventId`, `correlationId`, `causationId` |
| **Scalability** | Aggregate topics (`order.events`, `payment.events`); 3 partitions; key = `orderId`; ver [RULES.md](./RULES.md) |
| **Retention** | 7 days default (`KAFKA_TOPIC_RETENTION_MS`) |
| **Failure handling** | DLQ topic per event: `{event-type}.dlq` |
| **Evolution** | `version` field on every envelope (current: `1.0`) |

## Events

| Event | Topic | Producer | Consumers |
|-------|-------|----------|-----------|
| `order.created` | `order.events` | order-service | payment-service |
| `order.cancelled` | `order.cancelled` | order-service | payment, stock, notification |
| `payment.requested` | `payment.requested` | payment-service | payment-service |
| `payment.processed` | `payment.events` | payment-service | stock-service |
| `payment.failed` | `payment.events` | payment-service | order, notification |
| `stock.reserved` | `stock.reserved` | stock-service | notification-service |
| `stock.released` | `stock.released` | stock-service | order-service |
| `stock.failed` | `stock.failed` | stock-service | order, notification |
| `notification.send` | `notification.send` | notification-service | notification-service |
| `notification.sent` | `notification.sent` | notification-service | order-service |
| `notification.failed` | `notification.failed` | notification-service | dlq-service |

## Envelope structure

```json
{
  "eventId": "uuid",
  "eventType": "order.created",
  "version": "1.0",
  "timestamp": "2026-05-19T12:00:00.000Z",
  "correlationId": "order-uuid",
  "causationId": "optional-parent-event-uuid",
  "source": "order-service",
  "payload": { }
}
```

## Flow

```
order.created → payment.processed → stock.reserved → notification.sent
```

Failures branch to `*.failed` events and eventually `{topic}.dlq`.

## Code

Definitions live in `shared/src/events/`. Import from `@eventflow/shared` in services.

## Kafka integration

- Transport: `@nestjs/microservices` + `kafkajs`
- Producer: `EventPublisher` + `emitKafkaEvent()` + `publishWithProducerRetry()`
- Consumer: `@EventPattern` (ex.: `order.events`, not legacy topic name alone)
- Config: `getKafkaConsumerConfig(serviceName)` / `getKafkaClientConfig(clientId)`
- Env: `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`)
- Consumer group no broker: `eventflow.<service>-server` (Nest suffix)

## Related

- [RULES.md](./RULES.md) — regras completas

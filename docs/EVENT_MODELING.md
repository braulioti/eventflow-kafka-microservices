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

## Event envelope (standard structure)

Every published message uses `EventEnvelope<T>`:

| Field | Rule |
|-------|------|
| `eventId` | UUID v4; unique per message; idempotency key |
| `eventType` | Canonical event name; equals Kafka topic name |
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

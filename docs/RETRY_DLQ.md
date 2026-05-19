# Retry & DLQ Strategy

## Policy (default)

| Setting | Default | Env variable |
|---------|---------|--------------|
| Max attempts | 3 | `KAFKA_RETRY_MAX_ATTEMPTS` |
| Base delay | 1000 ms | `KAFKA_RETRY_BASE_DELAY_MS` |
| Max delay | 30000 ms | `KAFKA_RETRY_MAX_DELAY_MS` |
| Backoff multiplier | 2 (exponential) | `KAFKA_RETRY_BACKOFF_MULTIPLIER` |

Backoff formula: `min(baseDelay × multiplier^(attempt-1), maxDelay)`

Example delays: **1s → 2s → DLQ**

## Flow

```
Consume event
    │
    ▼
 Handler fails?
    │ no ──► ack (success)
    │
   yes
    │
    ▼
 attempt + 1 < maxAttempts?
    │ yes ──► sleep(backoff) ──► republish to same topic with retry headers
    │
   no
    │
    ▼
 Publish EventFailurePayload to {topic}.dlq
```

## When to send to DLQ

A message goes to DLQ when:

1. The handler throws an error, **and**
2. `retryCount + 1 >= maxAttempts` (default: after the 3rd failed attempt)

Non-retryable errors should be caught in the handler and handled explicitly before rethrowing.

## Retry headers

| Header | Description |
|--------|-------------|
| `x-retry-count` | Current retry attempt |
| `x-retry-max` | Configured max attempts |
| `x-retry-at` | Unix timestamp (ms) when retry was scheduled |
| `x-original-topic` | Source topic |

## Error event structure (`EventFailurePayload`)

Published to `{originalTopic}.dlq`:

```json
{
  "eventId": "uuid",
  "eventType": "order.created",
  "source": "payment-service",
  "correlationId": "order-uuid",
  "payload": {
    "kind": "event.failure",
    "originalTopic": "order.created",
    "originalEventId": "uuid",
    "originalEventType": "order.created",
    "originalSource": "order-service",
    "correlationId": "order-uuid",
    "originalPayload": { },
    "originalEnvelope": { },
    "error": {
      "message": "…",
      "name": "Error",
      "stack": "…",
      "occurredAt": "ISO-8601"
    },
    "retry": {
      "attempt": 3,
      "maxAttempts": 3,
      "exhausted": true
    },
    "failedBy": "payment-service",
    "failedAt": "ISO-8601"
  }
}
```

## Code

- Policy: `shared/src/kafka/retry/retry-policy.ts`
- Executor: `shared/src/kafka/retry/kafka-retry.executor.ts`
- Failure payload: `shared/src/events/event-failure.ts`
- Per service: `KafkaRetryRunner` + `@Ctx() KafkaContext` in consumers

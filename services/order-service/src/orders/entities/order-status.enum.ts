/**
 * @file order-status.enum.ts
 * @module order-service — order lifecycle states
 *
 * String enum persisted on `OrderEntity.status` (SQLite text column).
 * Transitions are driven by:
 *
 * - **HTTP create** → `pending`
 * - **Publish failure** → `failed`
 * - **Kafka consumers** → `failed` | `cancelled` | `completed`
 *
 * There is no `processing` intermediate state in the demo — downstream progress
 * is inferred from Kafka logs and other services.
 */
/** Persisted order states for the EventFlow purchase saga. */
export enum OrderStatus {
  /** Order saved; `order.created` may be in flight or awaiting downstream steps. */
  PENDING = 'pending',
  /** Terminal success — notification.sent processed. */
  COMPLETED = 'completed',
  /** Terminal failure — payment.failed or stock.failed. */
  FAILED = 'failed',
  /** Compensation — stock.released after rollback. */
  CANCELLED = 'cancelled',
}

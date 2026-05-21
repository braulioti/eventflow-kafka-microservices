/**
 * Notification bounded context event payloads.
 *
 * Models dispatch requests, delivery confirmations, and terminal failures. `notification.sent`
 * is consumed by order-service as the core-flow completion signal.
 */

/** Supported outbound notification channels in the reference implementation. */
export type NotificationChannel = 'email' | 'sms' | 'push';

/** Request to dispatch a template to a recipient. */
export interface NotificationSendPayload {
  notificationId: string;
  orderId: string;
  channel: NotificationChannel;
  recipient: string;
  template: string;
}

/** Confirms delivery; order-service treats this as saga completion. */
export interface NotificationSentPayload {
  notificationId: string;
  orderId: string;
  channel: NotificationChannel;
  sentAt: string;
}

/** Terminal failure; consumed by dlq-service for observability. */
export interface NotificationFailedPayload {
  notificationId: string;
  orderId: string;
  channel: NotificationChannel;
  reason: string;
  failedAt: string;
}

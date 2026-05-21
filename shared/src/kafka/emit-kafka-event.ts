/**
 * Reliable fire-and-forget publish via Nest `ClientKafka.emit`.
 *
 * `firstValueFrom(emit())` fails with an empty error when the observable completes
 * without emitting (common with Nest Kafka event emit). `lastValueFrom` + `defaultIfEmpty`
 * waits for `dispatchEvent` / `producer.send` to finish.
 */
import { lastValueFrom, type Observable } from 'rxjs';
import { defaultIfEmpty } from 'rxjs/operators';

export interface KafkaEmitMessage {
  key?: string;
  value: unknown;
  headers?: Record<string, string>;
}

/** Minimal Nest `ClientKafka` surface used for event emit. */
export interface KafkaEmitClient {
  emit(pattern: string, data: KafkaEmitMessage): Observable<unknown>;
}

export async function emitKafkaEvent(
  client: KafkaEmitClient,
  topic: string,
  message: KafkaEmitMessage,
): Promise<void> {
  await lastValueFrom(
    client.emit(topic, message).pipe(defaultIfEmpty(undefined)),
  );
}

/**
 * DLQ observability service: logs poison messages and structured event.failure payloads.
 * Port: PORT or DLQ_SERVICE_PORT (default 3005).
 */
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { getKafkaConsumerConfig } from '@eventflow/shared';
import { AppModule } from './app.module';

async function bootstrap() {
  const port = Number(process.env.PORT ?? process.env.DLQ_SERVICE_PORT ?? 3005);
  const app = await NestFactory.create(AppModule);

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: getKafkaConsumerConfig('dlq-service'),
  });

  await app.startAllMicroservices();
  await app.listen(port);
}
bootstrap();

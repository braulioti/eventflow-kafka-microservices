/**
 * Notification service: sends customer comms after stock.reserved; handles failures.
 * Port: PORT or NOTIFICATION_SERVICE_PORT (default 3004).
 */
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { getKafkaConsumerConfig } from '@eventflow/shared';
import { AppModule } from './app.module';

async function bootstrap() {
  const port = Number(
    process.env.PORT ?? process.env.NOTIFICATION_SERVICE_PORT ?? 3004,
  );
  const app = await NestFactory.create(AppModule);

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: getKafkaConsumerConfig('notification-service'),
  });

  await app.startAllMicroservices();
  await app.listen(port);
}
bootstrap();

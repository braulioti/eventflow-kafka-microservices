/**
 * Order service entrypoint: HTTP API (create order) + Kafka consumer for downstream events.
 * Port: PORT or ORDER_SERVICE_PORT (default 3001).
 */
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { getKafkaConsumerConfig } from '@eventflow/shared';
import { AppModule } from './app.module';

async function bootstrap() {
  const port = Number(process.env.PORT ?? process.env.ORDER_SERVICE_PORT ?? 3001);
  const app = await NestFactory.create(AppModule);

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: getKafkaConsumerConfig('order-service'),
  });

  await app.startAllMicroservices();
  await app.listen(port);
}
bootstrap();

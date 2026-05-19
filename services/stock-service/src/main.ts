/**
 * Stock service: reserves inventory after payment, handles cancellations.
 * Port: PORT or STOCK_SERVICE_PORT (default 3003).
 */
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { getKafkaConsumerConfig } from '@eventflow/shared';
import { AppModule } from './app.module';

async function bootstrap() {
  const port = Number(process.env.PORT ?? process.env.STOCK_SERVICE_PORT ?? 3003);
  const app = await NestFactory.create(AppModule);

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: getKafkaConsumerConfig('stock-service'),
  });

  await app.startAllMicroservices();
  await app.listen(port);
}
bootstrap();

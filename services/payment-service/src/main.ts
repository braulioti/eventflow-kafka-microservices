/**
 * Payment service: consumes order events, runs payment saga steps, publishes payment.*.
 * Port: PORT or PAYMENT_SERVICE_PORT (default 3002).
 */
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { getKafkaConsumerConfig } from '@eventflow/shared';
import { AppModule } from './app.module';

async function bootstrap() {
  const port = Number(
    process.env.PORT ?? process.env.PAYMENT_SERVICE_PORT ?? 3002,
  );
  const app = await NestFactory.create(AppModule);

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: getKafkaConsumerConfig('payment-service'),
  });

  await app.startAllMicroservices();
  await app.listen(port);
}
bootstrap();

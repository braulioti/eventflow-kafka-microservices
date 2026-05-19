import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const port = Number(
    process.env.PORT ?? process.env.PAYMENT_SERVICE_PORT ?? 3002,
  );
  const app = await NestFactory.create(AppModule);
  await app.listen(port);
}
bootstrap();

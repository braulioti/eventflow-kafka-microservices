/**
 * Full flow: POST /orders → message on order.events.
 *
 * Requires Kafka + order-service stack:
 *   KAFKA_INTEGRATION_TEST=true npm run test:integration -w order-service
 *
 * Manual check: open Kafka UI → topic order.events → verify key = orderId and envelope payload.
 */
import { INestApplication, ValidationPipe } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { Kafka, logLevel } from 'kafkajs';
import request from 'supertest';
import { App } from 'supertest/types';
import {
  EventType,
  OrderKafkaTopic,
  getKafkaConsumerConfig,
  type EventEnvelope,
  type OrderCreatedPayload,
} from '@eventflow/shared';
import { AppModule } from '../src/app.module';

const runIntegration = process.env.KAFKA_INTEGRATION_TEST === 'true';
const describeIntegration = runIntegration ? describe : describe.skip;

describeIntegration('POST /orders → order.events (integration)', () => {
  let app: INestApplication<App>;
  let kafka: Kafka;

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleFixture.createNestApplication();
    app.useGlobalPipes(
      new ValidationPipe({
        whitelist: true,
        forbidNonWhitelisted: true,
        transform: true,
      }),
    );

    app.connectMicroservice<MicroserviceOptions>({
      transport: Transport.KAFKA,
      options: getKafkaConsumerConfig('order-service'),
    });

    await app.startAllMicroservices();
    await app.listen(0);

    kafka = new Kafka({
      clientId: 'order-service-integration-test',
      brokers: (process.env.KAFKA_BOOTSTRAP_SERVERS ?? 'localhost:9092').split(','),
      logLevel: logLevel.ERROR,
    });
  }, 60000);

  afterAll(async () => {
    await app?.close();
  });

  it('publishes order.created with orderId key and valid envelope', async () => {
    const consumer = kafka.consumer({
      groupId: `integration-test-${Date.now()}`,
    });

    await consumer.connect();
    await consumer.subscribe({
      topic: OrderKafkaTopic.ORDER_EVENTS,
      fromBeginning: false,
    });

    let expectedOrderId: string | undefined;

    const messagePromise = new Promise<string>((resolve, reject) => {
      const timeout = setTimeout(
        () => reject(new Error('Timed out waiting for Kafka message on order.events')),
        45000,
      );

      void consumer
        .run({
          eachMessage: async ({ message }) => {
            if (!message.value || !expectedOrderId) {
              return;
            }

            const envelope = JSON.parse(
              message.value.toString(),
            ) as EventEnvelope<OrderCreatedPayload>;

            if (
              envelope.eventType !== EventType.ORDER_CREATED ||
              envelope.payload.orderId !== expectedOrderId
            ) {
              return;
            }

            const key = message.key?.toString();
            expect(key).toBe(expectedOrderId);
            expect(envelope.eventId).toMatch(
              /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i,
            );
            expect(envelope.correlationId).toBe(expectedOrderId);
            expect(envelope.source).toBe('order-service');
            expect(envelope.timestamp).toBeDefined();
            expect(envelope.payload.customerId).toBe('customer-integration');
            expect(envelope.payload.totalAmount).toBe(99.8);

            clearTimeout(timeout);
            resolve(expectedOrderId);
          },
        })
        .catch(reject);
    });

    const response = await request(app.getHttpServer())
      .post('/orders')
      .send({
        customerId: 'customer-integration',
        currency: 'BRL',
        items: [{ productId: 'sku-test', quantity: 2, unitPrice: 49.9 }],
      })
      .expect(201);

    expectedOrderId = response.body.orderId;
    expect(response.body.eventId).toBeDefined();

    const orderIdFromKafka = await messagePromise;
    await consumer.disconnect();

    expect(orderIdFromKafka).toBe(expectedOrderId);
  }, 60000);
});

import { Test } from '@nestjs/testing';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ProcessedEventEntity } from './entities/processed-event.entity';
import { ProcessedEventsService } from './processed-events.service';

describe('ProcessedEventsService', () => {
  let service: ProcessedEventsService;

  beforeEach(async () => {
    const module = await Test.createTestingModule({
      imports: [
        TypeOrmModule.forRoot({
          type: 'better-sqlite3',
          database: ':memory:',
          entities: [ProcessedEventEntity],
          synchronize: true,
        }),
        TypeOrmModule.forFeature([ProcessedEventEntity]),
      ],
      providers: [ProcessedEventsService],
    }).compile();

    service = module.get(ProcessedEventsService);
  });

  it('detects duplicate inbound eventId', async () => {
    await service.recordProcessed({
      inboundEventId: 'evt-1',
      eventType: 'order.created',
      orderId: 'order-1',
      paymentId: 'pay-1',
      outcome: 'approved',
    });

    const check = await service.checkOrderCreated('evt-1', 'order-1');
    expect(check.status).toBe('duplicate_event');
  });

  it('prevents duplicate payment for same orderId', async () => {
    await service.recordProcessed({
      inboundEventId: 'evt-1',
      eventType: 'order.created',
      orderId: 'order-1',
      paymentId: 'pay-1',
      outcome: 'approved',
    });

    const check = await service.checkOrderCreated('evt-2', 'order-1');
    expect(check.status).toBe('duplicate_order');
  });
});

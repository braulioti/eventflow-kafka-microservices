# EventFlow — Kafka Microservices

EventFlow is a learning and reference project that demonstrates an **event-driven microservices architecture** using **Apache Kafka**. It simulates an order processing flow where each step is handled by an independent service that communicates asynchronously through domain events.

The goal is to show how to design decoupled services, handle failures with a Dead Letter Queue (DLQ), and run the full stack locally with Docker/Podman.

---

## Project objectives

- Model a realistic order flow split into bounded contexts (orders, payments, inventory, notifications).
- Practice **event-driven architecture** with Kafka as the message broker.
- Build **independent, deployable services** with clear responsibilities.
- Provide a **reproducible local environment** (Kafka, UI, and all microservices in containers).
- Prepare the codebase for patterns such as idempotent consumers, retries, and DLQ handling.

---

## Proposed architecture

The flow starts when a client creates an order. Each service consumes events from Kafka, executes its logic, and publishes new events for downstream services.

```mermaid
flowchart LR
  Client([Client / API])

  subgraph services [Microservices]
    OS[order-service]
    PS[payment-service]
    SS[stock-service]
    NS[notification-service]
    DLQ[dlq-service]
  end

  subgraph kafka [Apache Kafka]
    T1[order.events]
    T2[payment.events]
    T3[stock.reserved]
    T4[notification.sent]
    TDLQ[*.dlq]
  end

  Client --> OS
  OS --> T1
  T1 --> PS
  PS --> T2
  T2 --> SS
  SS --> T3
  T3 --> NS
  NS --> T4

  PS -.->|failure| TDLQ
  SS -.->|failure| TDLQ
  NS -.->|failure| TDLQ
  TDLQ --> DLQ
```

### Service responsibilities

| Service | Role |
|---------|------|
| **order-service** | `POST /orders` — validates input, persists orders in **SQLite**, publishes `order.created`. |
| **payment-service** | Processes payments and publishes `payment.processed` or failure events. |
| **stock-service** | Reserves inventory and publishes `stock.reserved` events. |
| **notification-service** | Sends notifications when the flow completes (`order.completed`). |
| **dlq-service** | Consumes messages from Dead Letter Topics for inspection and reprocessing. |

> **Note:** Kafka producers/consumers are part of the proposed design. Services currently expose HTTP health endpoints and are ready for `@nestjs/microservices` integration.

---

## Tech stack

| Layer | Technology |
|-------|------------|
| Language | TypeScript |
| Framework | [NestJS](https://nestjs.com/) 11 |
| Runtime | Node.js 22 |
| Message broker | Apache Kafka 7.5 (Confluent Platform images) |
| Coordination | Zookeeper |
| Observability (local) | [Kafka UI](https://github.com/provectus/kafka-ui) |
| Containers | Docker / Podman |
| Monorepo | npm workspaces |
| Persistence (orders) | SQLite via TypeORM (`better-sqlite3`) |
| Validation | `class-validator` + `class-transformer` |

---

## Folder structure

```
eventflow-kafka-microservices/
├── docker/
│   ├── docker-compose.yml              # Kafka, Zookeeper, Kafka UI, kafka-init
│   ├── kafka-init/create-topics.sh       # Topic bootstrap (runs on compose up)
│   └── services/
│       ├── docker-compose.yml            # All NestJS microservices
│       ├── order-service/Dockerfile
│       ├── payment-service/Dockerfile
│       ├── stock-service/Dockerfile
│       ├── notification-service/Dockerfile
│       └── dlq-service/Dockerfile
├── services/                           # NestJS applications (source code)
│   ├── order-service/
│   ├── payment-service/
│   ├── stock-service/
│   ├── notification-service/
│   └── dlq-service/
├── shared/                             # @eventflow/shared — events, topics, envelopes
├── docs/
│   ├── PROJECT_DETAILS.md              # Full project guide (business + technical)
│   ├── EVENT_MODELING.md               # Core events, topics, envelope, POST /orders
│   ├── EVENT_CATALOG.md                # Full event catalog reference
│   └── RETRY_DLQ.md                    # Retry and DLQ strategy
├── scripts/                            # Automation scripts (planned)
├── .env                                # Local environment variables
├── .dockerignore
├── package.json                        # npm workspaces root
└── README.md
```

---

## Prerequisites

- **Node.js** 22+ and **npm** 11+
- **Docker** or **Podman** + **podman-compose** (Fedora)
- Ports available: `2181`, `9092`, `8080`, `3001`–`3005`

---

## Running the environment

### 1. Clone and configure

```bash
git clone <repository-url>
cd eventflow-kafka-microservices
cp .env.example .env   # adjust variables if needed
```

### 2. Start infrastructure (Kafka)

Creates the shared Docker network `eventflow-network` and **all Kafka topics** (including DLQ) via the `kafka-init` service.

**Docker:**

```bash
docker compose -f docker/docker-compose.yml up -d
```

**Podman (Fedora):**

```bash
podman-compose -f docker/docker-compose.yml up -d
```

| Component | URL / Address |
|-----------|----------------|
| Kafka (host) | `localhost:9092` |
| Kafka (Docker network) | `kafka:29092` |
| Kafka UI | http://localhost:8080 |
| Zookeeper | `localhost:2181` |

> Images use the `docker.io/` prefix so Podman can pull them without interactive short-name prompts.
>
> `kafka-init` runs once after Kafka is ready. `kafka-ui` starts only after topics are created.

### 3. Start all microservices (Docker)

Requires infrastructure from step 2.

```bash
podman-compose -f docker/services/docker-compose.yml up -d --build
```

This starts **two** `payment-service` instances (`payment-service-1`, `payment-service-2`) in the same Kafka consumer group `eventflow.payment-service`. Kafka assigns `order.events` partitions across them. Instance 1 exposes HTTP on port **3002**; instance 2 is consumer-only (no host port).

Optional third instance:

```bash
podman-compose -f docker/services/docker-compose.yml --profile payment-scale-3 up -d --build payment-service-3
```

Or use the helper script (rebuilds payment + other services):

```bash
npm run docker:payment-instances
PAYMENT_SERVICE_REPLICAS=3 npm run docker:payment-instances
```

**Compose scale mode** (single service name, N containers):

```bash
PAYMENT_DOCKER_MODE=scale PAYMENT_SERVICE_REPLICAS=3 npm run docker:payment-instances
```

Do not run named instances (`payment-service-1/2`) and compose-scale `payment-service` at the same time — you would duplicate consumers.

Idempotency uses a shared Docker volume `payment-idempotency` (`/data/payments.sqlite`). Fine for demos; prefer Postgres/Redis when running many replicas in production.

### Kafka partitions (distribution, ordering, balance)

```bash
# Increase order.events / payment.events to 3 partitions (uses podman exec if kafka-topics is not on PATH)
npm run kafka:partitions

podman-compose -f docker/services/docker-compose.yml restart payment-service-1 payment-service-2

# Validates: same key → same partition, spread across partitions, POST /orders, consumer group balance
npm run verify:kafka-partitions
```

Stop microservices:

```bash
podman-compose -f docker/services/docker-compose.yml down
```

Stop infrastructure:

```bash
podman-compose -f docker/docker-compose.yml down
```

### 4. Local development (without Docker for apps)

Install dependencies and run services individually:

```bash
npm install
npm run build
```

| Service | Port | Command |
|---------|------|---------|
| order-service | 3001 | `npm run start:order` |
| payment-service | 3002 | `npm run start:payment` |
| stock-service | 3003 | `npm run start:stock` |
| notification-service | 3004 | `npm run start:notification` |
| dlq-service | 3005 | `npm run start:dlq` |

Use `KAFKA_BOOTSTRAP_SERVERS=localhost:9092` from `.env` when running on the host.

### Health checks

Each service exposes:

- `GET /` — service info
- `GET /health` — health check
- `GET /events/catalog` — events produced/consumed by this service

### Event catalog & Kafka

Domain events, Kafka topics, payloads, and envelopes are defined in `@eventflow/shared`.

Each service runs as a **hybrid app** (HTTP + Kafka consumer) via `@nestjs/microservices`.

- [docs/RULES.md](docs/RULES.md) — **complete system rules** (topics, partitions, scale, producer, troubleshooting)
- [docs/PROJECT_DETAILS.md](docs/PROJECT_DETAILS.md) — step-by-step guide (architecture, runbook)
- [docs/EVENT_MODELING.md](docs/EVENT_MODELING.md) — core events, partitions, envelope
- [docs/EVENT_CATALOG.md](docs/EVENT_CATALOG.md) — event catalog
- [docs/RETRY_DLQ.md](docs/RETRY_DLQ.md) — retry and DLQ

Core flow: `order.created → payment.processed → stock.reserved → notification.sent`

```bash
curl http://localhost:3001/events/catalog   # order-service
curl http://localhost:3005/events/catalog   # dlq-service (full system catalog)
```

#### End-to-end flow (Kafka)

1. Start infrastructure and microservices (see [Running the environment](#running-the-environment)).
2. Trigger the happy path:

```bash
curl -X POST http://localhost:3001/orders \
  -H 'Content-Type: application/json' \
  -d '{
    "customerId": "customer-1",
    "items": [{ "productId": "sku-1", "quantity": 2, "unitPrice": 49.9 }]
  }'
```

Event chain:

```
order.created → payment.processed → stock.reserved → notification.sent
```

3. Inspect messages in Kafka UI: http://localhost:8080

Re-run topic creation only (if needed):

```bash
./scripts/create-kafka-topics.sh
```

Example:

```bash
curl http://localhost:3001/health
# {"service":"order-service","status":"ok"}
```

### Build a single Docker image

From the repository root:

```bash
podman build -f docker/services/order-service/Dockerfile -t eventflow-order-service .
```

---

## Environment variables

See also [docs/RULES.md](docs/RULES.md) and `.env.example`.

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker (host); Docker uses `kafka:29092` |
| `KAFKA_TOPIC_PARTITIONS` | `3` | Partitions in `kafka-init` / `kafka:partitions` |
| `KAFKA_RETRY_*` | see `.env.example` | Retry producer/consumer — **do not leave empty** |
| `KAFKA_CONSUMER_GROUP_PAYMENT_SERVICE` | `eventflow.payment-service` | Same value on all payment replicas |
| `ORDER_SERVICE_PORT` | `3001` | order-service HTTP |
| `ORDER_DATABASE_PATH` | `./services/order-service/data/orders.sqlite` | SQLite orders |
| `PAYMENT_SERVICE_PORT` | `3002` | payment-service-1 HTTP (Docker) |
| `PAYMENT_DATABASE_PATH` | see `.env.example` | Idempotency; Docker: `/data/payments.sqlite` |
| `PAYMENT_FAILURE_RATE` | `0.2` | Simulation ~80% approval |
| `PAYMENT_SERVICE_REPLICAS` | `2` | `docker:payment-instances` |
| `PAYMENT_DOCKER_MODE` | `named` | `named` or `scale` |
| `STOCK_SERVICE_PORT` | `3003` | stock-service |
| `NOTIFICATION_SERVICE_PORT` | `3004` | notification-service |
| `DLQ_SERVICE_PORT` | `3005` | dlq-service |

## npm scripts (verification)

| Script | Description |
|--------|-------------|
| `npm run verify:order-kafka` | POST /orders + message on `order.events` |
| `npm run verify:payment-flow` | order → payment flow |
| `npm run verify:payment-scale` | Consumer group with 2+ members |
| `npm run verify:kafka-partitions` | Partitions, key, distribution, balancing |
| `npm run kafka:partitions` | Sets `order.events` / `payment.events` to 3 partitions |
| `npm run docker:payment-instances` | Starts payment replicas |
| `npm run docker:rebuild-services` | Rebuild images after code changes |

Inside Docker, microservices use `KAFKA_BOOTSTRAP_SERVERS=kafka:29092` via `docker/services/docker-compose.yml`.

---

## License

UNLICENSED — private learning project.

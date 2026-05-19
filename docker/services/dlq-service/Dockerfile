FROM docker.io/node:22-alpine AS builder

WORKDIR /app

COPY package.json package-lock.json ./
COPY shared/package.json ./shared/
COPY services/dlq-service/package.json ./services/dlq-service/

RUN npm ci --workspace=dlq-service

COPY shared ./shared
COPY services/dlq-service ./services/dlq-service

RUN npm run build -w @eventflow/shared
RUN npm run build -w dlq-service

FROM docker.io/node:22-alpine AS production

WORKDIR /app

ENV NODE_ENV=production
ENV PORT=3005

COPY package.json package-lock.json ./
COPY shared/package.json ./shared/
COPY services/dlq-service/package.json ./services/dlq-service/

RUN npm ci --workspace=dlq-service --omit=dev

COPY --from=builder /app/shared/dist ./shared/dist
COPY --from=builder /app/services/dlq-service/dist ./services/dlq-service/dist

WORKDIR /app/services/dlq-service

EXPOSE 3005

USER node

CMD ["node", "dist/main.js"]

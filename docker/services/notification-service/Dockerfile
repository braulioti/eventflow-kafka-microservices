FROM docker.io/node:22-alpine AS builder

WORKDIR /app

COPY package.json package-lock.json ./
COPY shared/package.json ./shared/
COPY services/notification-service/package.json ./services/notification-service/

RUN npm ci --workspace=notification-service

COPY shared ./shared
COPY services/notification-service ./services/notification-service

RUN npm run build -w @eventflow/shared
RUN npm run build -w notification-service

FROM docker.io/node:22-alpine AS production

WORKDIR /app

ENV NODE_ENV=production
ENV PORT=3004

COPY package.json package-lock.json ./
COPY shared/package.json ./shared/
COPY services/notification-service/package.json ./services/notification-service/

RUN npm ci --workspace=notification-service --omit=dev

COPY --from=builder /app/shared/dist ./shared/dist
COPY --from=builder /app/services/notification-service/dist ./services/notification-service/dist

WORKDIR /app/services/notification-service

EXPOSE 3004

USER node

CMD ["node", "dist/main.js"]

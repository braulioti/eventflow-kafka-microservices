FROM docker.io/node:22-alpine AS builder

WORKDIR /app

COPY package.json package-lock.json ./
COPY services/order-service/package.json ./services/order-service/

RUN npm ci --workspace=order-service

COPY services/order-service ./services/order-service

RUN npm run build -w order-service

FROM docker.io/node:22-alpine AS production

WORKDIR /app

ENV NODE_ENV=production
ENV PORT=3001

COPY package.json package-lock.json ./
COPY services/order-service/package.json ./services/order-service/

RUN npm ci --workspace=order-service --omit=dev

COPY --from=builder /app/services/order-service/dist ./services/order-service/dist

WORKDIR /app/services/order-service

EXPOSE 3001

USER node

CMD ["node", "dist/main.js"]

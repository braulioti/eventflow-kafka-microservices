FROM docker.io/node:22-alpine AS builder

WORKDIR /app

COPY package.json package-lock.json ./
COPY services/payment-service/package.json ./services/payment-service/

RUN npm ci --workspace=payment-service

COPY services/payment-service ./services/payment-service

RUN npm run build -w payment-service

FROM docker.io/node:22-alpine AS production

WORKDIR /app

ENV NODE_ENV=production
ENV PORT=3002

COPY package.json package-lock.json ./
COPY services/payment-service/package.json ./services/payment-service/

RUN npm ci --workspace=payment-service --omit=dev

COPY --from=builder /app/services/payment-service/dist ./services/payment-service/dist

WORKDIR /app/services/payment-service

EXPOSE 3002

USER node

CMD ["node", "dist/main.js"]

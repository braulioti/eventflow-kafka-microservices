FROM docker.io/node:22-alpine AS builder

WORKDIR /app

COPY package.json package-lock.json ./
COPY shared/package.json ./shared/
COPY services/stock-service/package.json ./services/stock-service/

RUN npm ci --workspace=stock-service

COPY shared ./shared
COPY services/stock-service ./services/stock-service

RUN npm run build -w @eventflow/shared
RUN npm run build -w stock-service

FROM docker.io/node:22-alpine AS production

WORKDIR /app

ENV NODE_ENV=production
ENV PORT=3003

COPY package.json package-lock.json ./
COPY shared/package.json ./shared/
COPY services/stock-service/package.json ./services/stock-service/

RUN npm ci --workspace=stock-service --omit=dev

COPY --from=builder /app/shared/dist ./shared/dist
COPY --from=builder /app/services/stock-service/dist ./services/stock-service/dist

WORKDIR /app/services/stock-service

EXPOSE 3003

USER node

CMD ["node", "dist/main.js"]

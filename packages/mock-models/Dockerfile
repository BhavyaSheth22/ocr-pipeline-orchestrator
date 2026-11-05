# Build context is the repo root: docker build -f packages/mock-models/Dockerfile .
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json tsconfig.base.json ./
COPY packages/mock-models/package.json packages/mock-models/
RUN npm ci --workspace @ocr/mock-models --include-workspace-root
COPY packages/mock-models packages/mock-models
RUN npm run build -w @ocr/mock-models

FROM node:22-alpine
ENV NODE_ENV=production
WORKDIR /app
COPY package.json package-lock.json ./
COPY packages/mock-models/package.json packages/mock-models/
RUN npm ci --omit=dev --workspace @ocr/mock-models --include-workspace-root
COPY --from=build /app/packages/mock-models/dist packages/mock-models/dist
USER node
WORKDIR /app/packages/mock-models
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=2s --start-period=3s --retries=5 \
  CMD wget -qO- http://127.0.0.1:8080/healthz || exit 1
CMD ["node", "dist/server.js"]

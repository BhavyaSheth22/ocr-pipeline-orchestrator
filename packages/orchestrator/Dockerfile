# Build context is the repo root: docker build -f packages/orchestrator/Dockerfile .
# Stage 1: compile TypeScript (src + bench).
# Debian (glibc), not Alpine: better-sqlite3 on musl measured ~2.5x slower per insert (1.17 vs 0.46 ms to
# create a 20-page job), and job creation runs on the event loop during an upload burst (TTFP).
FROM node:22-bookworm-slim AS build
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends python3 make g++ && rm -rf /var/lib/apt/lists/*
COPY package.json package-lock.json tsconfig.base.json ./
COPY packages/orchestrator/package.json packages/orchestrator/
RUN npm ci --workspace @ocr/orchestrator --include-workspace-root
COPY packages/orchestrator packages/orchestrator
RUN npm run build -w @ocr/orchestrator

# Stage 2: production node_modules. better-sqlite3 is native; build tools here never reach the runtime image.
FROM node:22-bookworm-slim AS deps
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends python3 make g++ && rm -rf /var/lib/apt/lists/*
COPY package.json package-lock.json ./
COPY packages/orchestrator/package.json packages/orchestrator/
RUN npm ci --omit=dev --workspace @ocr/orchestrator --include-workspace-root

# Runtime: Node + qpdf (the page splitter) + wget (healthcheck) only.
FROM node:22-bookworm-slim
ENV NODE_ENV=production DATA_DIR=/data PORT=3000
RUN apt-get update && apt-get install -y --no-install-recommends qpdf wget && rm -rf /var/lib/apt/lists/* \
  && mkdir -p /data && chown node:node /data
WORKDIR /app
COPY --from=deps /app/node_modules node_modules
COPY --from=deps /app/package.json package.json
COPY packages/orchestrator/package.json packages/orchestrator/
COPY --from=build /app/packages/orchestrator/dist packages/orchestrator/dist
USER node
WORKDIR /app/packages/orchestrator
VOLUME /data
EXPOSE 3000
HEALTHCHECK --interval=5s --timeout=2s --start-period=5s --retries=5 \
  CMD wget -qO- http://127.0.0.1:3000/healthz || exit 1
CMD ["node", "--max-old-space-size=256", "dist/src/server.js"]

# OCR Pipeline Orchestrator

Monorepo for the Sarvam Vision takeaway: a high-throughput OCR pipeline orchestrator
(`packages/orchestrator`) in front of a mock layout/VLM model server (`packages/mock-models`).
Design, trade-offs and benchmark numbers: [ARCHITECTURE.md](ARCHITECTURE.md).

## Quick start (Docker)
```bash
docker compose up --build                                  # mock-models :8080, orchestrator :3000
curl -s -X POST localhost:3000/jobs -H 'content-type: application/pdf' --data-binary @doc.pdf
#   → 202 {"job_id":"…","events_url":"/jobs/…/events","total_pages":12,"backlog_pages":12,"eta_s":2}
curl -N localhost:3000/jobs/<job_id>/events                # SSE: job, page (layout|final)…, job.completed
curl -s localhost:3000/jobs/<job_id>/result                # assembled document tree (409 while running)
curl -s localhost:3000/metrics                             # Prometheus
docker compose --profile bench run --rm bench              # 50 jobs / 1000 pages; add --noise-rps 8 for 429 spikes
```

## Orchestrator API
| Method | Path | Purpose |
|---|---|---|
| POST | `/jobs` | Raw `application/pdf` body (≤ 100 MB, ≤ 100 pages), optional `Idempotency-Key` → 202. 413 too large, 422 not a PDF / too many pages, 503 + `Retry-After` when the backlog is full |
| GET | `/jobs/:id/events` | SSE stream, resumable with `Last-Event-ID` |
| GET | `/jobs/:id/result` | Document tree `{type:"document", children:[{type:"page", page, status, source, low_confidence, children}]}` |
| POST | `/evaluate` | `{prediction, ground_truth}` (same tree shape) → `{cer, wer, iou, ted, timing_ms}` |
| GET | `/metrics`, `/healthz` | Prometheus text; liveness |

Orchestrator settings (env): `PORT` (3000), `DATA_DIR` (./data), `MODEL_URL`, `QPDF_BIN` (qpdf),
`LAYOUT_RPS`/`VLM_RPS` (100/10, upper bounds for the adaptive rate), `MAX_QUEUED_PAGES` (6000),
`RESULT_TTL_MS` (1 h), `TED_MAX_STEPS` (5e6). Timing knobs used by tests: `LAYOUT_TIMEOUT_MS`,
`VLM_TIMEOUT_MS`, `BREAKER_OPEN_MS`, `BACKOFF_BASE_MS`, `DEGRADED_AFTER_MS`, `MAX_UPLOAD_BYTES`.

## Setup (all tooling is project-local)
```bash
nvm install && nvm use      # reads .nvmrc -> Node 22
npm ci                      # installs into ./node_modules only
```
No global installs: TypeScript, tsx, @types/express and autocannon are devDependencies, run through npm scripts or `npx`.
The orchestrator needs **qpdf** on the machine (`sudo apt install qpdf`; the Docker image has it).

```bash
npm test                                         # mock + orchestrator tests (17 qpdf tests skip without qpdf)
npm run dev -w @ocr/mock-models                  # mock on :8080
MODEL_URL=http://127.0.0.1:8080 npm run dev -w @ocr/orchestrator     # orchestrator on :3000
npm run bench:load -w @ocr/orchestrator -- --orch http://127.0.0.1:3000 --mock http://127.0.0.1:8080
npm run bench:ted -w @ocr/orchestrator           # TED p50/p99 on 1,000 random 50-node pairs
```

No qpdf locally? Run the full orchestrator suite (all 60 tests, crash test included) in a throwaway
container. It reuses this checkout's `node_modules`, so it needs a Linux x64 host (WSL works):
```bash
docker run --rm -v "$PWD":/app:ro -w /app/packages/orchestrator -e QPDF_BIN=/usr/bin/qpdf node:22-bookworm-slim \
  sh -c 'apt-get update -qq && apt-get install -y -qq qpdf >/dev/null && node --import tsx --test test/*.test.ts'
```
The orchestrator's Docker build image can't run these tests: it doesn't contain `packages/mock-models`,
which the pipeline and crash tests start.

## Mock model server
| Endpoint | Latency | Limit | Failure |
|---|---|---|---|
| `POST /v1/predict/layout` | 50 ms | 100 RPS | 2% (500) |
| `POST /v1/predict/vlm` | 1500-3000 ms | 10 RPS | 5% (500) |

Body: `{"document_id": "doc1", "page": 1}`. Over the limit returns `429` with `Retry-After`.
Express 5 service. Full request/response models, status codes and curl recipes: [docs/mock-api.md](docs/mock-api.md).
Responses are deterministic per `(document_id, page)`. Failures are reproducible via `MOCK_SEED`.

```bash
npm test                                  # unit + route tests (no server needed)
npm run build && npm start -w @ocr/mock-models
curl -s localhost:8080/healthz
curl -s localhost:8080/metrics-lite       # request, 429, failure, in-flight counters
```

### Rate limiter note
Each endpoint uses a token bucket. The burst defaults to 200 ms of tokens (`ceil(rps/5)`: 20 for layout,
2 for VLM) so the limit is close to strict per second. Override with `LAYOUT_BURST` / `VLM_BURST`.
A client that sends in large bursts with idle gaps gets less than the nominal rate, because tokens cannot
accumulate beyond the burst. Pace requests (the orchestrator's own limiter does).

### Docker
```bash
cp .env.example .env && docker compose up --build mock-models     # mock only
```

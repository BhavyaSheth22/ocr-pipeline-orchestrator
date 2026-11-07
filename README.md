# OCR Pipeline Orchestrator

Monorepo for the Sarvam Vision takeaway: a high-throughput OCR pipeline orchestrator
(`packages/orchestrator`) in front of a mock layout/VLM model server (`packages/mock-models`).
The orchestrator container runs one pipeline process (SQLite queue, model calls, qpdf page extraction)
and, on Linux, 2 HTTP worker processes that share port 3000 (uploads, SSE, `/evaluate`).
Design, trade-offs and benchmark numbers: [ARCHITECTURE.md](ARCHITECTURE.md). How TTFP was brought under
200 ms, with the measurements: [docs/PERFORMANCE.md](docs/PERFORMANCE.md).

## Quick start (Docker)
```bash
docker compose up --build                                  # mock-models :8080, orchestrator :3000
curl -s -X POST localhost:3000/jobs -H 'content-type: application/pdf' --data-binary @doc.pdf
#   → 202 {"job_id":"…","trace_id":"…","events_url":"/jobs/…/events","total_pages":12,"backlog_pages":12,"eta_s":2}
curl -N localhost:3000/jobs/<job_id>/events                # SSE: job, page (layout|final)…, job.completed
curl -s localhost:3000/jobs/<job_id>/result                # assembled document tree (409 while running)
curl -s localhost:3000/metrics                             # Prometheus
docker compose logs -f | grep <trace_id>                  # one job across both services (see Logging and tracing)
LOG_REQUESTS=0 docker compose up -d                        # request logging off, for benchmark runs
docker compose --profile bench run --rm bench              # 50 jobs / 1000 pages, then 1,000 POST /evaluate calls; add --noise-rps 8 for 429 spikes, --eval-during for /evaluate under load
```

## Orchestrator API
| Method | Path | Purpose |
|---|---|---|
| POST | `/jobs` | Raw `application/pdf` body (≤ 100 MB, ≤ 100 pages), optional `Idempotency-Key` → 202. 413 too large, 422 not a PDF / too many pages, 503 + `Retry-After` when the backlog is full |
| GET | `/jobs/:id/events` | SSE stream, resumable with `Last-Event-ID` |
| GET | `/jobs/:id/result` | Document tree `{type:"document", children:[{type:"page", page, status, source, low_confidence, children}]}` |
| POST | `/evaluate` | `{prediction, ground_truth}` (same tree shape) → `{cer, wer, iou, ted, timing_ms}` |
| GET | `/metrics`, `/healthz` | Prometheus text; liveness |

Orchestrator settings (env): `PORT` (3000), `HTTP_WORKERS` (HTTP worker processes in front of the pipeline
process; default min(2, CPUs − 1) on Linux, 0 elsewhere; 0 runs everything in one process), `DATA_DIR` (./data), `MODEL_URL`, `QPDF_BIN` (qpdf),
`LAYOUT_RPS`/`VLM_RPS` (100/10, upper bounds for the adaptive rate), `MAX_QUEUED_PAGES` (6000),
`RESULT_TTL_MS` (1 h), `TED_MAX_STEPS` (5e6). Timing knobs used by tests: `LAYOUT_TIMEOUT_MS`,
`VLM_TIMEOUT_MS`, `BREAKER_OPEN_MS`, `BACKOFF_BASE_MS`, `DEGRADED_AFTER_MS`, `MAX_UPLOAD_BYTES`.
Logging: `LOG_REQUESTS=1` (request and model-call lines; off unless set, on in compose), `LOG_LEVEL=silent`.

## Logging and tracing
Both services write JSON lines to stdout. Every job has one W3C trace id: the client's, if `POST /jobs` sends a
valid `traceparent` header, otherwise a new one. It is returned as `trace_id` in the 202 and as `X-Trace-Id`
on every response (job routes answer with the job's trace id). The orchestrator sends it to the models as
`traceparent: 00-<trace_id>-<span_id>-01`, with a new `span_id` for each call. With `LOG_REQUESTS=1`:

| Service | `msg` | Fields |
|---|---|---|
| orchestrator | `request` | `trace_id`, `job_id`, `method`, `url`, `status`, `ms`, `aborted` |
| orchestrator | `model call` | `trace_id`, `job_id`, `page`, `lane`, `attempt`, `span_id`, `outcome`, `status`, `ms` |
| mock-models | `request` | `trace_id`, `span_id`, `method`, `url`, `status`, `ms`, `idempotency_key`, `aborted`, `traceparent` |

`grep <trace_id>` across both services' logs shows a job end to end, and `span_id` pairs each `model call`
with the mock line for that call. `/healthz` is not logged. Lifecycle lines (`job accepted`, retries,
fallbacks, `job completed`) carry `trace_id` whatever `LOG_REQUESTS` is set to.

## Setup (all tooling is project-local)
```bash
nvm install && nvm use      # reads .nvmrc -> Node 22
npm ci                      # installs into ./node_modules only
```
No global installs: TypeScript, tsx, @types/express and autocannon are devDependencies, run through npm scripts or `npx`.
The orchestrator needs **qpdf** on the machine (`sudo apt install qpdf`; the Docker image has it).

```bash
npm test                                         # mock + orchestrator tests (21 qpdf tests skip without qpdf)
npm run dev -w @ocr/mock-models                  # mock on :8080
MODEL_URL=http://127.0.0.1:8080 npm run dev -w @ocr/orchestrator     # orchestrator on :3000
npm run bench:load -w @ocr/orchestrator -- --orch http://127.0.0.1:3000 --mock http://127.0.0.1:8080
#   options: --jobs 50 --pages 1000 --noise-rps 8 (real 429s) --eval-pairs 1000 (0 skips /evaluate)
#            --eval-nodes 50 --eval-during (/evaluate calls while the jobs run). Exits non-zero on a dropped
#            page, a failed /evaluate call, or a TED ≥ 100 ms.
npm run bench:ted -w @ocr/orchestrator           # TED p50/p99 on 1,000 random 50-node pairs
```

No qpdf locally? Run the full orchestrator suite (all 72 tests, crash test included) in a throwaway
container. It reuses this checkout's `node_modules`, so it needs a Linux x64 host (WSL works):
```bash
docker run --rm -v "$PWD":/app:ro -w /app/packages/orchestrator -e QPDF_BIN=/usr/bin/qpdf node:22-bookworm-slim \
  sh -c 'apt-get update -qq && apt-get install -y -qq qpdf >/dev/null && node --import tsx --test test/*.test.ts'
```
**Clean-room run (no host `node_modules`)**: copies the checkout into the container, installs `qpdf` and
the build tools, runs `npm ci` there (so `better-sqlite3` is compiled for the container's Node 22, not the host's),
then runs all 72 tests with none skipped. Use this when the host Node is not 22 (`better-sqlite3` segfaults
under Node 18) or the host `node_modules` was built for another platform:
```bash
docker run --rm -v "$PWD":/src:ro node:22-alpine sh -c '
  set -e
  apk add --no-cache python3 make g++ qpdf >/dev/null
  mkdir /app && cd /src && tar --exclude=node_modules --exclude=dist --exclude=.git --exclude=data -cf - . | tar -xf - -C /app
  cd /app && npm ci --silent
  cd packages/orchestrator && npm test'
```
If `docker` fails with `permission denied ... /var/run/docker.sock`, the user is not in the `docker` group. Without
re-logging in, wrap the command in `sg docker -c "..."` (escape inner `$` and quotes), or fix it permanently with
`sudo usermod -aG docker $USER` and a new shell.

Known flake: `stream.test.ts` "reconnecting with Last-Event-ID" occasionally fails with `duplicates` `[6]` instead of
`[5]` under full-suite load (events 5 and 6 can arrive in one chunk before the client abort lands). It passes alone.

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

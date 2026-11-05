# OCR Pipeline Orchestrator

Monorepo for the Sarvam Vision takeaway. Currently contains the mock model server
(`packages/mock-models`); the orchestrator packages are added next.

## Setup (all tooling is project-local)
```bash
nvm install && nvm use      # reads .nvmrc -> Node 22
npm ci                      # installs into ./node_modules only
```
No global installs: TypeScript, tsx and autocannon are devDependencies, run through npm scripts or `npx`.

## Mock model server
| Endpoint | Latency | Limit | Failure |
|---|---|---|---|
| `POST /v1/predict/layout` | 50 ms | 100 RPS | 2% (500) |
| `POST /v1/predict/vlm` | 1500-3000 ms | 10 RPS | 5% (500) |

Body: `{"document_id": "doc1", "page": 1}`. Over the limit returns `429` with `Retry-After`.
Responses are deterministic per `(document_id, page)`. Failures are reproducible via `MOCK_SEED`.

```bash
npm test                                  # unit + route tests
npm run build && npm start -w @ocr/mock-models
curl -s localhost:8080/healthz
curl -s localhost:8080/metrics-lite       # request, 429, failure, in-flight counters
```

### Control endpoints (header `X-Admin-Token: $ADMIN_TOKEN`)
```bash
# force a 429 spike on the VLM for 5 s
curl -X POST localhost:8080/admin/spike -H "X-Admin-Token: $ADMIN_TOKEN" \
  -H 'content-type: application/json' -d '{"endpoint":"vlm","durationMs":5000,"mode":"429"}'
# "mode":"slow" multiplies latency instead. Also: GET /admin/state, POST /admin/config, POST /admin/reset (send body {})
```

### Rate limiter note
Each endpoint uses a token bucket. The burst defaults to 200 ms of tokens (`ceil(rps/5)`: 20 for layout,
2 for VLM) so the limit is close to strict per second. Override with `LAYOUT_BURST` / `VLM_BURST`.
A client that sends in large bursts with idle gaps gets less than the nominal rate, because tokens cannot
accumulate beyond the burst. Pace requests (the orchestrator's own limiter does).

### Docker
```bash
cp .env.example .env && docker compose up --build mock-models
```

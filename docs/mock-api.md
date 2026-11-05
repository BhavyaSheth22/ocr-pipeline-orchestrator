# Mock Model API

Base URL: `http://localhost:8080` (via `docker compose up`) or the `PORT` you set locally.
Built with Express 5. All bodies are JSON; send `content-type: application/json` on predict calls.
The mock simulates a natural 429 whenever requests arrive faster than the endpoint's limit.

## Models
```ts
// Request, both predict endpoints
interface PredictRequest {
  document_id: string;   // 1-128 chars
  page: number;          // integer, 1-100000
  image_ref?: string;    // optional, max 512 chars, ignored by the mock
}

// POST /v1/predict/layout, 200
interface LayoutResponse {
  document_id: string;
  page: number;
  boxes: { x: number; y: number; w: number; h: number; label: string; confidence: number }[];
  reading_order: number[];   // indexes into boxes, top-to-bottom then left-to-right
}

// POST /v1/predict/vlm, 200
interface VlmResponse {
  document_id: string;
  page: number;
  text: string;
  tables: { markdown: string }[];
  key_values: Record<string, string>;
}

// Any non-200 response
interface ErrorResponse { error: string; message?: string; retry_after_ms?: number }
```
The same `(document_id, page)` always returns the same payload. Failures follow `MOCK_SEED`.

## Predict endpoints
| Endpoint | Latency | Limit | Failure |
|---|---|---|---|
| `POST /v1/predict/layout` | 50 ms | 100 RPS | 2% |
| `POST /v1/predict/vlm` | 1500-3000 ms | 10 RPS | 5% |

| Status | When | Notes |
|---|---|---|
| 200 | Success | Header `X-Mock-Latency-Ms` shows the simulated latency |
| 400 | Invalid body (missing field, wrong type, unknown field, bad JSON) | `{"error":"bad_request","message":"missing required property \"document_id\""}` |
| 413 | Body over 16 KB | `{"error":"payload_too_large"}` |
| 429 | Request rate over the endpoint's limit | `Retry-After` (whole seconds, min 1), `X-RateLimit-Limit`, `X-RateLimit-Remaining: 0`, body `{"error":"rate_limited","retry_after_ms":47}` |
| 500 | Injected model failure | `{"error":"model_failure"}`, returned after the simulated latency |
| 503 | Too many requests in flight (200 layout / 100 vlm) | `Retry-After: 1`, `{"error":"overloaded"}` |

A rejected request (400/429/503) costs no simulated latency.

## Utility endpoints
| Endpoint | Returns |
|---|---|
| `GET /healthz` | `{"ok":true}` |
| `GET /metrics-lite` | Per endpoint: `{total, ok, rateLimited, failed, shed, hung, inFlight}` |

## Settings (environment variables, set before starting)
`PORT`, `MOCK_SEED`, `LAYOUT_RPS`, `VLM_RPS`, `LAYOUT_BURST`, `VLM_BURST`, `LAYOUT_LATENCY_MS`,
`VLM_LATENCY_MIN_MS`, `VLM_LATENCY_MAX_MS`, `LAYOUT_FAILURE_RATE`, `VLM_FAILURE_RATE`,
`LAYOUT_HANG_RATE`, `VLM_HANG_RATE` (fraction of requests that never answer), `LAYOUT_MAX_INFLIGHT`,
`VLM_MAX_INFLIGHT`, `LOG_REQUESTS=1` (one JSON log line per request).

## Try it
```bash
H='content-type: application/json'

# 1. Happy path
curl -i -X POST localhost:8080/v1/predict/layout -H "$H" -d '{"document_id":"inv-001","page":1}'
curl -i -X POST localhost:8080/v1/predict/vlm    -H "$H" -d '{"document_id":"inv-001","page":2}'   # takes 1.5-3 s

# 2. Validation error (400)
curl -i -X POST localhost:8080/v1/predict/layout -H "$H" -d '{"page":0}'

# 3. Natural 429: fire 4 VLM calls at once (burst is 2, so about 2 succeed and 2 get 429)
for i in 1 2 3 4; do curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8080/v1/predict/vlm -H "$H" -d '{"document_id":"d","page":1}' & done; wait

# 4. Sustained overload on layout: 300 requests/s for 5 s, then look at the counters
for i in $(seq 1 1500); do curl -s -o /dev/null -X POST localhost:8080/v1/predict/layout -H "$H" -d '{"document_id":"d","page":1}' & sleep 0.003; done; wait
curl localhost:8080/metrics-lite
```
Failure and latency behaviour is changed through the environment, for example
`VLM_FAILURE_RATE=1 npm start -w @ocr/mock-models`, or in `docker-compose.yml`.

## Automated tests
`npm test` runs the limiter, seeded-failure, payload and route tests (real HTTP on a random port).
For a load check, pace requests rather than bursting them: a burst larger than the bucket gets 429s by design.

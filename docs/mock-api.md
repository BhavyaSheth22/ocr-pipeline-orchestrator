# Mock Model API

Base URL: `http://localhost:8080` (via `docker compose up`) or the `PORT` you set locally.
Admin calls need the header `X-Admin-Token: <ADMIN_TOKEN>` (`change-me` in `.env.example`).
All bodies are JSON. Send `content-type: application/json` on every POST, with a body (`{}` is fine).

## Models
```ts
// Request, both predict endpoints
interface PredictRequest {
  document_id: string;   // 1-128 chars
  page: number;          // integer, 1-100000
  image_ref?: string;    // optional, ignored by the mock
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

// Any non-200 predict response
interface ErrorResponse { error: string; retry_after_ms?: number }   // 400 uses Fastify's validation format
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
| 400 | Body fails validation | `{"statusCode":400,"code":"FST_ERR_VALIDATION",...}` |
| 429 | Over the rate limit, or an active 429 spike | `Retry-After` (whole seconds, min 1), `X-RateLimit-Limit`, `X-RateLimit-Remaining: 0`, body `{"error":"rate_limited","retry_after_ms":47}` |
| 500 | Injected model failure | `{"error":"model_failure"}`, returned after the simulated latency |
| 503 | Too many requests in flight (200 layout / 100 vlm) | `Retry-After: 1`, `{"error":"overloaded"}` |

A rejected request (429/503) costs no simulated latency. A "hang" (optional, `hangRate`) never answers until the client disconnects.

## Utility endpoints
| Endpoint | Returns |
|---|---|
| `GET /healthz` | `{"ok":true}` |
| `GET /metrics-lite` | Per endpoint: `{total, ok, rateLimited, failed, shed, hung, inFlight}` |

## Admin endpoints (token required, else `401 {"error":"unauthorized"}`)
| Endpoint | Body | Effect |
|---|---|---|
| `GET /admin/state` | none | Current config and active spike per endpoint |
| `POST /admin/spike` | `{endpoint: "layout"\|"vlm", durationMs: 1-600000, mode: "429"\|"slow", factor?: 1-50}` | `429`: every request is rejected for the window. `slow`: latency multiplied by `factor` (default 5) |
| `POST /admin/config` | `{endpoint, rps?, burst?, latencyMinMs?, latencyMaxMs?, failureRate?, hangRate?, maxInFlight?}` | Changes behaviour live. Changing `rps` or `burst` refills the bucket |
| `POST /admin/reset` | `{}` | Restores defaults, reseeds the RNG, clears spikes and counters |

The `untilMs` value in a spike response is the server's internal monotonic clock. Treat it as opaque.

## Try it
```bash
H='content-type: application/json'; A='X-Admin-Token: change-me'

# 1. Happy path
curl -i -X POST localhost:8080/v1/predict/layout -H "$H" -d '{"document_id":"inv-001","page":1}'
curl -i -X POST localhost:8080/v1/predict/vlm    -H "$H" -d '{"document_id":"inv-001","page":2}'   # takes 1.5-3 s

# 2. Validation error (400)
curl -i -X POST localhost:8080/v1/predict/layout -H "$H" -d '{"page":0}'

# 3. Natural 429: fire 3 VLM calls at once (burst is 2)
for i in 1 2 3; do curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8080/v1/predict/vlm -H "$H" -d '{"document_id":"d","page":1}' & done; wait

# 4. Force failures (every VLM call returns 500), then restore
curl -X POST localhost:8080/admin/config -H "$H" -H "$A" -d '{"endpoint":"vlm","failureRate":1}'
curl -i -X POST localhost:8080/v1/predict/vlm -H "$H" -d '{"document_id":"d","page":1}'

# 5. Force a 429 spike for 5 s, then watch it clear
curl -X POST localhost:8080/admin/spike -H "$H" -H "$A" -d '{"endpoint":"layout","durationMs":5000,"mode":"429"}'
curl -i -X POST localhost:8080/v1/predict/layout -H "$H" -d '{"document_id":"d","page":1}'   # 429 with Retry-After

# 6. Slow spike: layout takes ~500 ms instead of 50 ms for 4 s
curl -X POST localhost:8080/admin/spike -H "$H" -H "$A" -d '{"endpoint":"layout","durationMs":4000,"mode":"slow","factor":10}'

# 7. Inspect, then reset everything
curl localhost:8080/metrics-lite
curl localhost:8080/admin/state -H "$A"
curl -X POST localhost:8080/admin/reset -H "$H" -H "$A" -d '{}'
```

## Automated tests
`npm test` runs the limiter, seeded-failure, payload and route tests (no server needed).
For a load check, pace requests rather than bursting them: a burst larger than the bucket gets 429s by design.

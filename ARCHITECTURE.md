# Architecture: OCR Pipeline Orchestrator

One Node.js process (API, scheduler, SSE) in front of two rate-limited model endpoints, with SQLite as the
queue, result store and event log. Every scored target was met in the local benchmark below; TTFP only
just (p99 199 ms).

```
client ── POST /jobs (raw PDF, streamed) ──▶ ingest: stream→disk (byte cap) → qpdf --show-npages → 1 SQLite tx
   ▲                                                      │
   │  GET /jobs/:id/events (SSE, Last-Event-ID)           ▼
   │                                   SQLite (WAL): jobs · pages (state machine) · events (append-only log)
   │                                      ▲ claim (UPDATE…RETURNING)       │ commit + notify(job)
   │                                      │                                 ▼
   └── stream.ts: cursor per client   Lane "layout": AIMD bucket ≤100 rps · ≤16 in flight · breaker
       (read log after cursor,        Lane "vlm":    AIMD bucket ≤10 rps  · ≤32 in flight · breaker
        write, await drain)                │ fetch + Idempotency-Key + traceparent
                                           ▼
                                  mock-models /v1/predict/{layout,vlm}
```

## Results (bench/load.ts, 50 jobs × 20 pages = 1,000 pages, spec mock settings)

Measured on WSL2 with every process on one machine and no container, so the cgroup row is empty.
Inside compose, `container_memory_peak_bytes` reports the container's peak, qpdf children included.

| Metric | Target | Result |
|---|---|---|
| Peak RSS | < 500 MB | **137 MB** (heap capped at 256 MB) |
| Zero drop | 0 unhandled failures | **50/50 jobs, 1,000/1,000 pages**. 47 VLM and 22 layout 500s retried; 0 fallbacks |
| Zero drop with real 429s | | `--noise-rps 6` (another client spending the VLM quota): **55 × 429, 20/20 jobs, 0 fallbacks** |
| TED latency, 50 nodes | < 100 ms | **p50 0.10 ms, p99 0.18 ms**; worst-case 50-node shape 1.5 ms; HTTP round trip p99 4.9 ms |
| TTFP (POST start → first SSE chunk) | < 200 ms | **p50 180 ms, p99 199 ms**: 135 ms upload accept + 43 ms 202 → event |
| First *page* event (reported) | | p50 467 ms, p99 727 ms |
| Client reorder buffer | ≤ window (32) | max 2 pages |

**Reading TTFP.** The first SSE chunk is the `job` header event. It is committed with the job, so it is
there the moment the stream opens; most of TTFP is the upload itself. With 50 uploads at once, 50
`qpdf --show-npages` spawns stall the event loop: about 50 ms measured for the bare binary. That
cost is why the margin is thin. The first *page* event can't beat about 500 ms for the 50th job, because 50
page-ones at the spec's 100 RPS layout limit take 500 ms. We report it separately rather than hide it.

## Key decisions

**Single process + SQLite (WAL, `synchronous=NORMAL`).** The bottleneck is the downstream 10 RPS, not
orchestrator CPU. One process gives in-process notify for SSE and a single writer, and crash recovery
needs no leases: everything `running` belonged to the dead process. SQLite is a library in the
process, not a service, so it adds no failure domain of its own. A process crash loses no committed
transaction. A power loss can lose the last few commits but cannot corrupt the database, and the replayed
work is deduplicated downstream (below). Rejected: Redis + BullMQ (static limiter, stalled-job re-runs,
still needs a store for results and the event log), RabbitMQ (two stores, two consistency boundaries),
an in-memory queue (fails crash recovery).

**Ingestion streams to disk, then random-access.** A PDF can't be split in one forward pass because the
xref is at the end. The body is piped through a byte counter to `data/tmp`, so a 100 MB upload grows the
heap by under 40 MB (tested). qpdf, outside the V8 heap, counts pages; each page is extracted lazily by
the layout task (temp name, then rename; at most 4 qpdf processes at once). Model calls carry `image_ref`,
never bytes (claim-check pattern).

**Pull-based lanes (bulkheads), not `Promise.all`.** Each model has one dispatcher loop that claims a page
from SQLite only when it can send it now: in-flight below the cap, breaker closed, token available. A
queued or retrying page is a row with `not_before`, never a closure or a timer, so memory is
O(in-flight). The caps come from Little's law: 10 rps × 3 s ≈ 32 for VLM. Claim order `ORDER BY page,
job_seq` runs every job's page 1 before anyone's page 2. A per-job window (watermark + 32) bounds the
client's reorder buffer.

**Rate limiting: lazy token bucket with AIMD.** Burst 1 spaces dispatches evenly. A 429/503 or an EWMA
latency over the ceiling multiplies the rate by 0.7, at most once per second, and pauses for
`Retry-After`. Each success adds back about max/20 rps per second. Retries draw tokens too, so retry traffic
can never exceed the adaptive rate. Rejected: Netflix-style gradient limits, because VLM latency is
uniform over 1.5–3 s by design, so the delay signal is noise. 429 is an explicit signal.

**A 429 is backpressure, not a failure.** It requeues the page with `not_before` and spends no attempt;
that is the zero-drop guarantee. 5xx, timeouts and network errors spend an attempt and retry with
full-jitter backoff (layout 5, VLM 3). VLM exhausted → finalize from the stored layout result
(`low_confidence: true, source: "layout"`, no extra call). Layout exhausted → VLM still runs. Both
exhausted → `status: "failed"`, still streamed and counted. A per-lane breaker (5 consecutive failures,
5 s open, one half-open probe) *pauses* the lane instead of burning attempts. A VLM outage over 30 s
switches to degraded mode: finish from layout.

**Exactly-once effects from at-least-once delivery.** A transition, its result, its event and (on the
last page) job completion commit in one transaction, guarded by `WHERE state = '<lane>_running'`, so a
stale outcome changes nothing. Boot recovery is one statement: `*_running → *_ready`. Duplicate model
calls after a SIGKILL are prevented with an IETF `Idempotency-Key`: `job:page:lane:attempt`, persisted at
claim time and kept by recovery. The mock stores the response *promise* of every admitted key (bounded
at 10k, oldest evicted), so a resend joins or replays it without running the model or spending a token.
`crash.test.ts` kills the process with VLM calls in flight, restarts it, and asserts one `final` event
per page and **mock `vlm.ok` == pages finalized via VLM**. The mock's `ok` counts model executions only;
replays count as `replayed`. `POST /jobs` takes an `Idempotency-Key` too.

**SSE fan-out: event log + cursor.** Each connection holds a cursor, not a queue. It reads events
after the cursor, writes them, honours `res.write()` backpressure (drops a reader that hasn't drained
after 30 s), and sleeps on a notify that coalesces bursts. Replay after reconnect is the same loop. Every
event carries a gap-free per-job `seq` as its SSE `id`. Pings carry no id, because that would move the
client's resume point. Page events carry `watermark`: every page below it is final. The client drops
`seq ≤ last`, buffers finals by page and emits contiguously (`bench/sse-client.ts`). Admission control
answers 503 with a jittered `Retry-After` computed from the live VLM rate once the backlog passes
6,000 pages (about 10 minutes of VLM work). The 202 body carries `backlog_pages` and `eta_s`.

**Evaluation (`POST /evaluate`).** One iterative pass per tree validates the input and builds postorder
arrays, keyroots, per-page text and boxes; there is no recursion. TED is Zhang-Shasha over typed arrays
with interned labels. Its exact cost is W(A)·W(B) steps (W = Σ keyroot subtree sizes), known before it
runs, so over-budget inputs get 413 (`TED_MAX_STEPS` = 5·10⁶ ≈ 40 ms, which also bounds memory).
Every 50-node pair passes. APTED is the upgrade path for deep or degenerate trees. CER and WER share one
Levenshtein routine (code points vs interned word ids), page-aligned and micro-averaged. IoU uses greedy
one-to-one matching at 0.5, COCO style.

**Observability.** JSON logs with `AsyncLocalStorage` context (`trace_id, job_id, page, lane, attempt`).
The job's W3C trace id is persisted, survives a crash, and is sent as `traceparent` on every model call.
The mock logs it (`LOG_REQUESTS=1`). `GET /metrics` serves queue depth, worker lag, in-flight, AIMD
rate, breaker state, responses by code, 429 requeues, retries, fallbacks, recovered pages, SSE clients,
RSS and event-loop lag.

## Scale-out path

Postgres replaces SQLite: claims use `FOR UPDATE SKIP LOCKED` with a `lease_until` column and heartbeat,
SSE fan-out uses `LISTEN/NOTIFY`, and the per-endpoint limit becomes a shared GCRA in Redis, so N
orchestrators respect one quota. Ingestion moves to an object store, and the claim-check URI becomes an
`s3://` key. A worker thread takes `/evaluate` off the event loop.

## Known limitations

- If a SQLite write throws mid-pipeline (disk full), that page stays `running` until the next restart
  recovers it.
- qpdf is spawned per upload and per page. Under 50 simultaneous uploads the spawns cost about 50 ms of
  event-loop time, which is most of the TTFP margin. Batching extraction per claim window is the next
  step.
- Multi-page TIFF and images are out of scope. The same pipeline applies with a different splitter.
- `docker compose` files are written but were not built in the development environment, which had no Docker access.

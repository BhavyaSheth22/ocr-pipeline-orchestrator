# Architecture: OCR Pipeline Orchestrator

An orchestrator in front of two rate-limited models (layout 100 RPS, VLM 10 RPS). SQLite is the queue,
result store and event log. One **pipeline process** owns the queue and calls the models; **HTTP worker
processes** (2 by default, Linux) take uploads, stream events and run `/evaluate`. Measurement history:
[docs/PERFORMANCE.md](docs/PERFORMANCE.md).

```
 POST /jobs ─▶ HTTP workers ×2 (SO_REUSEPORT :3000): body → disk → page count → 1 transaction → 202
 SSE       ◀─   SSE cursor over the event log · /result · /evaluate · /metrics
                     │ wake (IPC)            ▲ notify (IPC, batched per loop turn)
                     ▼                       │
               pipeline process (no port): lane layout ≤100 rps ≤16 in flight · lane vlm ≤10 rps ≤32
                 · qpdf page extraction (≤4) · crash recovery · retention
                     SQLite WAL: jobs · pages (state machine) · events      ──▶ mock-models (Idempotency-Key)
```

## Results

`bench/load.ts`, 50 jobs × 20 pages at once, default mock (2% layout / 5% VLM failures), `docker compose`
on Docker Desktop/WSL2, cold start.

| Metric | Target | Result |
|---|---|---|
| Peak memory | < 500 MB | **158 MB** container (cgroup, qpdf included); 302 MB RSS summed over 3 processes |
| Zero drop | 0 unhandled | **1,000/1,000 pages**, 47 VLM + 22 layout 500s retried; with `--noise-rps 8`: **543 real 429s, 0 failed, 0 fallbacks** |
| TED, 50 nodes | < 100 ms | **p99 0.21 ms** (1,000 random pairs); worst shape (comb) 2.6 ms; `/evaluate` round trip p99 4.8 ms |
| TTFP: POST → first SSE chunk | < 200 ms | **p50 105–112 ms, p99 118–125 ms** (one process: p99 142–144 ms same session) |
| First page event / first final page | reported | partial layout event p50 380–396 ms; final (with VLM text) p50 5.0–5.1 s, p99 8.0 s |

TTFP's first chunk is the `job` header event, committed with the job. Each page then streams twice: a
partial `stage: "layout"` event and the `stage: "final"` OCR result. The 50th job's first page event
cannot beat ~600 ms: page 1 of 50 jobs at 100 RPS is 500 ms of dispatch.

## 1. Queue and state machine

SQLite (WAL) is the queue because the bottleneck is the downstream 10 RPS, not throughput: one process
claims pages, so crash recovery needs no leases (`*_running → *_ready` at boot) and a crash loses no
committed transaction. Rejected: Redis + BullMQ (static limiter, still needs a result store), RabbitMQ
(two stores, two consistency boundaries), an in-memory queue (no crash recovery).

```
layout_ready ─claim▶ layout_running ─200▶ vlm_ready ─claim▶ vlm_running ─200▶ done (final, source vlm)
   429/503 → back to ready, no attempt spent · 5xx/timeout → attempt+1, not_before = now + jitter
   layout exhausted (5) → vlm_ready without layout · vlm exhausted (3) or outage > 30 s → done from layout
   (low_confidence) · both exhausted → done, status failed (still streamed and counted)
```

- **Claim** is one `UPDATE … RETURNING` over `ORDER BY page, job_seq`: every job's page 1 before anyone's
  page 2, only pages inside the job's window (watermark + 32). A waiting page is a row, never a timer.
- **Exactly-once effects.** A transition, its result, its event and job completion commit in one
  transaction guarded by `WHERE state = '<lane>_running'`, so a stale outcome is a no-op. Model calls carry
  `Idempotency-Key: job:page:lane:attempt`, persisted at claim and kept by recovery; the mock replays a
  key instead of re-running the model. `crash.test.ts` SIGKILLs mid-run and asserts one final event per
  page and VLM executions == pages finalized via VLM.
- **Events.** `job` → per page `page` (`stage: layout`, then `stage: final` with `status`, `source`,
  `low_confidence`) → `job.completed`. A gap-free per-job `seq` is the SSE id (`Last-Event-ID` resumes);
  `watermark` marks every page below it final. `/result` returns the document once done (409 before).
- **Processes.** Workers commit jobs on their own connections and send *wake*; the pipeline sends
  *notify*. Multi-statement writes use `BEGIN IMMEDIATE` so processes wait on the busy timeout rather than
  fail a read→write upgrade. Dead workers respawn in 1 s; workers exit if the pipeline dies.

## 2. Backpressure and memory bounds

Work is pulled, never pushed: a lane claims a page only when in-flight is under its cap, the breaker is
closed and a token is available, so memory is O(in-flight), not O(queued).

- **Rate:** token bucket per lane (burst 1), AIMD: a 429/503 or EWMA latency over the ceiling cuts the
  rate ×0.7 (≤ once/s) and honours `Retry-After`; each success adds ~max/20 rps per second.
- **429 is backpressure, not failure:** requeued with `not_before`, no attempt spent (the zero-drop
  guarantee). A breaker (5 failures, 5 s open, one probe) pauses a lane instead of burning attempts.
- **Front door:** past 6,000 queued pages, 503 + jittered `Retry-After` from the live VLM rate, before
  reading the body. **SSE:** honours `res.write()` backpressure; drops a reader not drained in 30 s.

| Bound | Value |
|---|---|
| Upload | ≤ 256 KiB in memory per upload (≤ 12.5 MB per 50), larger streamed in ~64 KB chunks; 100 MB (413), 100 pages (422) |
| In flight / processes | 16 layout, 32 VLM calls (Little's law, 10 rps × 3 s); ≤ 4 qpdf extractions |
| Queue, clients | 6,000 queued pages; reorder buffer ≤ 32 pages (measured 2); per SSE client a cursor + ≤ 100 events |
| Per process | V8 heap 256 MB, SQLite cache 16 MB; page-count reader inflates ≤ 8 MB, else qpdf |
| Evaluation, retention | 1 MB body; TED ≤ 5·10⁶ steps (413); finished jobs purged after 1 h |

## 3. TED: algorithmic complexity

- **Zhang–Shasha**, unit costs, iterative, typed arrays, interned labels. For trees of n and m nodes it
  runs exactly **W(A)·W(B)** inner steps, W(T) = Σ keyroot subtree sizes ≤ n·min(depth, leaves): time
  **O(n·m·min(d_A,l_A)·min(d_B,l_B))**, near O(n·m) for shallow documents, O(n²m²) worst (a comb).
  Space **O(n·m)**: one tree-distance table and one reused forest table, no inner-loop allocation.
- **Budget before work:** W(A)·W(B) > 5·10⁶ (~40 ms) → 413. Since W(T) ≥ n, this also caps the tables
  at ~40 MB.
- **CER/WER:** one Levenshtein over code points or interned word ids, prefix/suffix trimmed, O(n·m) time,
  O(min) space. **IoU:** greedy one-to-one match at ≥ 0.5, O(k·g·log(k·g)). `/evaluate` runs in the HTTP
  workers, never on the pipeline loop. APTED is the upgrade path for deep or degenerate trees.

## 4. Trade-offs and scale-out path

- **Cluster mode** costs ~85 MB RSS per worker: 2 workers give TTFP p99 118–125 ms at 302 MB; 4 gave
  94–121 ms at 468 MB. Linux only (`SO_REUSEPORT`, 32 sockets: libuv accepts one connection per socket
  per loop turn); other platforms run one process.
- **Custom page counter** (430 lines) instead of a ~3 ms qpdf fork per upload: 17–53 µs per document,
  ~10× cheaper than pdfjs-dist; anything unusual goes to qpdf, which runs as a separate process so a
  crash in its C++ parser cannot take the orchestrator down.
- **Limitations:** qpdf has no timeout, and a qpdf crash on upload returns 500 not 422; rate control
  reads a local stall as congestion; SQLite is one host, one writer; numbers drift with Docker Desktop.

| Today | Scaled out | Pitfall |
|---|---|---|
| SQLite | Postgres, `SKIP LOCKED` claims, leases + heartbeat | expired lease → duplicate model call; keep keys stable |
| AIMD per process | shared limiter per endpoint (GCRA in Redis) | N AIMD loops overshoot one quota |
| IPC notify | `LISTEN/NOTIFY` or Redis pub/sub | `LISTEN` breaks under PgBouncer transaction pooling |
| Local disk | object store, `s3://` claim checks | ranged GETs on the TTFP path; qpdf needs local files |
| Forked workers | separate HTTP tier behind a load balancer | SSE buffering and idle timeouts; resume via `Last-Event-ID` |
| Global claim order | partitioned queues per tenant | one tenant's burst delays everyone's first page |

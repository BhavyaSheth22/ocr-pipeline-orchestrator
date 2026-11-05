# Implementation Plan: OCR Pipeline Orchestrator

This plan covers everything the assignment asks for after the mock model server, which already exists in
`packages/mock-models`. For each core decision it gives the choice, the alternatives considered, and why
they were rejected. It also sets coding rules to keep the codebase small enough to defend line by line in
the deep-dive interview.

---

## 0. How each scored metric is won

| Metric (weight) | Target | Mechanism that hits it | How the bench proves it |
|---|---|---|---|
| Peak RSS (20%) | < 500 MB, 50 concurrent jobs | Uploads stream to disk. The queue, results and event log live in SQLite, not in the heap. Each lane caps its in-flight requests. SSE fan-out keeps only a per-client cursor. SQLite page cache is capped. Heap is capped. | Bench polls `/metrics` every 200 ms and reports max `process_resident_memory_bytes` plus cgroup `memory.peak` (container, includes qpdf children) |
| Zero drop (20%) | 0 unhandled failed jobs during 429 spikes | A 429 is backpressure, not a failure: it costs no retry attempt, `Retry-After` is honoured and the rate backs off (AIMD). The queue is durable. VLM failures fall back to layout. A page never disappears, it always reaches `done`. | Bench asserts every job emits `job.completed` with exactly N final pages. A **noisy-neighbour** mode spends the mock's VLM quota from outside to create real 429 spikes. |
| TED latency (15%) | < 100 ms for 50 nodes | Zhang-Shasha over flat typed arrays, iterative postorder, no allocation in the inner loop. Prototype: 0.11 ms p50 for 50-node document trees, 1.9 ms for the worst 50-node shape (§3.9). | `bench/ted.ts`: p50/p99 over 1,000 random 50-node trees, both in-process and over HTTP |
| TTFP (15%) | < 200 ms ingestion → first SSE chunk | The `job` header event is in the log before `POST /jobs` returns, so it replays the moment the stream opens. Pages run layout first (~50 ms), and the layout result streams as a partial `page` event. Scheduling is page-index-first round-robin, so every job's page 1 is dispatched before anyone's page 2. | Bench records three numbers per job: time to first event, time to first page event, time to first final page |

**Our reading of TTFP:** "initial SSE chunk" is the first event (the `job` header). We also report time
to first *page* event, because under 50 simultaneous jobs the layout limit of 100 RPS puts the 50th job's
first page at about 550 ms whatever we do. ARCHITECTURE.md will say this plainly instead of hiding it.

---

## 1. Rules for keeping the code small

These rules apply to every phase. They exist because the mock picked up extra pieces (admin endpoints)
that no grader or test would ever call.

1. **Every file, endpoint, env var and dependency traces to a line of the assignment or to a test.**
   If nothing in the spec or the test suite uses it, it is not written.
2. **No admin or test-only control surfaces.** Failure modes are produced by real traffic (the
   noisy-neighbour bench) or by env vars set at startup (the mock's existing `*_FAILURE_RATE`,
   `*_HANG_RATE`, `VLM_RPS`).
3. **Use platform built-ins first:** `fetch` + `AbortSignal.timeout`, `stream.pipeline`,
   `AsyncLocalStorage`, `timers/promises`, `node:test`, `crypto.randomUUID`.
4. **Runtime dependencies are exactly:** `express` (user preference, already used by the mock),
   `better-sqlite3` (see §3.2), `prom-client` (standard Prometheus exposition; gives RSS and event-loop
   lag for free). Dev only: `pdf-lib` (the bench generates test PDFs). No queue library, no validation
   library, no logger library.
5. **A one-line comment above every function**, inline lambdas included (user rule). Comments say *why*,
   not *what*.
6. **Classes only for things that hold state:** `AdaptiveTokenBucket`, `CircuitBreaker`, `Lane`. Everything
   else is a function.
7. **One config module** (`config.ts`). Each env var has a default taken from the spec. Add a knob only
   when a test or the bench sets it.
8. **Size budget:** orchestrator `src/` stays at roughly 1,000 to 1,200 lines. If a module grows past its
   budget in §7, simplify it before adding anything else.

---

## 2. Architecture at a glance

```
             POST /jobs (application/pdf, streamed)
client ───────────────────────────────────────────┐
   ▲  GET /jobs/:id/events (SSE, Last-Event-ID)   │
   │                                              ▼
   │   ┌──────────────────── orchestrator (one Node process) ─────────────────────┐
   │   │  ingest.ts: stream→disk (byte cap) → qpdf --show-npages → 1 SQLite tx       │
   │   │                                                                          │
   │   │  SQLite (WAL)  jobs · pages (state machine) · events (append-only log)    │
   │   │        ▲ claim (UPDATE…RETURNING)          │ commit + notify(jobId)       │
   │   │        │                                    ▼                             │
   │   │  Lane "layout"  bucket 100 rps · in-flight ≤ 16 · breaker                │
   │   │  Lane "vlm"     bucket  10 rps · in-flight ≤ 32 · breaker  (bulkheads)    │
   │   │        │ fetch + Idempotency-Key + traceparent                           │
   │   │  stream.ts: per-client cursor → read events > cursor → res.write/drain   │
   └───┼──────────────────────────────────────────────────────────────────────────┘
       │                              │
       └──────────────────────────────┼──► mock-models  /v1/predict/layout · /v1/predict/vlm
                                      ▼
                                   /data volume: jobs/<id>.pdf, pages/<id>/<n>.pdf, orchestrator.db
```

The API, scheduler and stream server run in **one process**, and the queue is **SQLite**. docker-compose
runs two services, `orchestrator` and `mock-models`, plus a one-shot `bench` under a compose profile. §3.1
explains why this beats separate API and worker containers at this scale, and what the scale-out path is.

---

## 3. Decisions, alternatives, and reasons

### 3.1 Process topology

| Option | Pros | Cons |
|---|---|---|
| **Single process: API, scheduler and SSE together (chosen)** | Completion events reach SSE clients through an in-process notify (microseconds, no polling, no pub/sub). SQLite has one writer, so there is no lock contention. Crash recovery is trivial: everything `running` belonged to the process that died, so no leases or heartbeats are needed. A SIGKILL test is one `docker compose kill`. | One node only. A CPU-heavy `/evaluate` blocks streaming (bounded to about 40 ms by the TED step budget, §3.9). |
| API and workers as separate containers over a shared queue | Matches "workers" in the compose wording. Scales out. | Needs cross-process eventing (Redis pub/sub, PG LISTEN/NOTIFY, or polling) to push SSE, plus leases with heartbeats for crash recovery. Much more code for no gain on one machine: the workload is I/O-bound and the downstream limit is 10 RPS. |

**Why:** the bottleneck is the downstream 10 RPS, not orchestrator CPU. One Node event loop holds
thousands of in-flight sockets. The "workers" are the two `Lane` dispatchers. **Scale-out path (for
ARCHITECTURE.md):** move to Postgres. Claims use `FOR UPDATE SKIP LOCKED` with a `lease_until` column and
a heartbeat. SSE fan-out uses `LISTEN/NOTIFY`. The per-endpoint rate limit moves to a shared GCRA in Redis
(as in redis-cell), so N workers together respect one quota.

### 3.2 Queue and persistence

| Option | Pros | Cons / why rejected |
|---|---|---|
| **SQLite (WAL) via `better-sqlite3` (chosen)** | Embedded, so no extra service and no extra RSS. Transactions let a state change, the result and the event append commit atomically. WAL + `synchronous=NORMAL` loses nothing on a process crash (SIGKILL), only possibly on power loss. The sync API is a feature here: each statement takes microseconds, and every transaction is atomic with respect to the event loop. | Single node. The sync calls do block the loop, but they are microsecond-scale and indexed. |
| `node:sqlite` (built in) | No native addon. | Still experimental on Node 22, which `.nvmrc` pins, and prints an `ExperimentalWarning`. It is only a release candidate even on Node 24.15+/25.7+. Revisit when it is stable. |
| Redis + BullMQ | The Node industry default. Retries, backoff, rate limiter and stalled-job recovery built in. | Extra service and memory. BullMQ's limiter is static per queue, not adaptive. Stalled-job recovery is at-least-once and re-runs the job (a duplicate model call). Results and the SSE log still need a store. It also hides the very mechanics the interview probes. |
| RabbitMQ | `prefetch` gives consumer backpressure. DLX for retries. | Delayed retry needs TTL+DLX gymnastics or a plugin. Still needs a second store for results and events, so two systems and two consistency boundaries. |
| Postgres | `SKIP LOCKED` + `LISTEN/NOTIFY` is the best scale-out story. | Heavier compose setup and a client dependency. Kept as the documented scale-out path. |
| Pure in-memory queue | Fastest and simplest. | Fails crash recovery outright. |

**Why:** one store holds the state machine, the results, the replayable event log and idempotency, all
with atomic transitions. Settings: `journal_mode=WAL`, `synchronous=NORMAL`, `cache_size=-16000` (16 MB
cap), `foreign_keys=ON`.

### 3.3 PDF ingestion and page splitting (Module D: zero-OOM)

Key fact found in research: **a general PDF cannot be split in one forward streaming pass.** The xref
table and trailer sit at the end of the file (`startxref`). The spec itself says readers should start
from the end, and PDFBox and HummusJS maintainers say the same. Only *linearized* PDFs are streamable,
and most aren't linearized. So "no full byte array in memory" means **stream to disk, then
random-access** the file.

| Option | Verdict |
|---|---|
| `express.raw()` / multer memory storage | Rejected. Buffers the whole 100 MB in the heap; 50 concurrent uploads means 5 GB. |
| `pdf-lib` / `pdfjs` loading from a Buffer | Rejected. The whole file plus its object graph (2-3× the file size) lives in the V8 heap. |
| Single-pass streaming parser | Impossible in general (xref at EOF). |
| **`stream.pipeline(req, byteLimit, fs.createWriteStream(tmp))` + `qpdf` CLI (chosen)** | The pipeline propagates backpressure, so heap use is about 64 KB per upload. qpdf is a mature C++ tool that seeks to the xref and copies only the objects a page needs. Its memory scales with the object count, not the byte size, and it runs outside the V8 heap. |
| mutool / pdfium | Also viable. They are rendering engines, which is more than we need; qpdf is a lossless structural splitter (Apache-2.0, `apk add qpdf`). |

**Flow:**
1. `POST /jobs` with `Content-Type: application/pdf`. An early 413 if `Content-Length` is over 100 MB, and
   a byte-counting transform enforces the cap anyway.
2. Write to `/data/tmp/<uuid>`, run `qpdf --show-npages` (~ms), and reject non-PDFs or anything over 100
   pages with 422.
3. Rename to `/data/jobs/<jobId>.pdf`. Then **one transaction** inserts the job, N page rows and the
   `job` header event. Respond 202 `{ job_id, events_url, backlog_pages, eta_s }` (§3.11).
4. **Page splitting happens lazily** inside the layout task: `qpdf <pdf> --pages . N -- pages/<id>/N.pdf`
   writes to a temp name and then renames, so it is idempotent across crashes. At most `SPLIT_CONCURRENCY`
   (4) qpdf processes run at once. The page file is deleted when the page reaches `done`, and the source
   PDF when the job completes. Disk and child-process memory are therefore bounded by in-flight pages, not
   by queued pages.
5. The model request carries `image_ref` (the page path), never bytes. This is the **claim-check
   pattern**: in production it would be an object-store URI, and the model service would rasterise.

**Risk:** spawning a process per page costs about 1-3 ms of main-thread time each, about 10% of the loop at
100 pages/s. If the bench shows event-loop lag, batch-extract one window (`--pages . a-b` with
`--split-pages`) per claim. The design does not change.

Scope: multi-page TIFF/image payloads are out of scope. ARCHITECTURE.md will say the same pipeline applies
with a different splitter.

### 3.4 Scheduling: pull-based lanes (bulkheads)

The naive approach, which the assignment explicitly warns about, creates a promise per page
(`Promise.all(pages.map(...))`) behind a semaphore. With 5,000 queued pages that means 5,000 closures,
timers and retained payloads.

**Chosen:** two `Lane`s (layout and VLM). Each runs **one dispatcher loop** that *pulls* work from SQLite
only when it can actually send it:

```
loop:
  await inFlight < maxInFlight            // hard memory bound (wake on completion)
  task = claimNext(lane)                  // one UPDATE…RETURNING; null → await wake/nextNotBefore
  await bucket.take()                     // pacing; also honours Retry-After pause
  if breaker.open → release task, sleep until half-open
  run(task) (not awaited; finally: inFlight--, wake)
```

- **Memory is O(in-flight), not O(queued).** A retrying page is a row with a `not_before` timestamp, not
  a sleeping `setTimeout` closure.
- **Bulkheads:** separate lanes keep the slow VLM from head-of-line-blocking layout.
- **In-flight caps come from Little's law** (L = λ·W): VLM is 10 rps × 3 s max ≈ 30, so the cap is 32.
  Layout is 100 rps × (50 ms + split ~20 ms) ≈ 7, so the cap is 16. The cap is the memory safety bound;
  the token bucket is the politeness bound.
- **Fairness and TTFP:** claim order is `ORDER BY page, job_seq`, which round-robins by page index across
  jobs. Every job's page 1 goes first, so one 100-page job cannot starve 49 small ones.
- **Sliding window (Module B "state buffer windows"):** a page is claimable only if
  `page < job.watermark + job.window`, where `watermark` is the lowest page not yet done and `window` is 32.
  Like TCP flow control, this **bounds the client's reorder buffer at `window` pages** and adds per-job
  fairness. The trade-off is head-of-line blocking behind a retrying page, which retries plus fallback
  keep short. Fifty jobs × 32 is 1,600 claimable pages, well over lane capacity, so the window never
  limits throughput in the benchmark.
- **Wake-ups:** a lane sleeps until the earliest of: a notify (new job, or a page became ready), the next
  `not_before`, or a freed slot. The sleep uses an `AbortController`-cancelled `timers/promises.setTimeout`,
  so no timers leak from `Promise.race`.

Rejected: a priority heap in memory mirrored to the DB (two sources of truth). One global queue for both
models (no bulkhead). BullMQ's per-queue limiter (static).

### 3.5 Rate limiting and adaptive backpressure (Module A)

**Choosing the limiter algorithm:**

| Algorithm | Notes | Verdict |
|---|---|---|
| Fixed window counter | 2× burst at window edges | Rejected |
| Sliding log | O(requests) memory | Rejected |
| Sliding window counter | Good for *inbound* quotas; approximate | Not needed client-side |
| Leaky bucket (queue) | Smooth, but needs its own queue | Equivalent to token bucket with burst = 1 |
| GCRA | Token bucket with one timestamp of state; ideal in Redis | The scale-out choice (§3.1) |
| **Token bucket, lazy refill (chosen)** | O(1) state, no timers, burst configurable | Chosen. Burst = 1 gives evenly spaced dispatch (10 ms / 100 ms gaps), which fits inside the mock's burst of `rps/5` |

**Choosing the adaptive control law:**

| Option | Signal | Verdict |
|---|---|---|
| Static rate | none | Rejected: the spec requires slowing down on 429s or latency spikes, and the quota may be shared |
| Netflix Gradient2 / Vegas (delay-based concurrency) | RTT vs min-RTT | Rejected as the primary signal. VLM latency is *intrinsically* uniform over 1.5-3 s (2× spread), so the gradient is noisy and needs windowed RTT-noload tuning |
| **AIMD on the token rate (chosen)**, like TCP Reno/CUBIC | 429/503 (explicit, like ECN) + an EWMA latency ceiling | Chosen. An explicit 429 makes loss-based control precise. AIMD converges to fair shares, which is exactly the noisy-neighbour case |

**Control law** (`AdaptiveTokenBucket`, about 60 lines):
- **Multiplicative decrease:** on 429/503 or `ewmaLatency > ceiling`, set `rate = max(min, rate × 0.7)`
  (β = 0.7, CUBIC's value; gentler than 0.5 because we also pause). Apply **at most one decrease per
  1 s window**, so a burst of 429s from the same window counts as one congestion event.
- **Pause:** set `pausedUntil = max(pausedUntil, now + Retry-After)`, using the mock's `retry_after_ms` when
  present.
- **Additive increase:** on success, `rate = min(max, rate + (max/20)/rate)`, which adds roughly max/20 RPS
  per second. Recovery from half rate takes about 10 s.
- **Latency ceilings:** layout 250 ms, VLM 4,000 ms, with EWMA α = 0.2.
- **Retries draw tokens too.** Retry traffic can never push total load past the adaptive rate, so no
  separate retry budget (Finagle/gRPC style) is needed. ARCHITECTURE.md should state this.

### 3.6 Failure handling: retries, circuit breaker, fallback

**Classifying each outcome:**

| Outcome | Counts as attempt? | Action |
|---|---|---|
| 2xx | — | Advance state |
| 429, 503 | **No** | `not_before = now + Retry-After`; bucket decrease + pause. **This is the zero-drop guarantee.** |
| 5xx, timeout (`AbortSignal.timeout`: layout 1 s, VLM 5 s), network error | Yes | Backoff with **full jitter** (`random(0, min(8 s, 250 ms·2^attempt))`, the AWS Architecture Blog variant with the least total work); breaker records a failure |
| Other 4xx | — | Programming error: page `done` with `status:"failed"`, logged at error level |

- **Max attempts:** layout 5 (2% failure rate, so exhausting all five is about 3·10⁻⁹), VLM 3.
- **Pipeline order:** layout first, then VLM. This gives (a) a fast partial result for TTFP, and (b) a
  **free fallback**: when VLM is exhausted, the page is finalized from the *already stored* layout result
  with `low_confidence: true, source: "layout"`. No extra downstream call is made, and that is what
  "fall back to Fast-Layout-Model" means here. If layout itself is exhausted, VLM still runs. If both
  fail, the page is `done` with `status:"failed"`: handled, recorded, streamed, and the job still completes.
- **Circuit breaker (per lane):** `closed` opens after **5 consecutive** failures. It stays `open` for
  5 s, during which the lane *pauses* (queued pages wait, no attempts are consumed). It then goes
  `half-open` and lets 1 probe through: success closes it, failure reopens it. Random 5% failures almost
  never trip it (0.05⁵ ≈ 3·10⁻⁷); it exists for outages.
- **Degraded mode:** if the VLM breaker stays open for more than 30 s, pending VLM pages are finalized via
  layout fallback. This bounds job completion time during a long outage. It costs about 5 lines, and
  without it "zero unhandled failed jobs" could become "jobs that never finish".

Rejected: breaker-open treated as an immediate attempt failure (Hystrix/resilience4j default). A 5 s blip
would then burn every page's retries and mass-downgrade to low confidence. Pausing is backpressure and
keeps quality.

### 3.7 Crash recovery and idempotency (Module D)

There are two guarantees to prove: (1) completed pages never re-run, and (2) **no duplicate downstream
model calls**.

1. **Atomic transitions.** Storing a page result, changing its state, appending the event and (on the
   last page) completing the job all happen in **one SQLite transaction**, guarded by
   `WHERE state = '<lane>_running'`. If `changes = 0`, the outcome is a stale duplicate and is dropped.
   The event log is therefore exactly-once per transition.
2. **Recovery on boot** is one statement: `*_running → *_ready`, with `not_before = 0`. No leases are
   needed because a single process owned every running row (§3.1). Pages that are `done` are never
   touched, and pages past layout resume at VLM.
3. **Duplicate model calls.** A SIGKILL can land after the mock received the request but before our
   commit. No client can know whether that call ran, which is why exactly-once *delivery* is impossible.
   The industry answer is an **idempotency key on the downstream call** (IETF
   `draft-ietf-httpapi-idempotency-key-header`, the Stripe pattern):
   - At claim time, `call_key = COALESCE(call_key, '<job>:<page>:<lane>:<attempt>')`. The key is
     persisted before the request is sent.
   - Recovery **keeps** `call_key`, so the re-dispatch sends the same `Idempotency-Key`. A real failure
     (5xx/timeout) clears it, so the next attempt gets a new key.
   - **Mock change (the only one):** the mock stores admitted requests by key in a bounded `Map` of
     *promises* (insertion-order eviction at 10k). A replay of a key whose call is in flight joins the
     same promise; a replay of a finished call returns the stored response. Replays spend no rate-limit
     token, and the `replayed` counter goes in `/metrics-lite`.
   - Net effect: each `(page, stage, attempt)` runs **at most once** in the model, even across a SIGKILL.
     This is directly provable from the mock's counters.
4. **Ingestion idempotency:** an optional `Idempotency-Key` on `POST /jobs` (UNIQUE column). A client
   retrying after an API crash gets back the original job and the body is not read. Uploads are written
   to `tmp/`, then renamed, then committed. On boot, files in `tmp/` and job files without a job row are
   deleted.
5. **Retention:** a `setInterval` sweep (unref'd) deletes jobs, pages and events older than
   `RESULT_TTL_MS` (1 h). Without it, disk grows forever under sustained load.

### 3.8 Streaming (Module B)

**Transport:** **SSE.** It is plain HTTP, unidirectional (which matches the data flow), and `EventSource`
reconnects automatically with `Last-Event-ID` resume built in. WebSocket was rejected: it is
bidirectional for no reason, needs a `ws` dependency, and has no standard resume.

**Fan-out design:**

| Option | Memory | Verdict |
|---|---|---|
| Per-client in-memory queue, push on event | O(slow clients × backlog), unbounded under a slow reader | Rejected (classic LLM leak) |
| **Event log in SQLite + per-client cursor (chosen)** | O(1) per client: a cursor plus at most one socket buffer | Chosen. The same model as Kafka consumer offsets. Replay and live delivery share one code path. |

Connection loop:
```
cursor = Last-Event-ID ?? 0
loop until closed:
  rows = SELECT … FROM events WHERE job_id=? AND seq > cursor ORDER BY seq LIMIT 100
  for row: if !res.write(frame(row)) await once(res,'drain') (30 s timeout → end; client resumes)
           cursor = row.seq
  if job completed and caught up → end
  if rows.length == 0 → await wake(jobId)   // notify() resolves; coalesces bursts
```
`res.flushHeaders()`, `X-Accel-Buffering: no`, `retry: 1000`, and a 15 s `: ping` comment that carries
**no `id:`** (an id on a heartbeat would overwrite the client's resume cursor). All listeners and timers
are removed on `close`.

**Event envelope (the "stream header"):**
```
id: 17
event: page
data: {"seq":17,"page":9,"stage":"final","watermark":4,"low_confidence":false,"result":{…}}
```
- The first event is `job`: `{seq:1, job_id, total_pages, window, backlog_pages, eta_s}` (§3.11).
- `page`: stage is `layout` (partial) or `final`.
- `job.completed` is last.
- `seq` is per-job and monotonic, assigned inside the commit transaction (`jobs.next_event_seq`), so
  `id:` is gap-free and resumable.
- `watermark` means every page below it is final, so the client can flush up to that point and free its
  buffer.
- `window` bounds how far ahead of `watermark` a final page can be (§3.4), so the client knows its maximum
  buffer up front.

**Client assembly:** documented in ARCHITECTURE.md and implemented by the bench as its verifier. Drop any
`seq ≤ lastSeq` (duplicates after reconnect), put final pages into `Map<page, result>`, and emit
contiguously from `next` while the map has `next`.

### 3.9 Evaluation engine (Module C): `POST /evaluate`

**Input schema** (the same tree `GET /jobs/:id/result` returns, so the modules connect end to end):
```ts
type Node = { type: string; text?: string; bbox?: [x, y, w, h]; children?: Node[] };
// body: { prediction: Node, ground_truth: Node }   root.type = "document", children of type "page"
```
**One iterative pass per tree** (explicit stack, no recursion, so deep trees cannot overflow the stack)
both validates the input and builds everything the metrics need: postorder label ids, leftmost-leaf
indices, per-page text and per-page boxes. Caps: 1 MB body, plus the TED step budget below.

#### Tree Edit Distance

| Algorithm | Time | Space | Verdict |
|---|---|---|---|
| **Zhang-Shasha (1989) (chosen)** | O(n·m·min(d₁,l₁)·min(d₂,l₂)); worst O(n²m²) | O(n·m) | Chosen. Document trees are **shallow** (document→page→block→line/cell, depth ≤ 5-6), so the min(depth, leaves) factors are small constants and the cost is effectively O(25·n·m). About 80 lines; the standard algorithm, easy to defend. |
| RTED | O(n³) worst | Up to 2× the memory of competitors | Rejected |
| APTED (Pawlik & Augsten 2016) | O(n³) worst, optimal path strategy | O(n·m) | Rejected. Its advantage only shows on degenerate (deep, zig-zag) shapes, which document trees aren't. It is 1,000+ lines (strategy computation, several single-path functions), too much to write and defend in 48 h. Named as the upgrade path. |
| Klein / Demaine | O(n² log n) / O(n³) | O(n·m) | Same reasoning as APTED |
| Top-down / constrained edit distance | O(n·m) | O(n·m) | Rejected. It cannot express moving a node between parents (re-nesting tables), which is exactly the drift we want to measure. |

**Implementation details:**
- 1-indexed postorder with `lml[]` (leftmost leaf) and `keyroots`.
- `treedist` is an `Int32Array(n·m)`; the forest distance is one `Int32Array((n+1)·(m+1))` **reused**
  across keyroot pairs, which is safe because each pair only reads cells it wrote.
- Labels are **interned to ints**, so the inner loop compares integers.
- Unit costs; rename cost is 0 when `type` matches.
- **The cap is on work, not node count.** A node cap bounds memory but not time: Zhang-Shasha's cost
  depends on tree *shape*. The parse pass already has everything needed to compute the exact
  inner-loop step count before running anything:
  `steps = W(A) · W(B)`, where `W(T) = Σ over keyroots k of |subtree(k)|`. This is an exact count,
  not an estimate (verified against measured counts in the prototype below).
  - Over `TED_MAX_STEPS` (5·10⁶, about 40 ms) → 413 `{error:"ted_too_large", steps, max_steps}`.
  - The same cap also bounds memory: `steps ≥ n·m` (the root pair alone costs n·m), so the two
    `Int32Array`s stay ≤ 5·10⁶ × 8 B = 40 MB.
  - **Every 50-node pair always passes:** steps ≤ n·m·min(d,l)² ≤ 50·50·25·25 ≈ 1.6·10⁶.
  - Evaluation runs synchronously, one at a time, so worst-case event-loop freeze is about 40 ms.
- **Prototype measurements** (throwaway JS, Node 18 on WSL2, p50; re-measure on Node 22 in P5):

| Trees (each side) | Steps | p50 | p99 | Under the cap? |
|---|---|---|---|---|
| 50-node document (depth 5) | 1.7·10⁴ | 0.11 ms | 0.24 ms | Yes, the graded case: ~900× under 100 ms |
| 50-node adversarial comb (depth 25) | 3.9·10⁵ | 1.9 ms | 2.5 ms | Yes, the worst 50-node shape: still ~40× under |
| 500-node document | 3.0·10⁶ | 21 ms | 23 ms | Yes |
| 2,000-node document | 6.1·10⁷ | 513 ms | 541 ms | No → 413 (the old node cap would have let this freeze the loop for 0.5 s) |
| 1,000-node adversarial comb | 6.2·10¹⁰ | 217 s | — | No → 413 (the old node cap would have let this hang the server) |

  Throughput is about 120k steps/ms (~8 ns per step), which is where the 40 ms figure comes from.
- **Output:** `{distance, normalized: distance/(n+m), nodes_pred, nodes_gt}`.
- **Golden test:** the classic Zhang-Shasha paper example, `f(d(a c(b)) e)` vs `f(c(d(a b)) e)`, has
  distance 2.

#### CER and WER

- **One generic `editDistance(a: Int32Array, b: Int32Array)`:** two-row DP, O(n·m) time and O(min(n,m))
  space, with the common prefix and suffix trimmed first.
  - **CER:** code points → `Int32Array`.
  - **WER:** split on whitespace and intern each word to an int id, then call **the same function**.
    One routine serves both metrics.
- **Normalization:** NFC, whitespace collapsed (standard practice in OCR evaluation).
- **Page-aligned:** computed per page (pages matched by index), then **micro-averaged**:
  `CER = Σ edits / Σ ref_chars`. This keeps every DP at page size (~2-5k chars, ~20 ms worst), where a
  full-document DP (200k × 200k) would be infeasible. A page missing from the prediction counts as all
  deletions.
- Rejected for now: Myers/Hyyrö bit-parallel (about 32× faster, used by edlib and fastest-levenshtein). It
  is the documented next step if the bench shows evaluation is hot; page-sized inputs don't need it.

#### IoU

- **Formula:** `iw = max(0, min(x1+w1, x2+w2) − max(x1, x2))`, same for `ih`; `IoU = iw·ih / (a1 + a2 − iw·ih)`,
  guarding against a zero union.
- **Matching per page:**

| Option | Verdict |
|---|---|
| Report all n×m pairs | Not a metric; it double-counts |
| Hungarian (optimal 1-to-1) | O(n³); rarely differs from greedy on real layouts. Rejected. |
| **Greedy by descending IoU at a threshold of 0.5 (chosen)** | The COCO/PASCAL VOC matching convention; O(nm log nm) with n,m ≈ 10-100 per page |

- **Output:** `{mean_iou (matched pairs), precision, recall, f1 @0.5, matched, pred, gt}`.
  Class-agnostic; `type` agreement is already measured by TED.

### 3.10 Observability

- **Logger:** about 15 lines writing JSON lines to stdout (pipes are synchronous on Linux, which is fine
  at ~100 lines/s). It merges in `AsyncLocalStorage` context: `trace_id, job_id, page, lane, attempt`.
  Each task runs inside `als.run(ctx, …)`, so every log line under it, including in the fetch and DB
  layers, is correlated without passing context through arguments.
- **Trace propagation across services:** each job gets a W3C trace id, **persisted in `jobs`**, so the
  trace survives a crash. Every model call sends `traceparent: 00-<trace>-<span>-01`, and the mock's
  existing `LOG_REQUESTS=1` access log adds a `traceparent` field (a one-line change).
- **Metrics:** `prom-client` at `GET /metrics`:

| Metric | Type | Meaning |
|---|---|---|
| `queue_depth{lane}` | gauge, computed on scrape with `COUNT` | pages ready to dispatch |
| `worker_lag_seconds{lane}` | gauge, computed on scrape | age of the oldest ready page (now − min ready_at) |
| `inflight{lane}` | gauge | requests in flight |
| `dispatch_rate{lane}` | gauge | current AIMD rate |
| `breaker_state{lane}` | gauge | 0 closed, 1 half-open, 2 open |
| `downstream_responses_total{lane,code}` | counter | responses by status code |
| `rate_limited_retries_total{lane}` | counter | 429/503 requeues |
| `retries_total{lane}` | counter | retries after real failures |
| `fallbacks_total` | counter | pages finalized from layout |
| `recovered_pages_total` | counter | pages reset to ready on boot |
| `sse_clients` | gauge | open SSE connections |
| `container_memory_peak_bytes` | gauge, read from `/sys/fs/cgroup/memory.peak` when present | container peak memory, including qpdf children |

  The default metrics add RSS, heap and event-loop lag.

### 3.11 Front-door backpressure: telling clients to slow down

Total throughput is capped by the VLM lane (~10 pages/s), so sustained arrivals above that grow the queue
without bound. Memory stays flat because the queue is on disk; the cost is wait time. Two signals tell
clients about it.

**1. Admission with a computed `Retry-After`.**
- `backlog` is the number of pages not yet `done` (one indexed `COUNT`).
- The check runs **before the body is read**, so a rejected client never uploads 100 MB. If
  `backlog ≥ MAX_QUEUED_PAGES` (default 6,000, about 10 min of VLM work), respond `503` + `Retry-After`.
- `Retry-After = clamp(ceil((backlog − MAX_QUEUED_PAGES + 100) / vlmRate × (1 + 0.2·random)), 1, 600)`
  seconds. That is the time for the backlog to drain enough to fit a max-size (100-page) job at the
  *current* adaptive VLM rate.
  - It uses the live AIMD rate, so during a 429 spike the estimate grows by itself.
  - The jitter stops every rejected client from coming back in the same second (thundering herd).
- **503, not 429:** the whole service is full. 429 means a per-client quota, which this plan does not have.
- **The cap is soft.** Page count is unknown until the upload finishes, so concurrent uploads can overshoot
  it by at most 100 pages each.

**2. Backlog and ETA at admission.** The `202` body and the SSE `job` header both carry `backlog_pages`
(pages not done in the system, this job's included) and `eta_s = ceil(backlog_pages / vlmRate)`.
- **This is not a "queue position".** Scheduling is round-robin by page index, so a new job doesn't wait
  behind earlier jobs: its page 1 runs next, and it shares capacity with everyone.
- **What `eta_s` bounds:** this job needs k rounds, and each round finishes at most one page per active
  job, so the job is done before the current backlog drains. `eta_s` is therefore an upper bound *if no new
  jobs arrive*. New arrivals share capacity and can stretch it; small jobs usually finish much sooner.
- **It is a snapshot taken at admission.** The `job` header is replayed on reconnect, so the value is not
  live. Ongoing progress shows through `watermark` on page events. No periodic ETA events are added.

---

## 4. Data model

```sql
CREATE TABLE jobs (
  id TEXT PRIMARY KEY, seq INTEGER NOT NULL UNIQUE, idem_key TEXT UNIQUE, trace_id TEXT NOT NULL,
  pdf_path TEXT NOT NULL, total_pages INTEGER NOT NULL, window INTEGER NOT NULL,
  watermark INTEGER NOT NULL DEFAULT 1, next_event_seq INTEGER NOT NULL DEFAULT 1,
  state TEXT NOT NULL CHECK (state IN ('running','done')), created_at INTEGER NOT NULL);

CREATE TABLE pages (
  job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE, page INTEGER NOT NULL,
  state TEXT NOT NULL,        -- layout_ready|layout_running|vlm_ready|vlm_running|done
  attempts INTEGER NOT NULL DEFAULT 0,  -- per stage, reset on layout→vlm
  not_before INTEGER NOT NULL DEFAULT 0, ready_at INTEGER NOT NULL,
  call_key TEXT, layout_json TEXT,
  PRIMARY KEY (job_id, page)) WITHOUT ROWID;
CREATE INDEX pages_claim ON pages (state, page, not_before);

CREATE TABLE events (
  job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE, seq INTEGER NOT NULL,
  type TEXT NOT NULL, page INTEGER, data TEXT NOT NULL,
  PRIMARY KEY (job_id, seq)) WITHOUT ROWID;
```

The **claim** is a single statement, so there is no read-then-write race:
```sql
UPDATE pages SET state = :running, call_key = COALESCE(call_key, :key)
WHERE (job_id, page) = (
  SELECT p.job_id, p.page FROM pages p JOIN jobs j ON j.id = p.job_id
  WHERE p.state = :ready AND p.not_before <= :now AND p.page < j.watermark + j.window
  ORDER BY p.page, j.seq LIMIT 1)
RETURNING *;
```

**Page state machine:**
```
layout_ready ──claim──▶ layout_running ──2xx──▶ vlm_ready ──claim──▶ vlm_running ──2xx──▶ done
     ▲   429/5xx (not_before) │  exhausted ─────────▲                 │ 429/5xx ─▶ vlm_ready
     └────────────────────────┘                                      │ exhausted / degraded ─▶ done(low_confidence)
boot recovery: *_running ─▶ *_ready (call_key kept)
```

---

## 5. API surface (complete; nothing else)

| Method | Path | Purpose |
|---|---|---|
| POST | `/jobs` | Raw `application/pdf` body, optional `Idempotency-Key` → `202 {job_id, events_url, backlog_pages, eta_s}`. Errors: 413 (> 100 MB), 422 (not a PDF or > 100 pages), 503 + computed, jittered `Retry-After` when `backlog ≥ MAX_QUEUED_PAGES`, checked before the body is read (§3.11) |
| GET | `/jobs/:id/events` | SSE stream (§3.8) |
| GET | `/jobs/:id/result` | The assembled document tree in the §3.9 schema; 409 while still running |
| POST | `/evaluate` | `{prediction, ground_truth}` → `{cer, wer, iou, ted, timing_ms}` |
| GET | `/metrics` | Prometheus text |
| GET | `/healthz` | Liveness, used by the compose healthcheck |

---

## 6. Memory budget (target well under 500 MB)

These are estimates to check against the bench, not measurements.

| Consumer | Bound | Est. |
|---|---|---|
| Node + Express + better-sqlite3 baseline | fixed | ~60 MB |
| SQLite page cache | `cache_size=-16000` | 16 MB |
| Uploads (50 concurrent) | 64 KB pipeline high-water mark each | ~6 MB |
| In-flight model calls | 16 + 32 × ~10 KB | < 1 MB |
| SSE clients | cursor plus ≤ 1 drained socket buffer | ~3 MB |
| qpdf children (counted in cgroup, not Node RSS) | `SPLIT_CONCURRENCY=4` | ≤ ~100 MB |
| `/evaluate` transient | TED step budget 5·10⁶ (bounds n·m), one at a time (sync) | ≤ 40 MB |
| Safety | `--max-old-space-size=256`, compose `mem_limit: 512m` | — |

---

## 7. Repository layout and size budget

```
packages/
  mock-models/                 existing; + Idempotency-Key replay (~25 lines), + traceparent in access log
  orchestrator/
    src/
      server.ts                boot: db open → recover → lanes start → listen; SIGTERM drain   (~50)
      app.ts                   Express routes, error handler                                   (~120)
      config.ts                env → typed config, spec defaults                               (~40)
      db.ts                    schema, pragmas, prepared statements, claim/complete/recover tx (~180)
      ingest.ts                stream-to-disk w/ cap, qpdf count + per-page extract            (~90)
      lane.ts                  dispatcher loop, in-flight bound, wake/notify                   (~110)
      token-bucket.ts          AdaptiveTokenBucket (lazy refill + AIMD + pause)                (~60)
      breaker.ts               CircuitBreaker                                                  (~40)
      pipeline.ts              layout/vlm handlers, outcome classification, backoff, fallback  (~120)
      model-client.ts          fetch, timeout, Idempotency-Key, traceparent, body drain        (~50)
      stream.ts                SSE cursor loop                                                 (~80)
      log.ts                   JSON logger + AsyncLocalStorage                                 (~25)
      metrics.ts               prom-client registry                                            (~50)
      eval/{tree,ted,edit-distance,iou,evaluate}.ts                                            (~250)
    test/                      node:test, tsx (same as mock)
    Dockerfile                 node:22-alpine + qpdf
  bench/
    load.ts                    50 jobs / 1,000 pages, SSE verifier, metrics table; --noise-rps
    ted.ts                     TED latency p50/p99
docker-compose.yml             mock-models, orchestrator (+ /data volume), bench (profile)
ARCHITECTURE.md                2–3 pages, condensed from this plan
```

---

## 8. Testing strategy (minimal but sufficient)

| Test | What it proves |
|---|---|
| `token-bucket.test.ts` (injected clock, as in the mock) | Pacing, pause, AIMD decrease at most once per window, additive recovery |
| `breaker.test.ts` | closed → open → half-open → closed/open |
| `ted.test.ts` | Zhang-Shasha golden example = 2; identical = 0; empty vs n = n; symmetry |
| `edit-distance.test.ts`, `iou.test.ts` | Known values, CER/WER through one function, greedy matching, zero-area guard |
| `db.test.ts` (`:memory:`) | Claim order and window; stale completion ignored (`changes = 0`); recovery keeps `call_key`; one event per transition |
| `crash.test.ts` (integration) | Spawns the mock and the orchestrator as child processes, submits 5 jobs, **SIGKILLs** mid-run, restarts, and asserts: every page has exactly one `final` event; no `final` was emitted twice; mock `vlm.ok − vlm.replayed == pages finalized via VLM` (no duplicate model execution) |
| `stream.test.ts` | Reconnecting with `Last-Event-ID` gets no gaps and no duplicates; slow reader keeps O(1) server memory (drain path) |
| `bench/load.ts --noise-rps 8` | Zero drop under real 429 spikes; prints 429 count and rate adaptation |

---

## 9. Implementation order (confirm with the user before each phase)

| # | Phase | Done when |
|---|---|---|
| P0 | Mock: Idempotency-Key replay + traceparent log | Mock tests pass, plus a replay test |
| P1 | `db.ts` + state machine + recovery (+ `db.test.ts`) | Claim, complete and recover are proven in-memory |
| P2 | `ingest.ts` + `POST /jobs` (qpdf) | A 100 MB PDF upload keeps heap flat; 413/422 paths |
| P3 | `token-bucket`, `breaker`, `model-client`, `pipeline`, `lane` | 1 job flows to done against the mock; metrics move |
| P4 | `stream.ts` + `/events` + `/result` | Out-of-order pages assemble; resume works |
| P5 | `eval/*` + `POST /evaluate` | Golden tests; `bench/ted.ts` p99 ≪ 100 ms |
| P6 | `log.ts`, `metrics.ts`, trace propagation | One trace id visible in both services' logs |
| P7 | Dockerfile, compose, `bench/load.ts`, `crash.test.ts` | `docker compose up` clean; bench meets all four targets |
| P8 | ARCHITECTURE.md + README | ≤ 3 pages, numbers taken from the bench output |

---

## 10. Pitfalls in naive (LLM-generated) code, and how this plan avoids each one

1. **Unbounded fan-out:** `Promise.all(pages.map(process))` with a semaphore. → Pull-based lanes (§3.4).
2. **Treating 429 as a failure:** it burns retries and drops jobs. → 429 is not an attempt (§3.6).
3. **Retrying 429 immediately, ignoring `Retry-After`:** a retry storm. → Pause plus AIMD (§3.5).
4. **Retries outside the limiter:** amplification. → Retries pay tokens (§3.5).
5. **Unconsumed `fetch` bodies on errors:** keep-alive sockets leak. → Always `await res.json()` or
   `res.body?.cancel()`.
6. **A refill `setInterval` per bucket, or `Promise.race` with uncancelled sleeps:** timer leaks. → Lazy
   refill; sleeps cancelled with `AbortController`.
7. **Per-client SSE queues, ignoring `res.write()`'s return value, listeners never removed:** memory leaks
   and MaxListeners warnings. → Cursor, `drain`, cleanup on `close` (§3.8).
8. **`express.raw()` for uploads:** 100 MB in the heap. → `stream.pipeline` to disk (§3.3).
9. **Recursive tree walks:** stack overflow on deep input. → Explicit stacks (§3.9).
10. **Result persisted and event emitted outside one transaction:** duplicate or lost events after a crash.
    → One transaction (§3.7).
11. **Semaphore permits released without `finally`:** permit leak under exceptions. → Every release is in
    `finally`.
12. **Unbounded CPU work on the event loop:** a giant `/evaluate` freezes SSE. → An exact TED step
    budget (computed before running, about 40 ms worst case) plus a body cap; a worker thread is the
    documented upgrade path.

---

## 11. Deep-dive questions to expect, with short answers

- **"Is it exactly-once?"** Exactly-once *effects* come from at-least-once delivery plus idempotency keys
  plus transactional state transitions. The model runs each (page, stage, attempt) at most once, and
  `crash.test.ts` proves it from the mock's counters.
- **"Why AIMD and not Netflix's gradient?"** The 429 is an explicit congestion signal, and VLM latency
  varies 2× by design, which makes delay-based control noisy. AIMD converges to fair shares with other
  clients of the same quota.
- **"Why not BullMQ?"** Its limiter is static, its stalled-job recovery re-runs work, and it would still
  need a second store for results and the event log. One SQLite transaction gives us state, result and
  event atomically.
- **"What happens with 10 orchestrators?"** Postgres with SKIP LOCKED and leases, a shared GCRA in Redis,
  and LISTEN/NOTIFY for fan-out (§3.1).
- **"TED complexity?"** O(n·m·min(d,l)²). Document trees are shallow, so it is near-quadratic; APTED only
  wins on degenerate shapes.
- **"Why is TTFP over 200 ms for the 50th job's first page?"** 50 page-ones at 100 RPS take 500 ms. The
  first SSE chunk is still under 200 ms; changing the page result would mean breaking the spec's own rate
  limit.
- **"What if the queue runs very long?"** Memory doesn't change, because the queue is on disk; wait time
  does. Admission caps the backlog at about 10 minutes of VLM work. A rejected client gets a jittered
  `Retry-After` computed from the live drain rate. An admitted client gets the backlog and an ETA up front
  (§3.11).

---

## Sources

- Netflix concurrency-limits (adaptive limits, AIMD vs gradient): https://github.com/Netflix/concurrency-limits ·
  https://netflixtechblog.medium.com/performance-under-load-3e6fa9a60581
- AIMD treating 429 as a backpressure signal (Vector's controller): https://docs.rs/crate/rate_limiter_aimd/0.1.1/source/README.md
- Exponential backoff and full jitter, AWS Architecture Blog: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
- IETF Idempotency-Key header draft: https://datatracker.ietf.org/doc/draft-ietf-httpapi-idempotency-key-header/
- SQLite WAL durability (application crash vs power loss): https://www.sqlite.org/wal.html · https://avi.im/blag/2025/sqlite-fsync/
- SQLite lease-queue patterns: https://github.com/kyan9400/leasequeue · https://bunqueue.dev/architecture/persistence/
- `node:sqlite` stability status: https://gh.hhj520.top/nodejs/node/blob/main/doc/api/sqlite.md
- PDFs can't be split as a forward stream (xref at EOF): https://dev.to/iurii_rogulia/large-pdfs-in-nodejs-without-unbounded-buffering-36ld ·
  https://www.github.com/galkahana/HummusJS/issues/121 · https://issues.apache.org/jira/browse/PDFBOX-4215
- qpdf page ranges and chunking: https://imagetotable.ai/blog/ocr-large-pdf-thousands-of-pages
- Tree edit distance (RTED/APTED, Zhang-Shasha bounds): https://tree-edit-distance.dbresearch.uni-salzburg.at
- Resumable SSE with Last-Event-ID; no ids on heartbeats: https://dev.to/mwmdgmb/your-sse-connection-dropped-now-what-building-resumable-server-sent-events-in-javascript-3g95

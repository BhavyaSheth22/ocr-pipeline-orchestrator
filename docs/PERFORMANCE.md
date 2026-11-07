# Performance notes: TTFP under a 50-upload burst

How time-to-first-progress (TTFP: `POST /jobs` start → first SSE chunk, 50 uploads at once, target
< 200 ms) was brought under target, with the evidence for each step. Summary and design are in
[ARCHITECTURE.md](../ARCHITECTURE.md).

## How it was measured

- `docker compose` on Docker Desktop/WSL2 (20 CPUs), the way the stack is graded, cold start every run.
- Docker Desktop's speed drifts across a day: the same image measured TTFP p99 152–187 ms one morning and
  300–337 ms that afternoon. Builds are therefore compared **interleaved in one session** (base, branch,
  base, branch …), and only numbers from one session are compared with each other.
- Two drivers: `bench/load.ts` (the full 1,000-page run) and a TTFP-only burst with the same client code up
  to the first event. CPU profiles: `node --cpu-prof` on every orchestrator process; all processes in a
  container share the monotonic clock, so their profiles line up on one timeline.

## The changes, in order

| Commit | Change | Why (evidence) | Effect (same-session comparison) |
|---|---|---|---|
| `9505e7d` | qpdf from 2 worker threads; boot warms `fetch` and qpdf | Profile: main thread busy 902 of the first 1,000 ms, 231 ms of it qpdf spawns (~3 ms synchronous each) | TTFP p50 506 → 368 ms; burst main-thread time 902 → 677 ms. More threads made it worse (p50 303 ms): forks compete for CPU |
| `26bf909` | Page count in-process, qpdf as fallback | All 50 page counts came back in one clump ~150 ms into the burst: each thread forked ~25 qpdf processes back to back | Largest single cut; see the next row for the combined effect |
| `fda204b` | Upload written straight to `jobs/<id>.pdf` | The rename went through libuv's 4-thread pool, shared with writes and extraction | One fewer pool round trip per upload |
| `33ff964` | Debian (glibc) instead of Alpine | Creating a 20-page job: 1.17 ms on musl, 0.46 ms on glibc, once per upload on the event loop | With the two above: p50 370–418 → 158–270 ms; p99 392–445 → 305–473 ms (tail unchanged) |
| `1218b34` | 32 `SO_REUSEPORT` listeners; bodies ≤ 256 KiB buffered, one sync write, counted from memory; indexed backlog count | Profile: ~1.9 ms of main-thread work per upload, spread thin; libuv accepts one connection per socket per loop iteration, so late connections waited | p99 162–232 vs 202–300 ms (main); p50 unchanged |
| `69ed9d3` | Cluster mode: HTTP worker processes in front of one pipeline process | Per-upload work is serial on one thread; pipeline work took about a third of busy time in the burst | Burst p99 112–145 ms vs 166–186 (previous row) and 170–198 (main) |
| `39e566a` | 2 HTTP workers by default instead of 4 | Each worker is ~85 MB of RSS; 4 workers peaked at 468 MB summed against the 500 MB target | p99 118–125 ms (4 workers 94–121, main 142–144); summed RSS 302 MB, container 158 MB |

Tried and dropped: a boot-time warm-up of each HTTP worker (150 runs of the upload path against a private
app). It halved worker CPU in a burst (221 → 123 ms summed; GC 22 → 3 ms) but did not move TTFP, because
the bench client is the bottleneck (below), and memory was the same without it.

## What limits TTFP now

- **The bench client.** Profiled, its single event loop is fully busy for the first ~150 ms of a burst
  (undici 83 ms, opening connections 37 ms): ~1.5 ms per upload plus stream. The HTTP workers sat idle
  20–50 ms into the burst waiting for requests.
- **Pauses outside our code.** Every burst starts with a 10–45 ms window in which the profiled processes
  are not scheduled at all (the profiler thread stops too). It happens with the client on localhost and
  is not explained.

## First page event: where the time goes

Each job's path to its first `page` event was traced with timestamps at every hand-off (client, HTTP
worker, pipeline process), current build, 2 clean bursts (100 jobs). That event is the partial
`stage: "layout"` page event, sent when the layout result commits; page 1's VLM call starts after it.

| Stage | p50 | p90 | max |
|---|---|---|---|
| Upload: client send → server handler | 57 ms | 80 ms | 93 ms |
| Ingest: write, page count, job commit | 0.8 ms | 2.0 ms | 20 ms |
| **Layout queue: wait for a dispatch slot** | **256 ms** | **451 ms** | **499 ms** |
| Extract page 1 (qpdf) | 6.3 ms | 8.3 ms | 12 ms |
| Layout model call (mock, 50 ms) | 53 ms | 57 ms | 70 ms |
| Commit, notify, SSE delivery | ~1.5 ms | | |
| **First page event** | **375 ms** | | **642 ms** |

The layout lane sends one request every ~11 ms (100 RPS, burst 1), page 1 of every job first, so job *k*
waits ~*k* × 11 ms; the 50th leaves ~600 ms into the burst. In the same window the pipeline process was
idle 63% of the time: CPU is not the limit. Options, none implemented: a short burst allowance in the
layout bucket (the mock admits 20 at once; ~200 ms earlier for the 50th job), tighter spacing (~50 ms),
and not treating local stalls as congestion. In one of four traced bursts the pipeline dispatched nothing
for 553 ms; the rate controller read that as downstream congestion and cut the layout rate 30% for the rest
of the burst (first-page p99 1.3 s). The stall's cause is not identified.

## Page counting: custom reader vs pdfjs-dist

Microbenchmark in Node 22 on the bench's 20-page PDF and qpdf-rewritten versions (object streams,
linearized); both return the same count on all three.

| | `page-count.ts` | pdfjs-dist 4.10.38 |
|---|---|---|
| First call in a fresh process | 1 ms | 174 ms (module load) |
| Per document, p50 / p99 | 17–53 µs / 73–180 µs | 190–223 µs / 2.0–2.5 ms |
| 50 documents at once (event-loop busy) | 0.9 ms | 9.1 ms |

pdf.js would be viable (~4–5 ms per worker per burst with a boot-time load) and is maintained; the custom
reader is ~10× cheaper and is 430 lines to own. qpdf stays either way for extraction, run as a separate
process so a crash in its C++ parser cannot take the orchestrator down.

## HTTP worker count

Same session, full bench, 2 cold runs each:

| | main (1 process) | 4 workers | 2 workers (default) |
|---|---|---|---|
| TTFP p99 | 142–144 ms | 94–121 ms | 118–125 ms |
| RSS summed over processes | 127–128 MB | 468–469 MB | 302 MB |
| Container peak (cgroup) | 92–93 MB | 218–219 MB | 158 MB |

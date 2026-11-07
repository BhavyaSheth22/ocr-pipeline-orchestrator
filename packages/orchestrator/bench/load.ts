// Load benchmark and verifier for the four scored metrics.
// Submits JOBS PDFs at once, follows every job's SSE stream with the client assembler, samples /metrics
// every 200 ms, and prints one table. Exits non-zero if any job is missing pages (zero-drop check).
//
//   npm run bench:load -w @ocr/orchestrator -- --orch http://127.0.0.1:3000 --mock http://127.0.0.1:8080
//   options: --jobs 50 --pages 1000 --noise-rps 8 (spends the mock's VLM quota from outside → real 429 spikes)
//            --eval-pairs 1000 (POST /evaluate round trips after the jobs drain; 0 skips) --eval-nodes 50
//            --eval-during (also run sequential /evaluate calls while the jobs run; perturbs TTFP slightly)
import { randomUUID } from 'node:crypto';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { monitorEventLoopDelay } from 'node:perf_hooks';
import { parseArgs } from 'node:util';
import { makePdf } from './make-pdf.js';
import { Assembler, readSse } from './sse-client.js';
import { combTree, docTree, rng } from './trees.js';
import type { EvaluateResponse } from '../src/http/types.js';

const { values: args } = parseArgs({
  options: {
    orch: { type: 'string', default: process.env.ORCH_URL ?? 'http://127.0.0.1:3000' },
    mock: { type: 'string', default: process.env.MOCK_URL ?? 'http://127.0.0.1:8080' },
    jobs: { type: 'string', default: '50' },
    pages: { type: 'string', default: '1000' },
    'noise-rps': { type: 'string', default: '0' },
    'eval-pairs': { type: 'string', default: '1000' },
    'eval-nodes': { type: 'string', default: '50' },
    'eval-during': { type: 'boolean', default: false },
  },
});
// Unique per run: in Docker the bench is always PID 1, so a pid-based key replayed the previous run's jobs.
const RUN_ID = randomUUID();
const JOBS = Number(args.jobs);
const TOTAL_PAGES = Number(args.pages);
const NOISE_RPS = Number(args['noise-rps']);
const EVAL_PAIRS = Number(args['eval-pairs']);
const EVAL_NODES = Number(args['eval-nodes']);

interface JobStats {
  pages: number;
  t0: number;
  accepted?: number;
  firstEvent?: number;
  firstPageEvent?: number;
  firstFinal?: number;
  done?: number;
  asm: Assembler;
  finals: number;
  failed: number;
  lowConfidence: number;
  error?: string;
}

// p-th percentile of numbers (nearest rank).
function pct(values: number[], p: number): number {
  const s = [...values].sort((a, b) => a - b);
  return s.length ? s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))]! : NaN;
}
// Formats a ms value.
const fmt = (v: number) => (Number.isFinite(v) ? `${Math.round(v)} ms` : 'n/a');
// Formats bytes as MB.
const mb = (v: number) => (v > 0 ? `${(v / 1024 / 1024).toFixed(1)} MB` : 'n/a');

// Pulls one sample value out of Prometheus text.
function sample(text: string, name: string): number {
  const m = text.match(new RegExp(`^${name}(?:\\{[^}]*\\})? ([0-9.e+]+)$`, 'm'));
  return m ? Number(m[1]) : 0;
}

// Splits TOTAL_PAGES across JOBS as evenly as possible (e.g. 1000 / 50 = 20 each).
const pagesFor = (i: number) => Math.floor(TOTAL_PAGES / JOBS) + (i < TOTAL_PAGES % JOBS ? 1 : 0);

const dir = await mkdtemp(join(tmpdir(), 'bench-'));
const pdfs = new Map<number, Uint8Array<ArrayBuffer>>();
for (let i = 0; i < JOBS; i++) {
  const n = pagesFor(i);
  if (!pdfs.has(n)) {
    await makePdf(join(dir, `${n}.pdf`), n);
    pdfs.set(n, new Uint8Array(await readFile(join(dir, `${n}.pdf`))));
  }
}

// Memory sampler: max RSS and container peak seen on /metrics.
let peakRss = 0;
let peakCgroup = 0;
let peakLoopLag = 0;
let samplerOn = true;
// The bench's own event-loop delay, so a slow client is not mistaken for a slow server.
const clientLoop = monitorEventLoopDelay({ resolution: 10 });
clientLoop.enable();
// Samples /metrics every 200 ms until stopped.
const sampler = (async () => {
  while (samplerOn) {
    try {
      const text = await (await fetch(`${args.orch}/metrics`)).text();
      peakRss = Math.max(peakRss, sample(text, 'process_resident_memory_bytes'));
      peakCgroup = Math.max(peakCgroup, sample(text, 'container_memory_peak_bytes'));
      peakLoopLag = Math.max(peakLoopLag, sample(text, 'nodejs_eventloop_lag_max_seconds') * 1000);
    } catch {
      // orchestrator busy or restarting: skip this sample
    }
    await new Promise((r) => setTimeout(r, 200));
  }
})();

// Noisy neighbour: another client spending the same VLM quota, so 429 spikes are real, not injected.
let noiseOn = NOISE_RPS > 0;
let noiseSent = 0;
// Fires NOISE_RPS VLM requests per second at the mock, ignoring the results.
const noise = (async () => {
  while (noiseOn) {
    noiseSent++;
    void fetch(`${args.mock}/v1/predict/vlm`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ document_id: 'noise', page: 1 + (noiseSent % 1000) }),
    }).then((r) => r.body?.cancel(), () => {});
    await new Promise((r) => setTimeout(r, 1000 / NOISE_RPS));
  }
})();

// /evaluate client: sequential round trips against the deployed orchestrator (whichever process the kernel
// picks), recording the client round trip and the server-side timings. Pre-serialized bodies, so the
// measured time is the request, not JSON.stringify.
interface EvalStats {
  rtt: number[];
  ted: number[];
  total: number[];
  errors: number;
  firstError?: string;
}
const r = rng(42);
const evalBodies = Array.from({ length: Math.max(EVAL_PAIRS, 1) }, () => JSON.stringify({ prediction: docTree(EVAL_NODES, r), ground_truth: docTree(EVAL_NODES, r) }));
const combBody = JSON.stringify({ prediction: combTree(EVAL_NODES), ground_truth: combTree(EVAL_NODES) });
// Posts bodies in order (cycling) until `count` calls are made or `keepGoing` turns false.
async function runEval(bodies: string[], count: number, keepGoing: () => boolean = () => true): Promise<EvalStats> {
  const st: EvalStats = { rtt: [], ted: [], total: [], errors: 0 };
  for (let i = 0; i < count && keepGoing(); i++) {
    const t = performance.now();
    try {
      const res = await fetch(`${args.orch}/evaluate`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: bodies[i % bodies.length] });
      const out = (await res.json()) as EvaluateResponse;
      if (res.status !== 200 || typeof out.ted?.distance !== 'number' || typeof out.cer?.value !== 'number') throw new Error(`POST /evaluate ${res.status} ${JSON.stringify(out).slice(0, 200)}`);
      st.rtt.push(performance.now() - t);
      st.ted.push(out.timing_ms.ted);
      st.total.push(out.timing_ms.total);
    } catch (err) {
      st.errors++;
      st.firstError ??= String(err);
    }
  }
  return st;
}
// p50 / p99 / max of a sample, formatted to 0.01 ms (evaluate times are well under 1 ms server-side).
const tri = (v: number[]) => (v.length ? [pct(v, 50), pct(v, 99), Math.max(...v)].map((x) => `${x.toFixed(2)} ms`).join(' / ') : 'n/a');

// Optional: /evaluate traffic while the jobs run, to show it stays fast while uploads and SSE are busy.
let evalDuringOn = args['eval-during'];
const evalDuring = evalDuringOn ? runEval(evalBodies, Infinity, () => evalDuringOn) : null;

const before = await fetch(`${args.mock}/metrics-lite`).then((r) => r.json() as Promise<Record<string, Record<string, number>>>, () => null);

// Uploads one job and follows its stream to job.completed, recording the timing points.
async function runJob(i: number): Promise<JobStats> {
  const pages = pagesFor(i);
  const st: JobStats = { pages, t0: performance.now(), asm: new Assembler(), finals: 0, failed: 0, lowConfidence: 0 };
  try {
    let res: Response;
    // Admission may answer 503 + Retry-After when the backlog is full; honour it and retry.
    for (;;) {
      res = await fetch(`${args.orch}/jobs`, { method: 'POST', headers: { 'content-type': 'application/pdf', 'idempotency-key': `bench-${RUN_ID}-${i}` }, body: pdfs.get(pages)! });
      if (res.status !== 503) break;
      await new Promise((r) => setTimeout(r, Number(res.headers.get('retry-after') ?? 1) * 1000));
    }
    if (res.status !== 202) throw new Error(`POST /jobs ${res.status}`);
    const { job_id } = (await res.json()) as { job_id: string };
    st.accepted = performance.now();
    for await (const ev of readSse(`${args.orch}/jobs/${job_id}/events`)) {
      const now = performance.now();
      st.firstEvent ??= now;
      if (ev.event === 'page') {
        st.firstPageEvent ??= now;
        const d = JSON.parse(ev.data) as { stage: string; status: string; low_confidence: boolean };
        if (d.stage === 'final') {
          st.firstFinal ??= now;
          st.finals++;
          if (d.status === 'failed') st.failed++;
          if (d.low_confidence) st.lowConfidence++;
        }
      }
      st.asm.push(ev);
    }
    st.done = performance.now();
  } catch (err) {
    st.error = String(err);
  }
  return st;
}

const start = performance.now();
const results = await Promise.all(Array.from({ length: JOBS }, (_, i) => runJob(i)));
const wall = performance.now() - start;
noiseOn = false;
evalDuringOn = false;
const evalUnderLoad = await evalDuring;
// /evaluate on the idle stack, after the jobs have drained (the sampler keeps watching memory and loop lag).
let evalIdle: EvalStats | null = null;
let evalComb: EvalStats | null = null;
if (EVAL_PAIRS > 0) {
  await runEval(evalBodies, 50); // warm up each worker's JIT
  evalIdle = await runEval(evalBodies, EVAL_PAIRS);
  evalComb = await runEval([combBody], 200);
}
samplerOn = false;
await Promise.all([sampler, noise]);
const after = await fetch(`${args.mock}/metrics-lite`).then((r) => r.json() as Promise<Record<string, Record<string, number>>>, () => null);
await rm(dir, { recursive: true, force: true });

const complete = results.filter((r) => r.asm.done && r.asm.emitted.length === r.pages && r.finals === r.pages);
const dropped = results.length - complete.length;
// Per-job latency from upload start to the given timing point.
const rel = (k: 'accepted' | 'firstEvent' | 'firstPageEvent' | 'firstFinal') => results.filter((r) => r[k] !== undefined).map((r) => r[k]! - r.t0);
// Change in one mock counter over the run.
const delta = (lane: string, k: string) => (after && before ? (after[lane]![k] ?? 0) - (before[lane]![k] ?? 0) : NaN);

console.log(`\nLoad benchmark: ${JOBS} jobs, ${TOTAL_PAGES} pages, noise ${NOISE_RPS} rps → ${args.orch}\n`);
console.log('| Metric | Result | Target |');
console.log('|---|---|---|');
console.log(`| Peak RSS (orchestrator, summed over its processes; counts the Node binary's shared pages once per process) | ${mb(peakRss)} | < 500 MB |`);
console.log(`| Peak container memory (cgroup, incl. qpdf) | ${mb(peakCgroup)} | < 500 MB |`);
console.log(`| Max event-loop lag: orchestrator / bench client | ${fmt(peakLoopLag)} / ${fmt(clientLoop.max / 1e6)} | (diagnostic) |`);
console.log(`| Jobs completed with every page | ${complete.length} / ${JOBS} | ${JOBS} / ${JOBS} |`);
console.log(`| Pages failed / low-confidence fallback | ${results.reduce((a, r) => a + r.failed, 0)} / ${results.reduce((a, r) => a + r.lowConfidence, 0)} | handled, not dropped |`);
console.log(`| Upload accepted (202) p50 / p99 / max | ${fmt(pct(rel('accepted'), 50))} / ${fmt(pct(rel('accepted'), 99))} / ${fmt(Math.max(...rel('accepted')))} | (part of TTFP) |`);
const afterAccept = results.filter((r) => r.firstEvent !== undefined && r.accepted !== undefined).map((r) => r.firstEvent! - r.accepted!);
console.log(`| 202 → first SSE event p50 / p99 / max | ${fmt(pct(afterAccept, 50))} / ${fmt(pct(afterAccept, 99))} / ${fmt(Math.max(...afterAccept))} | (part of TTFP) |`);
console.log(`| TTFP: first SSE event p50 / p99 / max | ${fmt(pct(rel('firstEvent'), 50))} / ${fmt(pct(rel('firstEvent'), 99))} / ${fmt(Math.max(...rel('firstEvent')))} | < 200 ms |`);
console.log(`| First page event p50 / p99 / max | ${fmt(pct(rel('firstPageEvent'), 50))} / ${fmt(pct(rel('firstPageEvent'), 99))} / ${fmt(Math.max(...rel('firstPageEvent')))} | (reported) |`);
console.log(`| First final page p50 / p99 / max | ${fmt(pct(rel('firstFinal'), 50))} / ${fmt(pct(rel('firstFinal'), 99))} / ${fmt(Math.max(...rel('firstFinal')))} | (reported) |`);
console.log(`| Max client reorder buffer (pages) | ${Math.max(...results.map((r) => r.asm.maxBuffered))} | ≤ window (32) |`);
console.log(`| Wall time | ${(wall / 1000).toFixed(1)} s | |`);
console.log(`| Mock VLM: ok / 429 / 500 / replayed | ${delta('vlm', 'ok')} / ${delta('vlm', 'rateLimited')} / ${delta('vlm', 'failed')} / ${delta('vlm', 'replayed')} | |`);
console.log(`| Mock layout: ok / 429 / 500 | ${delta('layout', 'ok')} / ${delta('layout', 'rateLimited')} / ${delta('layout', 'failed')} | |`);
if (NOISE_RPS > 0) console.log(`| Noise requests sent | ${noiseSent} | |`);
if (evalIdle && evalComb) {
  console.log(`| /evaluate idle, ${evalIdle.rtt.length} random ${EVAL_NODES}-node pairs: HTTP round trip p50 / p99 / max | ${tri(evalIdle.rtt)} | (reported) |`);
  console.log(`| /evaluate idle: TED only, server-side p50 / p99 / max | ${tri(evalIdle.ted)} | < 100 ms |`);
  console.log(`| /evaluate idle: all metrics, server-side p50 / p99 / max | ${tri(evalIdle.total)} | (reported) |`);
  console.log(`| /evaluate worst ${EVAL_NODES}-node comb: TED server-side / HTTP round trip p99 | ${tri(evalComb.ted).split(' / ')[1]} / ${tri(evalComb.rtt).split(' / ')[1]} | < 100 ms |`);
}
if (evalUnderLoad) {
  console.log(`| /evaluate during the job run (${evalUnderLoad.rtt.length} calls): HTTP round trip p50 / p99 / max | ${tri(evalUnderLoad.rtt)} | (reported) |`);
  console.log(`| /evaluate during the job run: TED server-side p50 / p99 / max | ${tri(evalUnderLoad.ted)} | < 100 ms |`);
}
const evalRuns = [evalIdle, evalComb, evalUnderLoad].filter((x): x is EvalStats => x !== null);
const evalErrors = evalRuns.reduce((a, s) => a + s.errors, 0);
const evalSlow = evalRuns.some((s) => s.ted.some((v) => v >= 100));
for (const s of evalRuns.filter((x) => x.firstError)) console.log(`evaluate error: ${s.firstError}`);
for (const r of results.filter((x) => x.error)) console.log(`job error: ${r.error}`);
if (dropped > 0 || evalErrors > 0 || evalSlow) {
  if (dropped > 0) console.error(`\nFAIL: ${dropped} job(s) did not complete with every page`);
  if (evalErrors > 0) console.error(`\nFAIL: ${evalErrors} /evaluate call(s) failed`);
  if (evalSlow) console.error('\nFAIL: a /evaluate TED took ≥ 100 ms');
  process.exit(1);
}
console.log(`\nOK: every job completed with every page, assembled in order from the stream${evalRuns.length ? '; every /evaluate call answered 200 with TED < 100 ms' : ''}.`);

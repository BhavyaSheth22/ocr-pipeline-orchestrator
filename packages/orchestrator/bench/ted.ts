// TED latency benchmark: p50/p99 over 1,000 random 50-node document tree pairs, in-process and over HTTP,
// plus the worst-case comb shape. Run: npm run bench:ted -w @ocr/orchestrator
import { buildApp } from '../src/http/app.js';
import { loadConfig } from '../src/config.js';
import { openDb } from '../src/storage/db.js';
import { evaluate } from '../src/eval/evaluate.js';
import { createNotifier } from '../src/http/stream.js';
import { combTree, docTree, rng } from './trees.js';

const N = Number(process.env.TED_PAIRS ?? 1000);
const NODES = Number(process.env.TED_NODES ?? 50);

// p-th percentile of a sorted array.
const pct = (sorted: number[], p: number) => sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))]!;
// Formats milliseconds.
const ms = (v: number) => `${v.toFixed(3)} ms`;

const r = rng(42);
const pairs = Array.from({ length: N }, () => ({ prediction: docTree(NODES, r), ground_truth: docTree(NODES, r) }));
const max = loadConfig().tedMaxSteps;

// In-process: the TED step alone, as timed inside evaluate().
for (let i = 0; i < 50; i++) evaluate(pairs[i % N], max); // warm up the JIT
const ted: number[] = [];
const total: number[] = [];
for (const p of pairs) {
  const out = evaluate(p, max);
  ted.push(out.timing_ms.ted);
  total.push(out.timing_ms.total);
}
ted.sort((a, b) => a - b);
total.sort((a, b) => a - b);

// Worst 50-node shape.
const comb = { prediction: combTree(NODES), ground_truth: combTree(NODES) };
const combTimes = Array.from({ length: 200 }, () => evaluate(comb, max).timing_ms.ted).sort((a, b) => a - b);

// Over HTTP: the full POST /evaluate round trip on a local server.
const app = buildApp({ cfg: loadConfig(), db: openDb(':memory:'), scheduler: { wake() {}, vlmRate: () => 10 }, notifier: createNotifier() });
const server = app.listen(0);
await new Promise((res) => server.once('listening', res));
const url = `http://127.0.0.1:${(server.address() as { port: number }).port}/evaluate`;
const http: number[] = [];
for (const p of pairs) {
  const body = JSON.stringify(p);
  const t = performance.now();
  const res = await fetch(url, { method: 'POST', headers: { 'content-type': 'application/json' }, body });
  await res.json();
  http.push(performance.now() - t);
}
http.sort((a, b) => a - b);
server.close();

console.log(`TED benchmark: ${N} random ${NODES}-node document pairs (target < 100 ms)\n`);
console.log('| Measure | p50 | p99 | max |');
console.log('|---|---|---|---|');
console.log(`| TED only, in-process | ${ms(pct(ted, 50))} | ${ms(pct(ted, 99))} | ${ms(ted.at(-1)!)} |`);
console.log(`| All metrics, in-process | ${ms(pct(total, 50))} | ${ms(pct(total, 99))} | ${ms(total.at(-1)!)} |`);
console.log(`| POST /evaluate over HTTP | ${ms(pct(http, 50))} | ${ms(pct(http, 99))} | ${ms(http.at(-1)!)} |`);
console.log(`| TED only, worst ${NODES}-node comb | ${ms(pct(combTimes, 50))} | ${ms(pct(combTimes, 99))} | ${ms(combTimes.at(-1)!)} |`);

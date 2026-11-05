import { test } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { readFile } from 'node:fs/promises';
import { makePdf } from '../bench/make-pdf.js';
import { paths } from '../src/ingest.js';
import { startSystem, type System } from '../src/server.js';
import { cleanup, hasQpdf, startMock, testConfig, waitFor } from './helpers.js';

const skip = hasQpdf ? false : 'qpdf not available (set QPDF_BIN)';
process.env.LOG_LEVEL ??= 'silent';

// Fast mock settings so a test runs in a second or two.
// Failure tests use short breaker pauses and backoff so retries finish quickly.
const QUICK_RETRY = { BREAKER_OPEN_MS: '100', BACKOFF_BASE_MS: '10' };

const FAST = { LAYOUT_LATENCY_MS: '5', VLM_LATENCY_MIN_MS: '20', VLM_LATENCY_MAX_MS: '40', LAYOUT_FAILURE_RATE: '0', VLM_FAILURE_RATE: '0' };

// Boots mock + orchestrator, uploads a PDF of n pages, waits for job.completed, returns parsed events.
async function runJob(n: number, mockEnv: Record<string, string>, orchEnv: Record<string, string> = {}) {
  const mock = await startMock({ ...FAST, ...mockEnv });
  const cfg = testConfig({ MODEL_URL: mock.url, ...orchEnv });
  let sys: System | undefined;
  try {
    sys = await startSystem(cfg, 0);
    const file = join(cfg.dataDir, 'in.pdf');
    await makePdf(file, n);
    const res = await fetch(`http://127.0.0.1:${sys.port}/jobs`, { method: 'POST', body: await readFile(file) });
    assert.equal(res.status, 202);
    const { job_id } = (await res.json()) as { job_id: string };
    const db = sys.db;
    await waitFor(() => db.getJob(job_id)?.state === 'done', 30_000);
    const events = db.events(job_id, 0, 10_000).map((e) => ({ type: e.type, ...JSON.parse(e.data) }));
    return { events, job_id, cfg, mock: await mock.metrics(), sys };
  } finally {
    await sys?.stop();
    await mock.stop();
  }
}

// Every page has exactly one final event and job.completed is last.
function assertComplete(events: { type: string; stage?: string; page?: number }[], n: number): void {
  const finals = events.filter((e) => e.stage === 'final').map((e) => e.page).sort((a, b) => a! - b!);
  assert.deepEqual(finals, Array.from({ length: n }, (_, i) => i + 1));
  assert.equal(events.at(-1)!.type, 'job.completed');
  assert.deepEqual(events.map((e) => (e as unknown as { seq: number }).seq), events.map((_, i) => i + 1), 'gap-free seqs');
}

test('a job flows through layout and vlm to done, and its files are cleaned up', { skip, timeout: 30_000 }, async () => {
  const { events, job_id, cfg, mock } = await runJob(4, {});
  assertComplete(events, 4);
  const finals = events.filter((e) => e.stage === 'final');
  assert.ok(finals.every((e) => e.source === 'vlm' && e.low_confidence === false));
  assert.ok(finals[0].result.children.some((c: { type: string }) => c.type === 'text'), 'vlm text in the result');
  assert.equal(events.filter((e) => e.stage === 'layout').length, 4, 'one partial per page');
  assert.equal(mock.layout.ok, 4);
  assert.equal(mock.vlm.ok, 4);
  assert.equal(existsSync(paths.job(cfg, job_id)), false, 'source pdf deleted');
  assert.equal(existsSync(paths.pages(cfg, job_id)), false, 'page files deleted');
  cleanup(cfg);
});

test('VLM failing every time falls back to layout with low confidence (zero drop)', { skip, timeout: 30_000 }, async () => {
  const { events, cfg, mock } = await runJob(3, { VLM_FAILURE_RATE: '1' }, QUICK_RETRY);
  assertComplete(events, 3);
  const finals = events.filter((e) => e.stage === 'final');
  assert.ok(finals.every((e) => e.status === 'ok' && e.source === 'layout' && e.low_confidence === true));
  assert.equal(events.at(-1)!.low_confidence_pages, 3);
  assert.equal(mock.vlm.failed, 9, '3 pages × 3 attempts');
  cleanup(cfg);
});

test('429s are backpressure, not failures: a slow VLM quota still finishes every page', { skip, timeout: 30_000 }, async () => {
  // Mock allows 4 VLM rps (burst 1); the orchestrator starts at 10 and must adapt.
  const { events, cfg, mock } = await runJob(12, { VLM_RPS: '4', VLM_BURST: '1' });
  assertComplete(events, 12);
  assert.ok(events.filter((e) => e.stage === 'final').every((e) => e.source === 'vlm'), 'no page burned its retries on 429s');
  assert.ok(mock.vlm.rateLimited! > 0, 'the test actually produced 429s');
  assert.equal(mock.vlm.ok, 12);
  cleanup(cfg);
});

test('layout exhausted still runs VLM; both exhausted finishes the page as failed', { skip, timeout: 30_000 }, async () => {
  const a = await runJob(2, { LAYOUT_FAILURE_RATE: '1' }, QUICK_RETRY);
  assertComplete(a.events, 2);
  assert.ok(a.events.filter((e) => e.stage === 'final').every((e) => e.source === 'vlm'));
  cleanup(a.cfg);

  const b = await runJob(2, { LAYOUT_FAILURE_RATE: '1', VLM_FAILURE_RATE: '1' }, QUICK_RETRY);
  assertComplete(b.events, 2);
  assert.ok(b.events.filter((e) => e.stage === 'final').every((e) => e.status === 'failed'));
  assert.equal(b.events.at(-1)!.failed_pages, 2);
  cleanup(b.cfg);
});

test('a long VLM outage switches to degraded mode and finishes from layout', { skip, timeout: 30_000 }, async () => {
  // VLM hangs on every call; timeouts open the breaker and degraded mode takes over after 300 ms of outage.
  const { events, cfg } = await runJob(
    8,
    { VLM_HANG_RATE: '1' },
    { VLM_TIMEOUT_MS: '100', BREAKER_OPEN_MS: '200', DEGRADED_AFTER_MS: '300' },
  );
  assertComplete(events, 8);
  assert.ok(events.filter((e) => e.stage === 'final').every((e) => e.source === 'layout' && e.low_confidence));
  cleanup(cfg);
});

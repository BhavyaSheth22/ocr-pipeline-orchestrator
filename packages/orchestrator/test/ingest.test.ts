import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createReadStream, existsSync, readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { Readable } from 'node:stream';
import { buildApp } from '../src/http/app.js';
import { openDb } from '../src/storage/db.js';
import { createSplitter } from '../src/pdf/pdf.js';
import { ensureDirs, paths } from '../src/storage/files.js';
import { createNotifier } from '../src/http/stream.js';
import { makePdf } from '../bench/make-pdf.js';
import { cleanup, hasQpdf, testConfig, withServer } from './helpers.js';

const skip = hasQpdf ? false : 'qpdf not available (set QPDF_BIN)';

// Builds an app on a fresh data dir with a stub scheduler that counts wakes.
async function setup(env: Record<string, string> = {}) {
  const cfg = testConfig(env);
  await ensureDirs(cfg);
  const db = openDb(':memory:');
  const wakes = { n: 0 };
  const app = buildApp({ cfg, db, scheduler: { wake: () => void wakes.n++, vlmRate: () => 10 }, notifier: createNotifier() });
  return { cfg, db, app, wakes };
}

// POSTs a file as a streamed application/pdf body.
const upload = (url: string, file: string, headers: Record<string, string> = {}) =>
  fetch(`${url}/jobs`, {
    method: 'POST',
    headers: { 'content-type': 'application/pdf', ...headers },
    body: Readable.toWeb(createReadStream(file)) as ReadableStream,
    duplex: 'half',
  } as RequestInit);

test('a PDF upload creates a job with one row per page and wakes the scheduler', { skip }, async () => {
  const { cfg, db, app, wakes } = await setup();
  const file = join(cfg.dataDir, 'in.pdf');
  await makePdf(file, 3);
  await withServer(app, async (url) => {
    const res = await upload(url, file);
    assert.equal(res.status, 202);
    const body = (await res.json()) as { job_id: string; events_url: string; total_pages: number; backlog_pages: number; eta_s: number };
    assert.equal(body.total_pages, 3);
    assert.equal(body.backlog_pages, 3);
    assert.equal(body.eta_s, 1);
    assert.equal(body.events_url, `/jobs/${body.job_id}/events`);
    assert.ok(existsSync(paths.job(cfg, body.job_id)));
    assert.deepEqual(readdirSync(join(cfg.dataDir, 'jobs')), [`${body.job_id}.pdf`]);
    assert.equal(db.getJob(body.job_id)!.total_pages, 3);
    assert.equal(wakes.n, 1);
  });
  cleanup(cfg);
});

test('non-PDF bodies get 422 and leave no files behind', { skip }, async () => {
  const { cfg, app } = await setup();
  await withServer(app, async (url) => {
    const res = await fetch(`${url}/jobs`, { method: 'POST', headers: { 'content-type': 'application/pdf' }, body: 'hello' });
    assert.equal(res.status, 422);
    assert.equal(((await res.json()) as { error: string }).error, 'not_a_pdf');
    const empty = await fetch(`${url}/jobs`, { method: 'POST' });
    assert.equal(empty.status, 422);
  });
  assert.deepEqual(readdirSync(join(cfg.dataDir, 'jobs')), []);
  cleanup(cfg);
});

test('more than 100 pages gets 422 too_many_pages', { skip }, async () => {
  const { cfg, app } = await setup();
  const file = join(cfg.dataDir, 'big.pdf');
  await makePdf(file, 101);
  await withServer(app, async (url) => {
    const res = await upload(url, file);
    assert.equal(res.status, 422);
    assert.equal(((await res.json()) as { error: string }).error, 'too_many_pages');
  });
  cleanup(cfg);
});

test('declared Content-Length over the cap gets an early 413', async () => {
  const { cfg, app } = await setup({ MAX_UPLOAD_BYTES: '1000' });
  await withServer(app, async (url) => {
    const res = await fetch(`${url}/jobs`, { method: 'POST', body: 'x'.repeat(5000) });
    assert.equal(res.status, 413);
  });
  cleanup(cfg);
});

test('a chunked body that crosses the cap mid-stream gets 413 and leaves no file', async () => {
  const { cfg, app } = await setup({ MAX_UPLOAD_BYTES: '1000' });
  await withServer(app, async (url) => {
    const res = await fetch(`${url}/jobs`, {
      method: 'POST',
      body: Readable.toWeb(Readable.from([Buffer.alloc(600), Buffer.alloc(600)])) as ReadableStream,
      duplex: 'half',
    } as RequestInit);
    assert.equal(res.status, 413);
  });
  assert.deepEqual(readdirSync(join(cfg.dataDir, 'jobs')), []);
  cleanup(cfg);
});

test('an Idempotency-Key replay returns the original job without reading the body', { skip }, async () => {
  const { cfg, app } = await setup();
  const file = join(cfg.dataDir, 'in.pdf');
  await makePdf(file, 2);
  await withServer(app, async (url) => {
    const a = (await (await upload(url, file, { 'idempotency-key': 'u1' })).json()) as { job_id: string };
    const res = await fetch(`${url}/jobs`, { method: 'POST', headers: { 'idempotency-key': 'u1' }, body: 'not even a pdf' });
    assert.equal(res.status, 202);
    assert.equal(((await res.json()) as { job_id: string }).job_id, a.job_id);
  });
  cleanup(cfg);
});

test('a full backlog gets 503 with a Retry-After before the body is read', { skip }, async () => {
  const { cfg, app } = await setup({ MAX_QUEUED_PAGES: '2' });
  const file = join(cfg.dataDir, 'in.pdf');
  await makePdf(file, 2);
  await withServer(app, async (url) => {
    assert.equal((await upload(url, file)).status, 202);
    const res = await fetch(`${url}/jobs`, { method: 'POST', body: 'ignored' });
    assert.equal(res.status, 503);
    const ra = Number(res.headers.get('retry-after'));
    // (2 - 2 + 100) / 10 = 10 s, jittered up to +20%.
    assert.ok(ra >= 10 && ra <= 12, `retry-after ${ra}`);
  });
  cleanup(cfg);
});

test('a 100 MB upload keeps the heap flat', { skip, timeout: 60_000 }, async () => {
  const { cfg, app } = await setup();
  const file = join(cfg.dataDir, 'huge.pdf');
  await makePdf(file, 5, 100 * 1024 * 1024 - 10_000);
  assert.ok(statSync(file).size <= 100 * 1024 * 1024);
  global.gc?.();
  const before = process.memoryUsage();
  let peakHeap = before.heapUsed;
  // Samples heap usage during the upload.
  const timer = setInterval(() => (peakHeap = Math.max(peakHeap, process.memoryUsage().heapUsed)), 20);
  await withServer(app, async (url) => {
    const res = await upload(url, file);
    assert.equal(res.status, 202);
  });
  clearInterval(timer);
  const growthMb = (peakHeap - before.heapUsed) / 1024 / 1024;
  assert.ok(growthMb < 40, `heap grew ${growthMb.toFixed(1)} MB for a 100 MB upload`);
  cleanup(cfg);
});

test('extractPage writes one PDF per page under concurrent calls and is idempotent', { skip }, async () => {
  const cfg = testConfig();
  await ensureDirs(cfg);
  const src = join(cfg.dataDir, 'in.pdf');
  await makePdf(src, 6);
  const extract = createSplitter(cfg);
  const outs = await Promise.all([1, 2, 3, 4, 5, 6].map((n) => extract('j', src, n)));
  assert.equal(outs.length, 6);
  const again = await extract('j', src, 2);
  assert.equal(again, outs[1]);
  assert.deepEqual(readdirSync(paths.pages(cfg, 'j')).sort(), ['1.pdf', '2.pdf', '3.pdf', '4.pdf', '5.pdf', '6.pdf']);
  cleanup(cfg);
});

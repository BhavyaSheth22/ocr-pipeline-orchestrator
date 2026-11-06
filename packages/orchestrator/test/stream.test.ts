import { test } from 'node:test';
import assert from 'node:assert/strict';
import { join } from 'node:path';
import { readFile } from 'node:fs/promises';
import { Assembler, readSse } from '../bench/sse-client.js';
import { makePdf } from '../bench/make-pdf.js';
import { openDb } from '../src/storage/db.js';
import { startSystem } from '../src/server.js';
import { createNotifier } from '../src/http/stream.js';
import { cleanup, hasQpdf, startMock, testConfig } from './helpers.js';

const skip = hasQpdf ? false : 'qpdf not available (set QPDF_BIN)';
process.env.LOG_LEVEL ??= 'silent';

// Wide VLM latency and a high rate so final pages arrive out of order.
const SCRAMBLE = {
  LAYOUT_LATENCY_MS: '5', VLM_LATENCY_MIN_MS: '10', VLM_LATENCY_MAX_MS: '400',
  LAYOUT_FAILURE_RATE: '0', VLM_FAILURE_RATE: '0', VLM_RPS: '100',
};

// Boots mock + orchestrator and submits an n-page job; returns handles and the events URL.
async function boot(n: number) {
  const mock = await startMock(SCRAMBLE);
  const cfg = testConfig({ MODEL_URL: mock.url, VLM_RPS: '100' });
  const sys = await startSystem(cfg, 0);
  const base = `http://127.0.0.1:${sys.port}`;
  const file = join(cfg.dataDir, 'in.pdf');
  await makePdf(file, n);
  const { job_id } = (await (await fetch(`${base}/jobs`, { method: 'POST', body: await readFile(file) })).json()) as { job_id: string };
  // Stops everything and removes the data dir.
  const close = async () => {
    await sys.stop();
    await mock.stop();
    cleanup(cfg);
  };
  return { base, job_id, eventsUrl: `${base}/jobs/${job_id}/events`, close };
}

test('out-of-order final pages assemble in order on the client', { skip, timeout: 30_000 }, async () => {
  const { base, job_id, eventsUrl, close } = await boot(20);
  try {
    const asm = new Assembler();
    for await (const ev of readSse(eventsUrl)) asm.push(ev);
    assert.equal(asm.done, true);
    assert.deepEqual(asm.emitted, Array.from({ length: 20 }, (_, i) => i + 1));
    assert.notDeepEqual(asm.arrival, asm.emitted, 'finals really arrived out of order');
    assert.ok(asm.maxBuffered <= 32, 'buffer bounded by the window');

    const result = (await (await fetch(`${base}/jobs/${job_id}/result`)).json()) as { type: string; children: { page: number; type: string }[] };
    assert.equal(result.type, 'document');
    assert.deepEqual(result.children.map((c) => c.page), asm.emitted);
    assert.ok(result.children.every((c) => c.type === 'page'));
  } finally {
    await close();
  }
});

test('reconnecting with Last-Event-ID gives no gaps and no duplicates', { skip, timeout: 30_000 }, async () => {
  const { eventsUrl, close } = await boot(10);
  try {
    const asm = new Assembler();
    const ac = new AbortController();
    let seen = 0;
    try {
      for await (const ev of readSse(eventsUrl, undefined, ac.signal)) {
        asm.push(ev);
        if (++seen === 5) ac.abort(); // drop the connection mid-stream
      }
    } catch (err) {
      assert.equal((err as Error).name, 'AbortError');
    }
    // Resume from one event earlier than we really have, to prove duplicates are dropped client-side.
    for await (const ev of readSse(eventsUrl, asm.lastSeq - 1)) asm.push(ev); // Assembler throws on a gap
    assert.equal(asm.done, true);
    assert.deepEqual(asm.duplicates, [5]);
    assert.deepEqual(asm.emitted, Array.from({ length: 10 }, (_, i) => i + 1));
  } finally {
    await close();
  }
});

test('/result is 409 while running and 404 for unknown jobs; /events 404 for unknown jobs', { skip, timeout: 30_000 }, async () => {
  const { base, job_id, close } = await boot(30);
  try {
    assert.equal((await fetch(`${base}/jobs/${job_id}/result`)).status, 409);
    assert.equal((await fetch(`${base}/jobs/nope/result`)).status, 404);
    assert.equal((await fetch(`${base}/jobs/nope/events`)).status, 404);
  } finally {
    await close();
  }
});

test('notifier: one notify wakes every waiter; aborted waiters are removed', async () => {
  const n = createNotifier();
  const ac = new AbortController();
  const woke: string[] = [];
  const a = n.wait('j', new AbortController().signal).then(() => woke.push('a'));
  const b = n.wait('j', new AbortController().signal).then(() => woke.push('b'));
  const c = n.wait('j', ac.signal).then(() => woke.push('c'));
  ac.abort();
  await c;
  n.notify('j');
  await Promise.all([a, b]);
  assert.deepEqual(woke.sort(), ['a', 'b', 'c']);
  n.notify('j'); // nobody waiting: no-op
});

test('events after a job completes replay from the log and the stream ends', async () => {
  const db = openDb(':memory:');
  db.createJob({ id: 'j', idemKey: null, traceId: 't', pdfPath: '/x', totalPages: 1, window: 32, header: {} });
  db.claim('layout', 0);
  db.advanceToVlm('j', 1, null, null);
  db.claim('vlm', 0);
  db.finalize('vlm', 'j', 1, { status: 'ok', source: 'vlm', low_confidence: false, result: {} });
  const { buildApp } = await import('../src/http/app.js');
  const cfg = testConfig();
  const app = buildApp({ cfg, db, scheduler: { wake() {}, vlmRate: () => 10 }, notifier: createNotifier() });
  const { withServer } = await import('./helpers.js');
  await withServer(app, async (url) => {
    const types: string[] = [];
    const ids: (number | null)[] = [];
    for await (const ev of readSse(`${url}/jobs/j/events`)) {
      types.push(ev.event);
      ids.push(ev.id);
    }
    assert.deepEqual(types, ['job', 'page', 'job.completed']);
    assert.deepEqual(ids, [1, 2, 3]);
  });
  cleanup(cfg);
});

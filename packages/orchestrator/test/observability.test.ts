import { test } from 'node:test';
import assert from 'node:assert/strict';
import { join } from 'node:path';
import { readFile } from 'node:fs/promises';
import { makePdf } from '../bench/make-pdf.js';
import { buildApp } from '../src/http/app.js';
import { createNotifier } from '../src/http/stream.js';
import { startSystem } from '../src/server.js';
import { openDb } from '../src/storage/db.js';
import { cleanup, hasQpdf, startMock, testConfig, waitFor, withServer } from './helpers.js';

const skip = hasQpdf ? false : 'qpdf not available (set QPDF_BIN)';

test('one trace id from the job row appears in the mock access log; /metrics moves', { skip, timeout: 30_000 }, async () => {
  process.env.LOG_LEVEL = 'silent';
  const mock = await startMock({ LOG_REQUESTS: '1', LAYOUT_LATENCY_MS: '5', VLM_LATENCY_MIN_MS: '10', VLM_LATENCY_MAX_MS: '20', LAYOUT_FAILURE_RATE: '0', VLM_FAILURE_RATE: '0' });
  const cfg = testConfig({ MODEL_URL: mock.url });
  const sys = await startSystem(cfg, 0);
  try {
    const base = `http://127.0.0.1:${sys.port}`;
    const file = join(cfg.dataDir, 'in.pdf');
    await makePdf(file, 2);
    // The client's traceparent becomes the job's trace id, returned in the body and the X-Trace-Id header.
    const clientTrace = '4bf92f3577b34da6a3ce929d0e0e4736';
    const res = await fetch(`${base}/jobs`, { method: 'POST', body: await readFile(file), headers: { traceparent: `00-${clientTrace}-00f067aa0ba902b7-01` } });
    const { job_id, trace_id } = (await res.json()) as { job_id: string; trace_id: string };
    assert.equal(trace_id, clientTrace);
    assert.equal(res.headers.get('x-trace-id'), clientTrace);
    await waitFor(() => sys.db.getJob(job_id)?.state === 'done', 20_000);
    const traceId = sys.db.getJob(job_id)!.trace_id;
    assert.equal(traceId, clientTrace);
    type MockLine = { traceparent?: string; trace_id?: string; span_id?: string };
    const traced = await waitFor(() => {
      const hits = mock.lines.map((l) => JSON.parse(l) as MockLine).filter((e) => e.trace_id === traceId);
      return hits.length >= 4 ? hits : null;
    }, 5000);
    assert.equal(traced!.length, 4, '2 layout + 2 vlm calls carry the job trace id');
    assert.match(traced![0]!.traceparent!, /^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/);
    assert.equal(new Set(traced!.map((e) => e.span_id)).size, 4, 'each model call has its own span id');

    // Job-scoped routes answer under the job's trace.
    const result = await fetch(`${base}/jobs/${job_id}/result`);
    await result.body?.cancel();
    assert.equal(result.headers.get('x-trace-id'), clientTrace);

    const text = await (await fetch(`${base}/metrics`)).text();
    for (const name of ['queue_depth', 'worker_lag_seconds', 'inflight', 'dispatch_rate', 'breaker_state', 'sse_clients', 'process_resident_memory_bytes', 'nodejs_eventloop_lag_seconds']) {
      assert.ok(text.includes(`\n${name}`) || text.startsWith(name) || text.includes(`# HELP ${name}`), `missing ${name}`);
    }
    assert.match(text, /downstream_responses_total\{lane="vlm",code="200"\} 2/);
    assert.match(text, /dispatch_rate\{lane="vlm"\} 10/);
  } finally {
    await sys.stop();
    await mock.stop();
    cleanup(cfg);
  }
});

test('every response carries X-Trace-Id: the caller trace when valid, else a fresh one', async () => {
  const cfg = testConfig();
  const app = buildApp({ cfg, db: openDb(':memory:'), notifier: createNotifier(), scheduler: { wake() {}, vlmRate: () => 10 } });
  try {
    await withServer(app, async (url) => {
      const tp = '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01';
      assert.equal((await fetch(`${url}/healthz`, { headers: { traceparent: tp } })).headers.get('x-trace-id'), '0af7651916cd43dd8448eb211c80319c');
      for (const bad of [undefined, 'garbage', `00-${'0'.repeat(32)}-b7ad6b7169203331-01`]) {
        const id = (await fetch(`${url}/nope`, { headers: bad ? { traceparent: bad } : {} })).headers.get('x-trace-id');
        assert.match(id ?? '', /^[0-9a-f]{32}$/);
        assert.notEqual(id, '0'.repeat(32));
      }
    });
  } finally {
    cleanup(cfg);
  }
});

test('log lines carry the AsyncLocalStorage context through awaits', async () => {
  // Runs the logger in a child process so its stdout can be read without touching the test reporter's stdout.
  const { execFileSync } = await import('node:child_process');
  const { fileURLToPath } = await import('node:url');
  const logModule = fileURLToPath(new URL('../src/observability/log.ts', import.meta.url));
  const code = `
    const { log, withLogContext } = await import(${JSON.stringify(logModule)});
    await withLogContext({ trace_id: 'abc', job_id: 'j1' }, async () => {
      await new Promise((r) => setTimeout(r, 5));
      await withLogContext({ page: 3 }, async () => log.info('hello'));
    });`;
  const env = { ...process.env };
  delete env.LOG_LEVEL;
  const out = execFileSync(process.execPath, ['--import', 'tsx', '--input-type=module', '-e', code], { env, encoding: 'utf8' });
  const entry = JSON.parse(out.trim()) as Record<string, unknown>;
  assert.equal(entry.msg, 'hello');
  assert.equal(entry.trace_id, 'abc');
  assert.equal(entry.job_id, 'j1');
  assert.equal(entry.page, 3);
});

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { join } from 'node:path';
import { readFile } from 'node:fs/promises';
import { makePdf } from '../bench/make-pdf.js';
import { startSystem } from '../src/server.js';
import { cleanup, hasQpdf, startMock, testConfig, waitFor } from './helpers.js';

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
    const { job_id } = (await (await fetch(`${base}/jobs`, { method: 'POST', body: await readFile(file) })).json()) as { job_id: string };
    await waitFor(() => sys.db.getJob(job_id)?.state === 'done', 20_000);
    const traceId = sys.db.getJob(job_id)!.trace_id;
    const traced = await waitFor(() => {
      const hits = mock.lines.map((l) => JSON.parse(l) as { traceparent?: string }).filter((e) => e.traceparent?.split('-')[1] === traceId);
      return hits.length >= 4 ? hits : null;
    }, 5000);
    assert.equal(traced!.length, 4, '2 layout + 2 vlm calls carry the job trace id');
    assert.match(traced![0]!.traceparent!, /^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/);

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

test('log lines carry the AsyncLocalStorage context through awaits', async () => {
  // Runs the logger in a child process so its stdout can be read without touching the test reporter's stdout.
  const { execFileSync } = await import('node:child_process');
  const { fileURLToPath } = await import('node:url');
  const logModule = fileURLToPath(new URL('../src/log.ts', import.meta.url));
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

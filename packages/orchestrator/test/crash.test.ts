import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn, type ChildProcess } from 'node:child_process';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { makePdf } from '../bench/make-pdf.js';
import { cleanup, hasQpdf, QPDF, startMock, testConfig, waitFor } from './helpers.js';

const skip = hasQpdf ? false : 'qpdf not available (set QPDF_BIN)';
const SERVER = fileURLToPath(new URL('../src/server.ts', import.meta.url));

// Starts the orchestrator as a real child process (so it can be SIGKILLed) and waits for /healthz.
async function startOrchestrator(env: Record<string, string>): Promise<{ child: ChildProcess; url: string }> {
  const port = 40000 + Math.floor(Math.random() * 20000);
  const child = spawn(process.execPath, ['--import', 'tsx', SERVER], {
    env: { ...process.env, ...env, PORT: String(port), LOG_LEVEL: 'silent' },
    stdio: ['ignore', 'ignore', 'inherit'],
  });
  const url = `http://127.0.0.1:${port}`;
  await waitFor(() => fetch(`${url}/healthz`).then((r) => r.ok, () => false), 15_000, 50);
  return { child, url };
}

// SIGKILLs a child and waits for it to exit.
async function kill(child: ChildProcess): Promise<void> {
  child.kill('SIGKILL');
  if (child.exitCode === null && child.signalCode === null) await new Promise((r) => child.once('exit', r));
}

test('SIGKILL mid-run: every page finalized exactly once, no duplicate model executions', { skip, timeout: 120_000 }, async () => {
  // No random failures, so every model execution is an `ok` and counters can be compared exactly.
  const mock = await startMock({
    LAYOUT_LATENCY_MS: '20', VLM_LATENCY_MIN_MS: '300', VLM_LATENCY_MAX_MS: '600',
    LAYOUT_FAILURE_RATE: '0', VLM_FAILURE_RATE: '0',
  });
  const cfg = testConfig();
  const env = { DATA_DIR: cfg.dataDir, MODEL_URL: mock.url, QPDF_BIN: QPDF };
  const JOBS = 5;
  const PAGES = 8;
  let orch = await startOrchestrator(env);
  try {
    const file = join(cfg.dataDir, 'in.pdf');
    await makePdf(file, PAGES);
    const body = await readFile(file);
    const ids: string[] = [];
    for (let i = 0; i < JOBS; i++) {
      const res = await fetch(`${orch.url}/jobs`, { method: 'POST', body });
      assert.equal(res.status, 202);
      ids.push(((await res.json()) as { job_id: string }).job_id);
    }

    // Kill while VLM calls are in flight, after some (not all) pages are final.
    await waitFor(async () => {
      const m = await mock.metrics();
      return m.vlm.ok! >= 5 && m.vlm.inFlight! >= 3;
    }, 30_000, 20);
    await kill(orch.child);
    const atKill = await mock.metrics();
    assert.ok(atKill.vlm.ok! < JOBS * PAGES, 'killed before the run finished');

    // Restart on the same data dir: recovery resends in-flight pages with the same Idempotency-Key.
    orch = await startOrchestrator(env);
    const url = orch.url;
    await waitFor(async () => {
      const codes = await Promise.all(ids.map((id) => fetch(`${url}/jobs/${id}/result`).then((r) => r.status)));
      return codes.every((c) => c === 200);
    }, 60_000, 100);

    const db = new Database(join(cfg.dataDir, 'orchestrator.db'), { readonly: true });
    const finals = db
      .prepare(`SELECT job_id, page, json_extract(data, '$.source') AS source FROM events
                WHERE type = 'page' AND json_extract(data, '$.stage') = 'final'`)
      .all() as { job_id: string; page: number; source: string }[];
    const seqGaps = db.prepare(`SELECT job_id, COUNT(*) AS n, MAX(seq) AS max FROM events GROUP BY job_id`).all() as { n: number; max: number }[];
    db.close();

    // 1. Every page has exactly one final event; none was emitted twice.
    assert.equal(finals.length, JOBS * PAGES);
    assert.equal(new Set(finals.map((f) => `${f.job_id}:${f.page}`)).size, JOBS * PAGES);
    assert.ok(seqGaps.every((g) => g.n === g.max), 'event seqs are gap-free per job');

    // 2. No duplicate model execution: the mock ran each page's VLM call exactly once,
    //    and the calls that were resent after the crash were served as replays.
    const m = await mock.metrics();
    const viaVlm = finals.filter((f) => f.source === 'vlm').length;
    assert.equal(viaVlm, JOBS * PAGES);
    assert.equal(m.vlm.ok, viaVlm, `vlm executions ${m.vlm.ok} == pages finalized via vlm ${viaVlm}`);
    assert.equal(m.layout.ok, JOBS * PAGES, 'layout ran once per page');
    assert.ok(m.vlm.replayed! + m.layout.replayed! > 0, 'recovery resent in-flight calls and the mock replayed them');
  } finally {
    await kill(orch.child);
    await mock.stop();
    cleanup(cfg);
  }
});

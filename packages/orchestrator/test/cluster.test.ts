import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn, type ChildProcess } from 'node:child_process';
import { readdirSync, readFileSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { get } from 'node:http';
import { createServer } from 'node:net';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { makePdf } from '../bench/make-pdf.js';
import { Assembler, readSse } from '../bench/sse-client.js';
import { cleanup, hasQpdf, QPDF, randomPort, startMock, testConfig, waitFor } from './helpers.js';

const skip = !hasQpdf ? 'qpdf not available (set QPDF_BIN)' : process.platform !== 'linux' ? 'cluster mode is Linux only' : false;
const SERVER = fileURLToPath(new URL('../src/server.ts', import.meta.url));

// HTTP worker pids of the orchestrator `ppid` (from /proc, so no ps needed). Other children (qpdf, tsx's
// esbuild service) are skipped by their command line.
function workersOf(ppid: number): number[] {
  const out: number[] = [];
  for (const d of readdirSync('/proc')) {
    if (!/^\d+$/.test(d)) continue;
    try {
      // Fields after the parenthesised command name: state, ppid, ...
      const stat = readFileSync(`/proc/${d}/stat`, 'utf8');
      const isChild = Number(stat.slice(stat.lastIndexOf(')') + 2).split(' ')[1]) === ppid;
      if (isChild && readFileSync(`/proc/${d}/cmdline`, 'utf8').includes(SERVER)) out.push(Number(d));
    } catch {
      // exited while listing
    }
  }
  return out;
}
// Running (not exited, and not a zombie waiting for a reaper: orphaned workers are reparented).
const alive = (pid: number) => {
  try {
    const stat = readFileSync(`/proc/${pid}/stat`, 'utf8');
    return stat.slice(stat.lastIndexOf(')') + 2, stat.lastIndexOf(')') + 3) !== 'Z';
  } catch {
    return false;
  }
};
// GET /healthz on a new connection.
const healthz = (port: number) =>
  new Promise<number>((resolve, reject) => {
    get({ host: '127.0.0.1', port, path: '/healthz', agent: false }, (res) => {
      res.resume();
      resolve(res.statusCode ?? 0);
    }).on('error', reject);
  });
// True when the port can be bound without SO_REUSEPORT, i.e. nothing listens on it any more.
const portFree = (port: number) =>
  new Promise<boolean>((resolve) => {
    const s = createServer();
    s.once('error', () => resolve(false));
    s.listen({ port, host: '0.0.0.0' }, () => s.close(() => resolve(true)));
  });

// Starts the orchestrator in cluster mode as a child process and waits until every worker is up.
async function startCluster(env: Record<string, string>, workers: number): Promise<{ child: ChildProcess; url: string; port: number }> {
  const port = randomPort();
  const child = spawn(process.execPath, ['--import', 'tsx', SERVER], {
    env: { ...process.env, ...env, PORT: String(port), HTTP_WORKERS: String(workers), LOG_LEVEL: 'silent' },
    stdio: ['ignore', 'ignore', 'inherit'],
  });
  const url = `http://127.0.0.1:${port}`;
  try {
    await waitFor(() => fetch(`${url}/healthz`).then((r) => r.ok, () => false), 20_000, 50);
    await waitFor(() => workersOf(child.pid!).length === workers, 10_000, 50);
  } catch (err) {
    child.kill('SIGKILL'); // a live child would keep the test process from exiting
    throw err;
  }
  return { child, url, port };
}

test('cluster mode: jobs stream to completion across processes, /metrics spans them, workers are replaced', { skip, timeout: 120_000 }, async () => {
  const mock = await startMock({ LAYOUT_LATENCY_MS: '5', VLM_LATENCY_MIN_MS: '20', VLM_LATENCY_MAX_MS: '40', LAYOUT_FAILURE_RATE: '0', VLM_FAILURE_RATE: '0' });
  const cfg = testConfig();
  const orch = await startCluster({ DATA_DIR: cfg.dataDir, MODEL_URL: mock.url, QPDF_BIN: QPDF }, 2).catch(async (err) => {
    await mock.stop();
    throw err;
  });
  try {
    const file = join(cfg.dataDir, 'in.pdf');
    await makePdf(file, 3);
    const body = await readFile(file);

    // Uploads and streams on fresh connections land on either worker; commits and events come from the
    // pipeline process, so each stream only completes if notifies cross the process boundary.
    const JOBS = 8;
    const runs = await Promise.all(
      Array.from({ length: JOBS }, async () => {
        const res = await fetch(`${orch.url}/jobs`, { method: 'POST', body });
        assert.equal(res.status, 202);
        const { job_id } = (await res.json()) as { job_id: string };
        const asm = new Assembler();
        for await (const ev of readSse(`${orch.url}/jobs/${job_id}/events`)) asm.push(ev);
        return asm;
      }),
    );
    for (const asm of runs) {
      assert.ok(asm.done, 'job.completed arrived');
      assert.deepEqual(asm.emitted, [1, 2, 3]);
    }

    // One scrape covers both kinds of process: lane metrics come from the pipeline, RSS is summed.
    const text = await (await fetch(`${orch.url}/metrics`)).text();
    assert.match(text, new RegExp(`downstream_responses_total\\{lane="vlm",code="200"\\} ${JOBS * 3}\\b`));
    assert.match(text, /\nsse_clients \d+/);
    assert.match(text, /\nqueue_depth\{lane="layout"\} 0/);
    const rss = Number(/\nprocess_resident_memory_bytes (\d+)/.exec(text)![1]);
    const procs = [orch.child.pid!, ...workersOf(orch.child.pid!)];
    const own = procs.map((pid) => Number(readFileSync(`/proc/${pid}/statm`, 'utf8').split(' ')[1]) * 4096);
    assert.ok(rss > Math.max(...own), `aggregated RSS ${rss} exceeds any single process`);

    // A worker that dies is replaced, and the service keeps answering meanwhile.
    const [victim] = workersOf(orch.child.pid!);
    process.kill(victim!, 'SIGKILL');
    // Connections the kernel had queued on the victim's sockets at that instant are reset with it
    // (SO_REUSEPORT; measured: only those in flight within ~1 ms of the kill). After that, every new
    // connection must reach the surviving worker. Fresh connections only: a pooled keep-alive one may
    // have been the victim's.
    await waitFor(() => !alive(victim!), 5000, 10);
    await waitFor(async () => {
      const codes = await Promise.all(Array.from({ length: 20 }, () => healthz(orch.port).catch(() => 0)));
      return codes.every((c) => c === 200);
    }, 3000, 50);
    await waitFor(() => {
      const now = workersOf(orch.child.pid!);
      return now.length === 2 && !now.includes(victim!);
    }, 10_000, 50);

    // If the pipeline process dies, its workers exit too and release the port (no orphan listeners).
    const workers = workersOf(orch.child.pid!);
    orch.child.kill('SIGKILL');
    await waitFor(() => workers.every((pid) => !alive(pid)), 5000, 20);
    assert.ok(await portFree(orch.port), 'port released');
  } finally {
    orch.child.kill('SIGKILL');
    await mock.stop();
    cleanup(cfg);
  }
});

test('cluster mode: SIGTERM stops workers and pipeline cleanly', { skip, timeout: 60_000 }, async () => {
  const cfg = testConfig();
  const orch = await startCluster({ DATA_DIR: cfg.dataDir, MODEL_URL: 'http://127.0.0.1:9', QPDF_BIN: QPDF }, 2);
  const workers = workersOf(orch.child.pid!);
  const exited = new Promise<number | null>((r) => orch.child.once('exit', r));
  orch.child.kill('SIGTERM');
  assert.equal(await exited, 0);
  assert.ok(workers.every((pid) => !alive(pid)), 'workers exited');
  cleanup(cfg);
});

// Cluster mode: one pipeline process plus N HTTP worker processes on the same port.
//
// Why: a 50-upload burst is ~2-3 ms of event-loop work per upload (HTTP parsing, routing, the file write,
// the job commit, the 202, then the SSE request), and on one thread that work is done serially: the 50th
// upload waits for the other 49, and model calls and SSE writes for jobs already running compete for the same
// loop. Workers divide the burst N ways and take it off the loop that dispatches model calls.
//
// Who does what:
// - The pipeline process (primary) owns everything that must exist once per data dir: boot recovery, orphan
//   cleanup, the lanes (the only code that claims pages, so "everything running belonged to a dead process"
//   still holds), qpdf extraction, retention, and the pipeline metrics. It listens on no port.
// - Each HTTP worker opens its own SQLite connection and serves every route: POST /jobs commits the job
//   itself, SSE reads the event log itself, /result and /evaluate run there too. The kernel spreads new
//   connections across all workers' SO_REUSEPORT sockets.
//
// IPC (JSON over the fork channel) carries only signals, never pages or results:
//   worker → primary: ready; wake (a job was committed); metrics? (a /metrics scrape)
//   primary → worker: notify (jobs with new events, batched per loop turn); rate (VLM rate for ETAs);
//                     metrics-json? / metrics (scrape assembly); drain; exit
import { fork, type ChildProcess } from 'node:child_process';
import { join } from 'node:path';
import type { Config } from './config.js';
import { startPipeline } from './boot.js';
import { buildApp } from './http/app.js';
import { HttpError } from './http/errors.js';
import { claimPort, closeAll, listenAll, LISTENERS } from './http/listen.js';
import { createNotifier } from './http/stream.js';
import { log } from './observability/log.js';
import { aggregate, createHttpMetrics, type MetricsJson, type MetricsText } from './observability/metrics.js';
import { openDb } from './storage/db.js';

type ToWorker =
  | { t: 'notify'; jobs: string[] }
  | { t: 'rate'; vlm: number }
  | { t: 'metrics-json?'; id: number }
  | { t: 'metrics'; id: number; scrape: MetricsText | null }
  | { t: 'drain' }
  | { t: 'exit' };
type ToPrimary = { t: 'ready' } | { t: 'wake' } | { t: 'metrics?'; id: number } | { t: 'metrics-json'; id: number; data: MetricsJson };

const RESPAWN_MS = 1000;
const STOP_GRACE_MS = 5000;
const SCRAPE_TIMEOUT_MS = 2000;

// Starts the pipeline, then forks `cfg.httpWorkers` HTTP workers running `entry` (this build's server.js).
// Resolves once every worker is listening. Returns a stop function, like startSystem.
export async function startPrimary(cfg: Config, entry: string, port = cfg.port) {
  const bound = await claimPort(port); // a port another process holds fails here, before anything starts
  const workers = new Set<ChildProcess>();

  // Coalesces notifies: one message per loop turn to each worker, listing every job with new events.
  const dirty = new Set<string>();
  // Sends the batched notifies now.
  const flush = () => {
    if (dirty.size === 0) return;
    const msg: ToWorker = { t: 'notify', jobs: [...dirty] };
    dirty.clear();
    for (const w of workers) send(w, msg);
  };
  const pipeline = await startPipeline(cfg, (id) => {
    if (dirty.size === 0) setImmediate(flush);
    dirty.add(id);
  });

  // The VLM rate feeds ETAs and Retry-After in the workers; it moves slowly, so it is pushed once a second.
  let rate = pipeline.scheduler.vlmRate();
  const rateTimer = setInterval(() => {
    const now = pipeline.scheduler.vlmRate();
    if (now === rate) return;
    rate = now;
    for (const w of workers) send(w, { t: 'rate', vlm: rate });
  }, 1000);
  rateTimer.unref();

  // Pending metrics-json requests, keyed by request id.
  const scrapes = new Map<number, (data: MetricsJson | null) => void>();
  let nextScrape = 0;
  // Asks one worker for its metrics; null if it does not answer in time.
  const askWorker = (w: ChildProcess) =>
    new Promise<MetricsJson | null>((resolve) => {
      const id = ++nextScrape;
      // Settles this request once (reply or timeout).
      const settle = (data: MetricsJson | null) => {
        scrapes.delete(id);
        resolve(data);
      };
      scrapes.set(id, settle);
      send(w, { t: 'metrics-json?', id });
      setTimeout(() => scrapes.get(id)?.(null), SCRAPE_TIMEOUT_MS).unref();
    });
  // One scrape for the whole orchestrator: the pipeline's metrics plus every worker's, aggregated.
  const scrapeAll = async (): Promise<MetricsText> => {
    const parts = await Promise.all([pipeline.metrics.registry.getMetricsAsJSON(), ...[...workers].map(askWorker)]);
    return aggregate(parts.filter((p): p is MetricsJson => p !== null));
  };

  let stopping = false;
  const perWorker = Math.ceil(LISTENERS / cfg.httpWorkers);
  // Forks one worker; resolves when it is listening, rejects if it dies first. A worker that dies later is
  // replaced after RESPAWN_MS. Its in-flight uploads fail (clients retry with their Idempotency-Key); a file
  // it was writing has no job row and is removed by the next boot's orphan cleanup.
  const spawn = () =>
    new Promise<void>((resolve, reject) => {
      const w = fork(entry, [], {
        env: { ...process.env, ORCH_ROLE: 'http', ORCH_HTTP_PORT: String(bound), ORCH_LISTENERS: String(perWorker) },
      });
      workers.add(w);
      let ready = false;
      w.on('message', (m: ToPrimary) => {
        switch (m.t) {
          case 'ready':
            ready = true;
            send(w, { t: 'rate', vlm: rate });
            resolve();
            return;
          case 'wake':
            pipeline.scheduler.wake();
            return;
          case 'metrics?':
            void scrapeAll().then(
              (scrape) => send(w, { t: 'metrics', id: m.id, scrape }),
              (err: unknown) => {
                log.error('metrics scrape failed', { err: String(err) });
                send(w, { t: 'metrics', id: m.id, scrape: null });
              },
            );
            return;
          case 'metrics-json':
            scrapes.get(m.id)?.(m.data);
        }
      });
      w.once('exit', (code, signal) => {
        workers.delete(w);
        if (!ready) return reject(new Error(`http worker exited during boot (${code ?? signal})`));
        if (stopping) return;
        log.error('http worker exited, restarting', { pid: w.pid, code, signal });
        setTimeout(() => {
          if (!stopping) spawn().catch((err: unknown) => log.error('http worker restart failed', { err: String(err) }));
        }, RESPAWN_MS);
      });
    });

  try {
    await Promise.all(Array.from({ length: cfg.httpWorkers }, spawn));
  } catch (err) {
    stopping = true;
    for (const w of workers) w.kill('SIGKILL');
    await pipeline.drain();
    await pipeline.close();
    throw err;
  }
  log.info('orchestrator listening', { port: bound, http_workers: cfg.httpWorkers, listeners_per_worker: perWorker });

  // Graceful stop: workers stop accepting, in-flight model calls finish and their events reach the workers'
  // SSE streams, then the workers exit and the db closes.
  async function stop(): Promise<void> {
    stopping = true;
    clearInterval(rateTimer);
    for (const w of workers) send(w, { t: 'drain' });
    await pipeline.drain();
    flush();
    const exited = [...workers].map((w) => exitOf(w));
    for (const w of workers) send(w, { t: 'exit' });
    await Promise.all(exited);
    await pipeline.close();
  }

  return { cfg, db: pipeline.db, scheduler: pipeline.scheduler, stop, port: bound, workers };
}

// Sends one IPC message if the channel is still open (a dying worker must not crash the primary).
function send(w: ChildProcess, msg: ToWorker): void {
  if (w.connected) w.send(msg);
}

// Resolves when the worker has exited; SIGKILLs it if it has not within STOP_GRACE_MS.
function exitOf(w: ChildProcess): Promise<void> {
  if (w.exitCode !== null || w.signalCode !== null) return Promise.resolve();
  return new Promise((resolve) => {
    const timer = setTimeout(() => w.kill('SIGKILL'), STOP_GRACE_MS);
    w.once('exit', () => {
      clearTimeout(timer);
      resolve();
    });
  });
}

// HTTP worker: one SQLite connection, the full Express app, SO_REUSEPORT sockets on the primary's port.
export async function startHttpWorker(cfg: Config): Promise<void> {
  const port = Number(process.env.ORCH_HTTP_PORT);
  const listeners = Number(process.env.ORCH_LISTENERS ?? 1);
  // Sends one message to the primary while the channel is open.
  const toPrimary = (m: ToPrimary) => {
    if (process.connected) process.send!(m);
  };
  // The primary decides when workers stop: a terminal's Ctrl-C reaches the whole process group, and a
  // worker that died first would cut SSE streams before the drain. If the primary dies, so do we.
  for (const sig of ['SIGTERM', 'SIGINT'] as const) process.on(sig, () => {});
  process.once('disconnect', () => process.exit(1));

  // The primary has already created the schema and run recovery; this connection only reads and writes rows.
  const db = openDb(join(cfg.dataDir, 'orchestrator.db'));
  const notifier = createNotifier();
  const registry = createHttpMetrics(() => notifier.clients);
  let vlmRate = cfg.lanes.vlm.maxRps;

  const scrapes = new Map<number, (s: MetricsText | null) => void>();
  let nextScrape = 0;
  // A /metrics request: the primary assembles the scrape from every process.
  const metrics = () =>
    new Promise<MetricsText>((resolve, reject) => {
      const id = ++nextScrape;
      // Settles this scrape once (reply or timeout).
      const settle = (s: MetricsText | null) => {
        scrapes.delete(id);
        if (s) resolve(s);
        else reject(new HttpError(503, 'metrics_unavailable'));
      };
      scrapes.set(id, settle);
      toPrimary({ t: 'metrics?', id });
      setTimeout(() => scrapes.get(id)?.(null), SCRAPE_TIMEOUT_MS).unref();
    });

  const app = buildApp({ cfg, db, notifier, metrics, scheduler: { wake: () => toPrimary({ t: 'wake' }), vlmRate: () => vlmRate } });
  const servers = await listenAll(app, port, listeners, true);
  let closeRest: (() => Promise<void>) | undefined;

  process.on('message', (m: ToWorker) => {
    switch (m.t) {
      case 'notify':
        for (const id of m.jobs) notifier.notify(id);
        return;
      case 'rate':
        vlmRate = m.vlm;
        return;
      case 'metrics-json?':
        void registry.getMetricsAsJSON().then((data) => toPrimary({ t: 'metrics-json', id: m.id, data }));
        return;
      case 'metrics':
        scrapes.get(m.id)?.(m.scrape);
        return;
      case 'drain':
        closeRest ??= closeAll(servers);
        return;
      case 'exit':
        closeRest ??= closeAll(servers);
        // One more turn first, so SSE loops woken by the last notify write their events before the cut.
        setImmediate(() =>
          void closeRest!().finally(() => {
            db.close();
            process.exit(0);
          }),
        );
    }
  });
  toPrimary({ t: 'ready' });
}

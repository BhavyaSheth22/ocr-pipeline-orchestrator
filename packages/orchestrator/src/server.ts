import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { buildApp } from './http/app.js';
import { claimPort, closeAll, listenAll, LISTENERS } from './http/listen.js';
import { loadConfig, type Config } from './config.js';
import { openDb } from './storage/db.js';
import { cleanOrphans, ensureDirs, removeJobFiles } from './storage/files.js';
import { createSplitter } from './pdf/pdf.js';
import { log } from './observability/log.js';
import { createMetrics } from './observability/metrics.js';
import { createScheduler, type SchedulerHandle } from './pipeline/pipeline.js';
import { qpdf } from './pdf/qpdf-pool.js';
import { createNotifier } from './http/stream.js';

// Pays one-time start-up costs at boot instead of during the first upload burst (profiled: ~170 ms of
// lazy module loading and ~3 ms per qpdf spawn landed on the event loop mid-burst). Failures only log:
// the model server or qpdf may come up later, and the normal paths retry.
async function warmUp(cfg: Config): Promise<void> {
  // The first fetch() loads undici; this one also opens a keep-alive connection to the model server.
  await fetch(`${cfg.modelUrl}/healthz`, { signal: AbortSignal.timeout(2000) }).then(
    (res) => res.body?.cancel(),
    (err: unknown) => log.warn('warm-up: model server not reachable', { err: String(err) }),
  );
  // Starts the qpdf worker threads and runs qpdf once on each.
  await qpdf.warm(cfg.qpdfBin).catch((err: unknown) => log.warn('warm-up: qpdf not available', { err: String(err) }));
}

// Boots the orchestrator: claim port → db open → recover → clean orphans → warm up → lanes start → listen. Returns a stop function.
export async function startSystem(cfg: Config, port = cfg.port) {
  const bound = await claimPort(port); // first, so a taken port fails before anything starts
  await ensureDirs(cfg);
  const db = openDb(join(cfg.dataDir, 'orchestrator.db'));
  const recovered = db.recover();
  const orphans = await cleanOrphans(cfg, (id) => db.getJob(id) !== undefined);
  log.info('boot', { recovered_pages: recovered, orphan_files: orphans });

  const notifier = createNotifier();
  // Lanes are created after metrics (their counters feed the pipeline), so the gauges read them late-bound.
  let lanes: SchedulerHandle['lanes'] | undefined;
  const metrics = createMetrics({ db, notifier, lanes: () => lanes! });
  metrics.recovered.inc(recovered);
  const scheduler = createScheduler({ cfg, db, extract: createSplitter(cfg), notify: (id) => notifier.notify(id), events: metrics.events });
  lanes = scheduler.lanes;
  const app = buildApp({ cfg, db, scheduler, notifier, registry: metrics.registry });
  await warmUp(cfg);
  scheduler.start();

  // Retention: delete finished jobs (rows and files) older than RESULT_TTL_MS. unref'd so it never holds the process.
  const sweep = setInterval(() => {
    const ids = db.purge(Date.now() - cfg.resultTtlMs);
    for (const id of ids) void removeJobFiles(cfg, id);
    if (ids.length) log.info('retention purge', { jobs: ids.length });
  }, 60_000);
  sweep.unref();

  const servers = await listenAll(app, bound, LISTENERS);
  log.info('orchestrator listening', { port: bound, listeners: servers.length });

  // Graceful stop: refuse new connections, let in-flight model calls finish, then close the db.
  async function stop(): Promise<void> {
    clearInterval(sweep);
    const closeRest = closeAll(servers);
    await scheduler.stop();
    await closeRest();
    await qpdf.close(); // after the scheduler drained, so no extraction is still waiting on a worker
    db.close();
  }

  return { cfg, db, scheduler, notifier, stop, port: bound };
}

export type System = Awaited<ReturnType<typeof startSystem>>;

// Entry point when run directly (node dist/src/server.js).
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const sys = await startSystem(loadConfig());
  for (const sig of ['SIGTERM', 'SIGINT'] as const) {
    // On a stop signal, drain and exit; a second signal or 10 s forces exit.
    process.once(sig, () => {
      log.info('shutting down', { sig });
      setTimeout(() => process.exit(1), 10_000).unref();
      void sys.stop().then(() => process.exit(0));
    });
  }
}

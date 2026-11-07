import { join } from 'node:path';
import type { Config } from './config.js';
import { openDb } from './storage/db.js';
import { cleanOrphans, ensureDirs, removeJobFiles } from './storage/files.js';
import { createSplitter } from './pdf/pdf.js';
import { log } from './observability/log.js';
import { createMetrics } from './observability/metrics.js';
import { createScheduler, type SchedulerHandle } from './pipeline/pipeline.js';
import { qpdf } from './pdf/qpdf-pool.js';

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

// Boots the pipeline side, the part that exists once per data dir: db open → recover → clean orphans →
// metrics → lanes → warm up → lanes start → retention sweep. Recovery and orphan cleanup run before anything
// can accept an upload: a file being uploaded has no job row yet and would look like an orphan.
// `notify` is called after every commit that appended events for a job.
export async function startPipeline(cfg: Config, notify: (jobId: string) => void, sseClients?: () => number) {
  await ensureDirs(cfg);
  const db = openDb(join(cfg.dataDir, 'orchestrator.db'));
  const recovered = db.recover();
  const orphans = await cleanOrphans(cfg, (id) => db.getJob(id) !== undefined);
  log.info('boot', { recovered_pages: recovered, orphan_files: orphans });

  // Lanes are created after metrics (their counters feed the pipeline), so the gauges read them late-bound.
  let lanes: SchedulerHandle['lanes'] | undefined;
  const metrics = createMetrics({ db, lanes: () => lanes!, sseClients });
  metrics.recovered.inc(recovered);
  const scheduler = createScheduler({ cfg, db, extract: createSplitter(cfg), notify, events: metrics.events });
  lanes = scheduler.lanes;
  await warmUp(cfg);
  scheduler.start();

  // Retention: delete finished jobs (rows and files) older than RESULT_TTL_MS. unref'd so it never holds the process.
  const sweep = setInterval(() => {
    const ids = db.purge(Date.now() - cfg.resultTtlMs);
    for (const id of ids) void removeJobFiles(cfg, id);
    if (ids.length) log.info('retention purge', { jobs: ids.length });
  }, 60_000);
  sweep.unref();

  return {
    db,
    scheduler,
    metrics,
    // Stops the sweep and lets in-flight model calls finish (their outcomes still commit and notify).
    async drain(): Promise<void> {
      clearInterval(sweep);
      await scheduler.stop();
    },
    // After drain: stops the qpdf workers and closes the db.
    async close(): Promise<void> {
      await qpdf.close();
      db.close();
    },
  };
}

export type Pipeline = Awaited<ReturnType<typeof startPipeline>>;

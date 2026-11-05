import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import type { Server } from 'node:http';
import { buildApp } from './app.js';
import { loadConfig, type Config } from './config.js';
import { openDb } from './db.js';
import { cleanOrphans, createSplitter, ensureDirs, removeJobFiles } from './ingest.js';
import { log } from './log.js';
import { createScheduler } from './pipeline.js';

// Boots the orchestrator: db open → recover → clean orphans → lanes start → listen. Returns a stop function.
export async function startSystem(cfg: Config, port = cfg.port) {
  await ensureDirs(cfg);
  const db = openDb(join(cfg.dataDir, 'orchestrator.db'));
  const recovered = db.recover();
  const orphans = await cleanOrphans(cfg, (id) => db.getJob(id) !== undefined);
  log.info('boot', { recovered_pages: recovered, orphan_files: orphans });

  const scheduler = createScheduler({ cfg, db, extract: createSplitter(cfg), notify: () => {} });
  const app = buildApp({ cfg, db, scheduler });
  scheduler.start();

  // Retention: delete finished jobs (rows and files) older than RESULT_TTL_MS. unref'd so it never holds the process.
  const sweep = setInterval(() => {
    const ids = db.purge(Date.now() - cfg.resultTtlMs);
    for (const id of ids) void removeJobFiles(cfg, id);
    if (ids.length) log.info('retention purge', { jobs: ids.length });
  }, 60_000);
  sweep.unref();

  const server: Server = await new Promise((resolve) => {
    const s = app.listen(port, '0.0.0.0', () => resolve(s));
  });
  log.info('orchestrator listening', { port: (server.address() as { port: number }).port });

  // Graceful stop: refuse new connections, let in-flight model calls finish, then close the db.
  async function stop(): Promise<void> {
    clearInterval(sweep);
    const closed = new Promise((r) => server.close(r));
    server.closeIdleConnections();
    await scheduler.stop();
    server.closeAllConnections();
    await closed;
    db.close();
  }

  return { cfg, db, scheduler, server, stop, port: (server.address() as { port: number }).port };
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

import { fileURLToPath, pathToFileURL } from 'node:url';
import { buildApp } from './http/app.js';
import { claimPort, closeAll, listenAll, LISTENERS } from './http/listen.js';
import { createNotifier } from './http/stream.js';
import { loadConfig, type Config } from './config.js';
import { startPipeline } from './boot.js';
import { startHttpWorker, startPrimary } from './cluster.js';
import { log } from './observability/log.js';
import { render } from './observability/metrics.js';

// Single-process mode: pipeline and HTTP on one event loop (HTTP_WORKERS=0, non-Linux, and most tests).
// claim port → pipeline boot → listen. Returns a stop function.
export async function startSystem(cfg: Config, port = cfg.port) {
  const bound = await claimPort(port); // first, so a taken port fails before anything starts
  const notifier = createNotifier();
  const pipeline = await startPipeline(cfg, (id) => notifier.notify(id), () => notifier.clients);
  const { db, scheduler } = pipeline;
  const app = buildApp({ cfg, db, scheduler, notifier, metrics: () => render(pipeline.metrics.registry) });
  const servers = await listenAll(app, bound, LISTENERS);
  log.info('orchestrator listening', { port: bound, listeners: servers.length, http_workers: 0 });

  // Graceful stop: refuse new connections, let in-flight model calls finish, then close the db.
  async function stop(): Promise<void> {
    const closeRest = closeAll(servers);
    await pipeline.drain();
    await closeRest();
    await pipeline.close();
  }

  return { cfg, db, scheduler, notifier, stop, port: bound };
}

export type System = Awaited<ReturnType<typeof startSystem>>;

// Entry point when run directly (node dist/src/server.js). The same file is the HTTP worker's entry
// (ORCH_ROLE=http, forked by startPrimary).
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const cfg = loadConfig();
  if (process.env.ORCH_ROLE === 'http') {
    await startHttpWorker(cfg);
  } else {
    const sys = cfg.httpWorkers > 0 ? await startPrimary(cfg, fileURLToPath(import.meta.url)) : await startSystem(cfg);
    for (const sig of ['SIGTERM', 'SIGINT'] as const) {
      // On a stop signal, drain and exit; a second signal or 10 s forces exit.
      process.once(sig, () => {
        log.info('shutting down', { sig });
        setTimeout(() => process.exit(1), 10_000).unref();
        void sys.stop().then(() => process.exit(0));
      });
    }
  }
}

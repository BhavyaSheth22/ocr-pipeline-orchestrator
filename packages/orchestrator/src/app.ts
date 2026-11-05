import { randomBytes, randomUUID } from 'node:crypto';
import { rename, rm } from 'node:fs/promises';
import { join } from 'node:path';
import express, { type Express, type NextFunction, type Request, type Response } from 'express';
import type { Config } from './config.js';
import type { Db, JobRow } from './db.js';
import { evaluate } from './eval/evaluate.js';
import { countPages, HttpError, paths, receiveUpload } from './ingest.js';
import { log } from './log.js';
import { streamEvents, type Notifier } from './stream.js';
import type { Registry } from 'prom-client';

/** What the HTTP layer needs from the scheduler. */
export interface Scheduler {
  /** A new job was committed: wake the layout lane. */
  wake(): void;
  /** Current adaptive VLM rate in pages/s, the throughput bottleneck used for ETAs. */
  vlmRate(): number;
}

export interface AppDeps {
  cfg: Config;
  db: Db;
  scheduler: Scheduler;
  notifier: Notifier;
  registry?: Registry;
}

// Body returned by POST /jobs (also for an Idempotency-Key replay).
function acceptedBody(job: JobRow, backlogPages: number, etaS: number) {
  return { job_id: job.id, events_url: `/jobs/${job.id}/events`, total_pages: job.total_pages, backlog_pages: backlogPages, eta_s: etaS };
}

// Builds the Express app; server.ts owns the process lifecycle, tests call this directly.
export function buildApp({ cfg, db, scheduler, notifier, registry }: AppDeps): Express {
  const app = express();
  app.disable('x-powered-by');
  app.set('etag', false);

  // Seconds to drain `pages` at the current VLM rate.
  const etaFor = (pages: number) => Math.ceil(pages / Math.max(scheduler.vlmRate(), 0.1));

  // Ingestion: admission check before reading the body, stream to disk, count pages, commit in one transaction.
  app.post('/jobs', async (req: Request, res: Response) => {
    const idemKey = req.get('Idempotency-Key') ?? null;
    if (idemKey !== null && (idemKey.length === 0 || idemKey.length > 255)) {
      throw new HttpError(400, 'bad_request', 'Idempotency-Key must be 1-255 characters');
    }
    // A retried upload gets the original job back; its body is never read.
    const existing = idemKey === null ? undefined : db.jobByIdemKey(idemKey);
    if (existing) {
      const backlog = db.backlogPages();
      res.status(202).json(acceptedBody(existing, backlog, etaFor(backlog)));
      return;
    }

    // Front-door backpressure: when the backlog is full, tell the client when there will be room.
    const backlogBefore = db.backlogPages();
    if (backlogBefore >= cfg.maxQueuedPages) {
      const base = (backlogBefore - cfg.maxQueuedPages + cfg.maxPages) / Math.max(scheduler.vlmRate(), 0.1);
      const retryAfter = Math.min(600, Math.max(1, Math.ceil(base * (1 + 0.2 * Math.random()))));
      throw new HttpError(503, 'backlog_full', 'queue is full, retry later', { 'Retry-After': String(retryAfter) });
    }

    const id = randomUUID();
    const tmp = join(paths.tmp(cfg), id);
    await receiveUpload(req, tmp, cfg.maxUploadBytes);
    let pages: number;
    try {
      pages = await countPages(cfg, tmp);
    } catch (err) {
      await rm(tmp, { force: true });
      throw err;
    }
    const pdfPath = paths.job(cfg, id);
    await rename(tmp, pdfPath);

    const backlog = db.backlogPages() + pages;
    const etaS = etaFor(backlog);
    try {
      db.createJob({
        id,
        idemKey,
        traceId: randomBytes(16).toString('hex'),
        pdfPath,
        totalPages: pages,
        window: cfg.window,
        header: { backlog_pages: backlog, eta_s: etaS },
      });
    } catch (err) {
      await rm(pdfPath, { force: true });
      // Two concurrent uploads with one Idempotency-Key: the loser returns the winner's job.
      const winner = idemKey === null ? undefined : db.jobByIdemKey(idemKey);
      if (winner) {
        res.status(202).json(acceptedBody(winner, backlog, etaS));
        return;
      }
      throw err;
    }
    log.info('job accepted', { job_id: id, trace_id: db.getJob(id)!.trace_id, pages, backlog_pages: backlog });
    scheduler.wake();
    res.status(202).json(acceptedBody(db.getJob(id)!, backlog, etaS));
  });

  // SSE stream of the job's events, resumable with Last-Event-ID.
  app.get('/jobs/:id/events', (req, res) => streamEvents(req, res, db, notifier));

  // The assembled document tree, once every page is final.
  app.get('/jobs/:id/result', (req, res) => {
    const job = db.getJob(req.params.id);
    if (!job) throw new HttpError(404, 'not_found');
    if (job.state !== 'done') throw new HttpError(409, 'not_ready', `job is still running (${job.watermark - 1}/${job.total_pages} pages in order)`);
    const children = db.finals(job.id).map((r) => {
      const d = JSON.parse(r.data) as { result: object; status: string; source: string | null; low_confidence: boolean };
      return { ...d.result, page: r.page, status: d.status, source: d.source, low_confidence: d.low_confidence };
    });
    res.json({ type: 'document', job_id: job.id, children });
  });

  // Evaluation engine. Runs synchronously: bounded by the 1 MB body cap and the TED step budget (~40 ms).
  app.post('/evaluate', express.json({ limit: '1mb' }), (req, res) => {
    res.json(evaluate(req.body, cfg.tedMaxSteps));
  });

  // Prometheus scrape endpoint.
  app.get('/metrics', async (_req, res) => {
    if (!registry) throw new HttpError(404, 'not_found');
    res.set('content-type', registry.contentType).send(await registry.metrics());
  });

  // Liveness, used by the compose healthcheck.
  app.get('/healthz', (_req, res) => {
    res.json({ ok: true });
  });

  // Unknown routes get a JSON 404.
  app.use((_req, res) => {
    res.status(404).json({ error: 'not_found' });
  });

  // Maps HttpError (and body-parser errors) to JSON; anything else is a logged 500.
  app.use((err: unknown, req: Request, res: Response, _next: NextFunction) => {
    if (res.headersSent) {
      res.destroy();
      return;
    }
    if (err instanceof HttpError) {
      // After a mid-upload 413 the rest of the body is unread, so the connection cannot be reused.
      if (err.status === 413 && !req.complete) res.set('Connection', 'close');
      res.status(err.status).set(err.headers).json({ error: err.code, message: err.message, ...err.details });
      return;
    }
    const e = err as { type?: string; status?: number };
    if (e.type === 'entity.parse.failed') {
      res.status(400).json({ error: 'bad_request', message: 'invalid JSON' });
    } else if (e.type === 'entity.too.large') {
      res.status(413).json({ error: 'payload_too_large' });
    } else {
      console.error(JSON.stringify({ level: 'error', msg: 'unhandled error', err: String(err), stack: (err as Error)?.stack }));
      res.status(500).json({ error: 'internal_error' });
    }
  });

  return app;
}

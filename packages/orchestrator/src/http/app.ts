import express, { type Express, type NextFunction, type Request, type Response } from 'express';
import type { Registry } from 'prom-client';
import type { Config } from '../config.js';
import { evaluate } from '../eval/evaluate.js';
import { createJobService, type Scheduler } from '../services/job-service.js';
import type { Db } from '../storage/db.js';
import { HttpError } from './errors.js';
import type { ErrorResponse, EvaluateRequest, EvaluateResponse, HealthResponse, JobAccepted, JobResultResponse } from './types.js';
import { streamEvents, type Notifier } from './stream.js';

export type { Scheduler };

export interface AppDeps {
  cfg: Config;
  db: Db;
  scheduler: Scheduler;
  notifier: Notifier;
  registry?: Registry;
}

// Builds the Express app; server.ts owns the process lifecycle, tests call this directly.
export function buildApp({ cfg, db, scheduler, notifier, registry }: AppDeps): Express {
  const app = express();
  app.disable('x-powered-by');
  app.set('etag', false);

  const jobs = createJobService({ cfg, db, scheduler });

  // Create a job from the raw PDF body; see JobService.submit for the admission and commit steps.
  app.post('/jobs', async (req: Request, res: Response<JobAccepted>) => {
    res.status(202).json(await jobs.submit(req, req.get('Idempotency-Key') ?? null));
  });

  // SSE stream of the job's events, resumable with Last-Event-ID.
  app.get('/jobs/:id/events', (req, res) => streamEvents(req, res, db, notifier));

  // The assembled document tree, once every page is final.
  app.get('/jobs/:id/result', (req: Request<{ id: string }>, res: Response<JobResultResponse>) => {
    res.json(jobs.result(req.params.id));
  });

  // Evaluation engine. Runs synchronously: bounded by the 1 MB body cap and the TED step budget (~40 ms).
  app.post('/evaluate', express.json({ limit: '1mb' }), (req: Request<object, EvaluateResponse, EvaluateRequest>, res: Response<EvaluateResponse>) => {
    res.json(evaluate(req.body, cfg.tedMaxSteps));
  });

  // Prometheus scrape endpoint.
  app.get('/metrics', async (_req, res) => {
    if (!registry) throw new HttpError(404, 'not_found');
    res.set('content-type', registry.contentType).send(await registry.metrics());
  });

  // Liveness, used by the compose healthcheck.
  app.get('/healthz', (_req, res: Response<HealthResponse>) => {
    res.json({ ok: true });
  });

  // Unknown routes get a JSON 404.
  app.use((_req, res: Response<ErrorResponse>) => {
    res.status(404).json({ error: 'not_found' });
  });

  // Maps HttpError (and body-parser errors) to JSON; anything else is a logged 500.
  app.use((err: unknown, req: Request, res: Response<ErrorResponse>, _next: NextFunction) => {
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

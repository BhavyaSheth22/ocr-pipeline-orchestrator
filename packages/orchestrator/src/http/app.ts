import express, { type Express, type NextFunction, type Request, type Response } from 'express';
import type { Config } from '../config.js';
import { evaluate } from '../eval/evaluate.js';
import { log, withLogContext } from '../observability/log.js';
import type { MetricsText } from '../observability/metrics.js';
import { newTraceId, parseTraceparent } from '../observability/trace.js';
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
  /** The Prometheus scrape; in cluster mode it is assembled across processes by the pipeline process. */
  metrics?: () => Promise<MetricsText>;
}

// Builds the Express app; server.ts owns the process lifecycle, tests call this directly.
export function buildApp({ cfg, db, scheduler, notifier, metrics }: AppDeps): Express {
  const app = express();
  app.disable('x-powered-by');
  app.set('etag', false);

  const jobs = createJobService({ cfg, db, scheduler });

  // Trace context: every request runs under a trace id, the caller's (`traceparent` header) or a fresh one.
  // It is echoed as X-Trace-Id and lands on every log line written while handling the request.
  app.use((req, res, next) => {
    const start = performance.now();
    const traceId = parseTraceparent(req.get('traceparent'))?.traceId ?? newTraceId();
    res.locals.traceId = traceId;
    res.set('X-Trace-Id', traceId);
    // Access log on 'close', so aborted requests (and SSE streams, when they end) are logged too.
    if (cfg.logRequests && req.path !== '/healthz') {
      // (/healthz is skipped: the Docker healthcheck would drown the log.)
      res.once('close', () => {
        const l = res.locals as { traceId: string; jobId?: string };
        log.info('request', {
          trace_id: l.traceId,
          job_id: l.jobId,
          method: req.method,
          url: req.originalUrl,
          status: res.statusCode,
          ms: Math.round(performance.now() - start),
          ...(res.writableFinished ? {} : { aborted: true }),
        });
      });
    }
    withLogContext({ trace_id: traceId }, next);
  });

  // Job-scoped routes run under the job's own trace id, so its SSE and result reads join the job's trace.
  app.param('id', (req, res, next, id: string) => {
    const job = db.getJob(id);
    if (!job) return next();
    res.locals.traceId = job.trace_id;
    res.locals.jobId = job.id;
    res.set('X-Trace-Id', job.trace_id);
    withLogContext({ trace_id: job.trace_id, job_id: job.id }, next);
  });

  // Create a job from the raw PDF body; see JobService.submit for the admission and commit steps.
  app.post('/jobs', async (req: Request, res: Response<JobAccepted>) => {
    const accepted = await jobs.submit(req, req.get('Idempotency-Key') ?? null, res.locals.traceId as string);
    // An Idempotency-Key replay returns the original job, and with it the original trace.
    res.locals.traceId = accepted.trace_id;
    res.locals.jobId = accepted.job_id;
    res.status(202).set('X-Trace-Id', accepted.trace_id).json(accepted);
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
    if (!metrics) throw new HttpError(404, 'not_found');
    const { contentType, body } = await metrics();
    res.set('content-type', contentType).send(body);
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
      log.error('unhandled error', { trace_id: res.locals.traceId, err: String(err), stack: (err as Error)?.stack });
      res.status(500).json({ error: 'internal_error' });
    }
  });

  return app;
}

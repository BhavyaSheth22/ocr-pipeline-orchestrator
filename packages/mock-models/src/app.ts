import { setTimeout as sleep } from 'node:timers/promises';
import express, { type Express, type NextFunction, type Request, type Response } from 'express';
import { defaultConfig, EndpointState, type Endpoint } from './chaos.js';
import { layoutPayload, vlmPayload } from './payloads.js';
import { validate, type Rule } from './validate.js';

export interface AppOptions {
  env?: NodeJS.ProcessEnv;
}

const predictRules: Record<string, Rule> = {
  document_id: { type: 'string', min: 1, max: 128, required: true },
  page: { type: 'integer', min: 1, max: 100000, required: true },
  image_ref: { type: 'string', max: 512 },
};

interface PredictBody { document_id: string; page: number }

// Creates the Express app plus per-endpoint state; separate from server.ts so tests can run it on any port.
export function buildApp(opts: AppOptions = {}): { app: Express; states: Record<Endpoint, EndpointState> } {
  const env = opts.env ?? process.env;
  const seed = Number(env.MOCK_SEED ?? 42);
  const states: Record<Endpoint, EndpointState> = {
    layout: new EndpointState('layout', defaultConfig(env, 'layout'), seed),
    vlm: new EndpointState('vlm', defaultConfig(env, 'vlm'), seed),
  };

  const app = express();
  app.disable('x-powered-by');
  app.set('etag', false);
  app.use(express.json({ limit: '16kb' }));

  // Optional one-line JSON access log (LOG_REQUESTS=1); off by default so logging I/O never skews benchmarks.
  if (env.LOG_REQUESTS === '1') {
    app.use((req, res, next) => {
      const start = performance.now();
      res.on('finish', () => {
        console.log(JSON.stringify({ level: 'info', method: req.method, url: req.originalUrl, status: res.statusCode, ms: Math.round(performance.now() - start) }));
      });
      next();
    });
  }

  // Shared handler for both predict endpoints: shed load, rate-limit, simulate latency and failure, return payload.
  async function predict(kind: Endpoint, req: Request, res: Response): Promise<void> {
    const err = validate(req.body, predictRules);
    if (err) {
      res.status(400).json({ error: 'bad_request', message: err });
      return;
    }

    const st = states[kind];
    const c = st.counters;
    c.total++;

    // 1. Protect the mock itself: refuse when too many requests are already running.
    if (c.inFlight >= st.config.maxInFlight) {
      c.shed++;
      res.status(503).set('Retry-After', '1').json({ error: 'overloaded' });
      return;
    }

    // 2. Token bucket: over the limit means a natural 429. Rejections cost no simulated work.
    const take = st.limiter.tryTake();
    if (!take.ok) {
      c.rateLimited++;
      const waitMs = take.retryAfterMs;
      res
        .status(429)
        .set({
          'Retry-After': String(Math.max(1, Math.ceil(waitMs / 1000))),
          'X-RateLimit-Limit': String(st.config.rps),
          'X-RateLimit-Remaining': '0',
        })
        .json({ error: 'rate_limited', retry_after_ms: waitMs });
      return;
    }

    // 3. Accepted: simulate the model doing work.
    c.inFlight++;
    try {
      const { hang, fail, latencyMs } = st.draw();
      if (hang) {
        c.hung++;
        // Never answer; give up only when the connection closes (lets clients test their timeouts).
        await new Promise<void>((resolve) => res.once('close', resolve));
        return;
      }
      await sleep(latencyMs);
      if (fail) {
        c.failed++;
        res.status(500).json({ error: 'model_failure' });
        return;
      }
      c.ok++;
      const { document_id, page } = req.body as PredictBody;
      res.set('X-Mock-Latency-Ms', String(latencyMs));
      res.json(kind === 'layout' ? layoutPayload(document_id, page) : vlmPayload(document_id, page));
    } finally {
      c.inFlight--;
    }
  }

  // Layout model: fast, high-rate endpoint.
  app.post('/v1/predict/layout', (req, res) => predict('layout', req, res));
  // VLM model: slow, strictly rate-limited endpoint.
  app.post('/v1/predict/vlm', (req, res) => predict('vlm', req, res));

  // Liveness check used by the Docker healthcheck.
  app.get('/healthz', (_req, res) => {
    res.json({ ok: true });
  });
  // Counters for requests, 429s, failures and in-flight work, for tests and benchmarks.
  app.get('/metrics-lite', (_req, res) => {
    res.json({ layout: states.layout.counters, vlm: states.vlm.counters });
  });

  // Unknown routes get a JSON 404 instead of Express's default HTML page.
  app.use((_req, res) => {
    res.status(404).json({ error: 'not_found' });
  });

  // Turns body-parser failures (bad JSON, oversize body) into JSON errors; anything else becomes a 500.
  app.use((err: { type?: string; status?: number }, _req: Request, res: Response, _next: NextFunction) => {
    if (err.type === 'entity.parse.failed') {
      res.status(400).json({ error: 'bad_request', message: 'invalid JSON' });
    } else if (err.type === 'entity.too.large') {
      res.status(413).json({ error: 'payload_too_large' });
    } else {
      console.error(JSON.stringify({ level: 'error', msg: 'unhandled error', err: String(err) }));
      res.status(500).json({ error: 'internal_error' });
    }
  });

  return { app, states };
}

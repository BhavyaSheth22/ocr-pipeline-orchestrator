import { timingSafeEqual } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import Fastify, { type FastifyInstance, type FastifyReply, type FastifyRequest } from 'fastify';
import { defaultConfig, EndpointState, type Endpoint, type EndpointConfig } from './chaos.js';
import { layoutPayload, vlmPayload } from './payloads.js';

export interface AppOptions {
  env?: NodeJS.ProcessEnv;
  logger?: boolean;
}

const predictSchema = {
  body: {
    type: 'object',
    required: ['document_id', 'page'],
    properties: {
      document_id: { type: 'string', minLength: 1, maxLength: 128 },
      page: { type: 'integer', minimum: 1, maximum: 100000 },
      image_ref: { type: 'string', maxLength: 512 },
    },
    additionalProperties: false,
  },
} as const;

interface PredictBody { document_id: string; page: number }

export function buildApp(opts: AppOptions = {}): { app: FastifyInstance; states: Record<Endpoint, EndpointState> } {
  const env = opts.env ?? process.env;
  const seed = Number(env.MOCK_SEED ?? 42);
  const adminToken = env.ADMIN_TOKEN ?? '';
  const states: Record<Endpoint, EndpointState> = {
    layout: new EndpointState('layout', defaultConfig(env, 'layout'), seed),
    vlm: new EndpointState('vlm', defaultConfig(env, 'vlm'), seed),
  };

  const app = Fastify({ logger: opts.logger ?? false, bodyLimit: 16 * 1024 });

  async function predict(kind: Endpoint, req: FastifyRequest, reply: FastifyReply) {
    const st = states[kind];
    const c = st.counters;
    c.total++;

    // 1. Protect the mock itself: refuse when too many requests are already running.
    if (c.inFlight >= st.config.maxInFlight) {
      c.shed++;
      return reply.code(503).header('Retry-After', '1').send({ error: 'overloaded' });
    }

    // 2. Forced 429 spike or the normal token bucket. Rejections cost no simulated work.
    const spike = st.activeSpike();
    const take = st.limiter.tryTake();
    if (spike?.mode === '429' || !take.ok) {
      c.rateLimited++;
      const waitMs = spike?.mode === '429' ? Math.max(1, Math.ceil(spike.untilMs - performance.now())) : take.retryAfterMs;
      return reply
        .code(429)
        .header('Retry-After', String(Math.max(1, Math.ceil(waitMs / 1000))))
        .header('X-RateLimit-Limit', String(st.config.rps))
        .header('X-RateLimit-Remaining', '0')
        .send({ error: 'rate_limited', retry_after_ms: waitMs });
    }

    // 3. Accepted: simulate the model doing work.
    c.inFlight++;
    try {
      const { hang, fail, latencyMs } = st.draw();
      if (hang) {
        c.hung++;
        // Never answer; give up only when the client disconnects (tests client timeouts).
        await new Promise<void>((resolve) => req.raw.once('close', resolve));
        return reply;
      }
      await sleep(latencyMs);
      if (fail) {
        c.failed++;
        return reply.code(500).send({ error: 'model_failure' });
      }
      c.ok++;
      const { document_id, page } = req.body as PredictBody;
      reply.header('X-Mock-Latency-Ms', String(latencyMs));
      return kind === 'layout' ? layoutPayload(document_id, page) : vlmPayload(document_id, page);
    } finally {
      c.inFlight--;
    }
  }

  app.post('/v1/predict/layout', { schema: predictSchema }, (req, reply) => predict('layout', req, reply));
  app.post('/v1/predict/vlm', { schema: predictSchema }, (req, reply) => predict('vlm', req, reply));

  app.get('/healthz', async () => ({ ok: true }));
  app.get('/metrics-lite', async () => ({ layout: states.layout.counters, vlm: states.vlm.counters }));

  // Control plane: guarded by a shared token, compared in constant time.
  app.register(async (admin) => {
    admin.addHook('onRequest', async (req, reply) => {
      const given = Buffer.from(String(req.headers['x-admin-token'] ?? ''));
      const want = Buffer.from(adminToken);
      if (!adminToken || given.length !== want.length || !timingSafeEqual(given, want)) {
        return reply.code(401).send({ error: 'unauthorized' });
      }
    });

    admin.get('/admin/state', async () => ({
      layout: { config: states.layout.config, spike: states.layout.activeSpike() },
      vlm: { config: states.vlm.config, spike: states.vlm.activeSpike() },
    }));

    admin.post<{ Body: { endpoint: Endpoint; durationMs: number; mode: '429' | 'slow'; factor?: number } }>(
      '/admin/spike',
      {
        schema: {
          body: {
            type: 'object',
            required: ['endpoint', 'durationMs', 'mode'],
            properties: {
              endpoint: { enum: ['layout', 'vlm'] },
              durationMs: { type: 'integer', minimum: 1, maximum: 600000 },
              mode: { enum: ['429', 'slow'] },
              factor: { type: 'number', minimum: 1, maximum: 50 },
            },
            additionalProperties: false,
          },
        },
      },
      async (req) => {
        const { endpoint, durationMs, mode, factor } = req.body;
        states[endpoint].startSpike(mode, durationMs, factor);
        return { ok: true, spike: states[endpoint].activeSpike() };
      },
    );

    admin.post<{ Body: { endpoint: Endpoint } & Partial<EndpointConfig> }>(
      '/admin/config',
      {
        schema: {
          body: {
            type: 'object',
            required: ['endpoint'],
            properties: {
              endpoint: { enum: ['layout', 'vlm'] },
              rps: { type: 'number', exclusiveMinimum: 0 },
              burst: { type: 'number', minimum: 1 },
              latencyMinMs: { type: 'number', minimum: 0 },
              latencyMaxMs: { type: 'number', minimum: 0 },
              failureRate: { type: 'number', minimum: 0, maximum: 1 },
              hangRate: { type: 'number', minimum: 0, maximum: 1 },
              maxInFlight: { type: 'integer', minimum: 1 },
            },
            additionalProperties: false,
          },
        },
      },
      async (req) => {
        const { endpoint, ...patch } = req.body;
        states[endpoint].update(patch);
        return { ok: true, config: states[endpoint].config };
      },
    );

    admin.post('/admin/reset', async () => {
      states.layout.reset();
      states.vlm.reset();
      return { ok: true };
    });
  });

  return { app, states };
}

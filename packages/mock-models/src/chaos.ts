import { TokenBucket } from './limiter.js';
import { mulberry32, type Rng } from './rng.js';

export type Endpoint = 'layout' | 'vlm';

export interface EndpointConfig {
  rps: number;
  burst: number;
  latencyMinMs: number;
  latencyMaxMs: number;
  failureRate: number;
  hangRate: number;
  maxInFlight: number;
}

export interface Counters {
  total: number;
  ok: number;
  rateLimited: number;
  failed: number;
  shed: number;
  hung: number;
  inFlight: number;
}

// Fresh set of all-zero counters.
const zero = (): Counters => ({ total: 0, ok: 0, rateLimited: 0, failed: 0, shed: 0, hung: 0, inFlight: 0 });

// Builds an endpoint's config from env vars, falling back to the assignment spec values.
export function defaultConfig(env: NodeJS.ProcessEnv, kind: Endpoint): EndpointConfig {
  // Reads a numeric env var: default when unset, throws if it is not a number.
  const num = (k: string, d: number) => {
    const v = env[k];
    if (v === undefined || v === '') return d;
    const n = Number(v);
    if (!Number.isFinite(n)) throw new Error(`${k} must be a number, got "${v}"`);
    return n;
  };
  if (kind === 'layout') {
    const rps = num('LAYOUT_RPS', 100);
    return {
      rps,
      // Burst defaults to 200 ms worth of tokens so the limit is close to strict per second.
      burst: num('LAYOUT_BURST', Math.max(1, Math.ceil(rps / 5))),
      latencyMinMs: num('LAYOUT_LATENCY_MS', 50),
      latencyMaxMs: num('LAYOUT_LATENCY_MS', 50),
      failureRate: num('LAYOUT_FAILURE_RATE', 0.02),
      hangRate: num('LAYOUT_HANG_RATE', 0),
      maxInFlight: num('LAYOUT_MAX_INFLIGHT', 200),
    };
  }
  const rps = num('VLM_RPS', 10);
  return {
    rps,
    burst: num('VLM_BURST', Math.max(1, Math.ceil(rps / 5))),
    latencyMinMs: num('VLM_LATENCY_MIN_MS', 1500),
    latencyMaxMs: num('VLM_LATENCY_MAX_MS', 3000),
    failureRate: num('VLM_FAILURE_RATE', 0.05),
    hangRate: num('VLM_HANG_RATE', 0),
    maxInFlight: num('VLM_MAX_INFLIGHT', 100),
  };
}

/** Behaviour of one mock endpoint: config, rate limiter, seeded RNG and counters. */
export class EndpointState {
  readonly limiter: TokenBucket;
  readonly counters: Counters = zero();
  private readonly rng: Rng;

  // Sets up the token bucket and seeded RNG for one endpoint (layout or vlm).
  constructor(
    readonly kind: Endpoint,
    readonly config: EndpointConfig,
    seed: number,
  ) {
    this.limiter = new TokenBucket(config.rps, config.burst);
    this.rng = mulberry32(seed + (kind === 'layout' ? 1 : 2));
  }

  /** Draw one outcome so hang, failure and latency come from one deterministic stream. */
  draw(): { hang: boolean; fail: boolean; latencyMs: number } {
    const c = this.config;
    const hang = this.rng() < c.hangRate;
    const fail = this.rng() < c.failureRate;
    const latencyMs = c.latencyMinMs + this.rng() * (c.latencyMaxMs - c.latencyMinMs);
    return { hang, fail, latencyMs: Math.round(latencyMs) };
  }
}

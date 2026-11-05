import { TokenBucket, type Clock } from './limiter.js';
import { mulberry32, type Rng } from './rng.js';

export type Endpoint = 'layout' | 'vlm';
export type SpikeMode = '429' | 'slow';

export interface EndpointConfig {
  rps: number;
  burst: number;
  latencyMinMs: number;
  latencyMaxMs: number;
  failureRate: number;
  hangRate: number;
  maxInFlight: number;
}

export interface Spike {
  mode: SpikeMode;
  untilMs: number;
  /** Latency multiplier for mode "slow". */
  factor: number;
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

// Builds an endpoint's starting config from env vars, falling back to the assignment spec values.
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

/** Live behaviour of one mock endpoint: config, limiter, RNG, active spike, counters. */
export class EndpointState {
  config: EndpointConfig;
  limiter: TokenBucket;
  spike: Spike | null = null;
  counters: Counters = zero();
  private rng: Rng;

  // Sets up the config, token bucket and seeded RNG for one endpoint (layout or vlm).
  constructor(
    readonly kind: Endpoint,
    private readonly base: EndpointConfig,
    private readonly seed: number,
    private readonly now: Clock = () => performance.now(),
  ) {
    this.config = { ...base };
    this.limiter = new TokenBucket(base.rps, base.burst, now);
    this.rng = mulberry32(seed + (kind === 'layout' ? 1 : 2));
  }

  // Returns the current spike, first clearing it if its time window has passed.
  activeSpike(): Spike | null {
    if (this.spike && this.now() >= this.spike.untilMs) this.spike = null;
    return this.spike;
  }

  // Begins a spike (forced 429s or slowed responses) lasting durationMs.
  startSpike(mode: SpikeMode, durationMs: number, factor = 5): void {
    this.spike = { mode, untilMs: this.now() + durationMs, factor };
  }

  /** Draw one outcome so failure, hang and latency use one deterministic stream. */
  draw(): { hang: boolean; fail: boolean; latencyMs: number } {
    const c = this.config;
    const hang = this.rng() < c.hangRate;
    const fail = this.rng() < c.failureRate;
    let latencyMs = c.latencyMinMs + this.rng() * (c.latencyMaxMs - c.latencyMinMs);
    const s = this.activeSpike();
    if (s?.mode === 'slow') latencyMs *= s.factor;
    return { hang, fail, latencyMs: Math.round(latencyMs) };
  }

  // Applies a partial config change live, rebuilding the limiter if rate or burst changed.
  update(patch: Partial<EndpointConfig>): void {
    this.config = { ...this.config, ...patch };
    if (patch.rps !== undefined || patch.burst !== undefined) {
      this.limiter = new TokenBucket(this.config.rps, this.config.burst, this.now);
    }
  }

  // Restores default config and RNG seed; clears spikes and counters (keeps the in-flight gauge).
  reset(): void {
    this.config = { ...this.base };
    this.limiter = new TokenBucket(this.base.rps, this.base.burst, this.now);
    this.rng = mulberry32(this.seed + (this.kind === 'layout' ? 1 : 2));
    this.spike = null;
    // In-flight requests are still running; keep that gauge, clear the rest.
    this.counters = { ...zero(), inFlight: this.counters.inFlight };
  }
}

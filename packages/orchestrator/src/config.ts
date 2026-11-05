// The one place env vars are read. Every default comes from the assignment spec or plan.md.

export type LaneName = 'layout' | 'vlm';

export interface LaneConfig {
  /** Path on the model server. */
  path: string;
  /** Upper bound for the adaptive rate (the spec's limit), requests per second. */
  maxRps: number;
  /** Floor the adaptive rate never drops below. */
  minRps: number;
  /** In-flight cap from Little's law (L = λ·W); the hard memory bound. */
  maxInFlight: number;
  /** Per-request timeout. */
  timeoutMs: number;
  /** Real failures (5xx, timeout, network) allowed before the stage is exhausted. */
  maxAttempts: number;
  /** EWMA latency above this counts as congestion. */
  latencyCeilingMs: number;
}

export interface Config {
  port: number;
  dataDir: string;
  modelUrl: string;
  qpdfBin: string;
  maxUploadBytes: number;
  maxPages: number;
  window: number;
  splitConcurrency: number;
  maxQueuedPages: number;
  resultTtlMs: number;
  breakerThreshold: number;
  breakerOpenMs: number;
  degradedAfterMs: number;
  tedMaxSteps: number;
  lanes: Record<LaneName, LaneConfig>;
}

// Builds the config from env, falling back to spec values; throws on a non-numeric number.
export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  // Reads a numeric env var with a default.
  const num = (k: string, d: number): number => {
    const v = env[k];
    if (v === undefined || v === '') return d;
    const n = Number(v);
    if (!Number.isFinite(n)) throw new Error(`${k} must be a number, got "${v}"`);
    return n;
  };
  return {
    port: num('PORT', 3000),
    dataDir: env.DATA_DIR ?? './data',
    modelUrl: env.MODEL_URL ?? 'http://127.0.0.1:8080',
    qpdfBin: env.QPDF_BIN ?? 'qpdf',
    maxUploadBytes: num('MAX_UPLOAD_BYTES', 100 * 1024 * 1024),
    maxPages: 100,
    window: 32,
    splitConcurrency: 4,
    maxQueuedPages: num('MAX_QUEUED_PAGES', 6000),
    resultTtlMs: num('RESULT_TTL_MS', 60 * 60 * 1000),
    breakerThreshold: 5,
    breakerOpenMs: num('BREAKER_OPEN_MS', 5000),
    degradedAfterMs: num('DEGRADED_AFTER_MS', 30_000),
    tedMaxSteps: num('TED_MAX_STEPS', 5_000_000),
    lanes: {
      layout: {
        path: '/v1/predict/layout',
        maxRps: num('LAYOUT_RPS', 100),
        minRps: 5,
        maxInFlight: 16,
        timeoutMs: num('LAYOUT_TIMEOUT_MS', 1000),
        maxAttempts: 5,
        latencyCeilingMs: 250,
      },
      vlm: {
        path: '/v1/predict/vlm',
        maxRps: num('VLM_RPS', 10),
        minRps: 0.5,
        maxInFlight: 32,
        timeoutMs: num('VLM_TIMEOUT_MS', 5000),
        maxAttempts: 3,
        latencyCeilingMs: 4000,
      },
    },
  };
}

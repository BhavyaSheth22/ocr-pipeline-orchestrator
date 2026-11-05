import { readFileSync } from 'node:fs';
import client from 'prom-client';
import type { LaneName } from './config.js';
import type { Db } from './db.js';
import type { PipelineEvents } from './pipeline.js';
import type { Lane } from './lane.js';
import type { Notifier } from './stream.js';

const BREAKER_VALUE = { closed: 0, 'half-open': 1, open: 2 } as const;

// Reads the container's peak memory (includes qpdf children); undefined outside a cgroup v2 container.
function cgroupPeak(): number | undefined {
  try {
    return Number(readFileSync('/sys/fs/cgroup/memory.peak', 'utf8').trim());
  } catch {
    return undefined;
  }
}

// Builds the Prometheus registry. Gauges are computed on scrape from live state (no bookkeeping);
// counters are driven by pipeline events. Default metrics add RSS, heap and event-loop lag.
export function createMetrics(deps: { db: Db; notifier: Notifier; lanes: () => Record<LaneName, Lane> }) {
  const registry = new client.Registry();
  client.collectDefaultMetrics({ register: registry });
  const lanes = ['layout', 'vlm'] as const;

  // Declares a per-lane gauge whose value is read from a lane at scrape time.
  const laneGauge = (name: string, help: string, read: (l: Lane, n: LaneName) => number) =>
    new client.Gauge({
      name,
      help,
      labelNames: ['lane'],
      registers: [registry],
      // Sets one sample per lane on every scrape.
      collect() {
        for (const n of lanes) this.set({ lane: n }, read(deps.lanes()[n], n));
      },
    });

  laneGauge('queue_depth', 'Pages ready to dispatch', (_l, n) => deps.db.laneDepth(n).n);
  laneGauge('worker_lag_seconds', 'Age of the oldest ready page', (_l, n) => {
    const oldest = deps.db.laneDepth(n).oldest;
    return oldest === null ? 0 : Math.max(0, (Date.now() - oldest) / 1000);
  });
  laneGauge('inflight', 'Model requests in flight', (l) => l.inFlight);
  laneGauge('dispatch_rate', 'Current adaptive (AIMD) rate, requests/s', (l) => l.bucket.rate);
  laneGauge('breaker_state', 'Circuit breaker: 0 closed, 1 half-open, 2 open', (l) => BREAKER_VALUE[l.breaker.state()]);

  new client.Gauge({
    name: 'sse_clients',
    help: 'Open SSE connections',
    registers: [registry],
    // Reads the live connection count.
    collect() {
      this.set(deps.notifier.clients);
    },
  });
  new client.Gauge({
    name: 'container_memory_peak_bytes',
    help: 'cgroup memory.peak (container peak, includes child processes)',
    registers: [registry],
    // Reads cgroup memory.peak when running in a container.
    collect() {
      const v = cgroupPeak();
      if (v !== undefined) this.set(v);
    },
  });

  const responses = new client.Counter({ name: 'downstream_responses_total', help: 'Model responses by outcome code', labelNames: ['lane', 'code'], registers: [registry] });
  const rateLimited = new client.Counter({ name: 'rate_limited_retries_total', help: '429/503 requeues (no attempt spent)', labelNames: ['lane'], registers: [registry] });
  const retries = new client.Counter({ name: 'retries_total', help: 'Retries after real failures', labelNames: ['lane'], registers: [registry] });
  const fallbacks = new client.Counter({ name: 'fallbacks_total', help: 'Pages finalized from the layout result', registers: [registry] });
  const recovered = new client.Counter({ name: 'recovered_pages_total', help: 'Pages reset from running to ready on boot', registers: [registry] });

  const events: PipelineEvents = {
    // Counts a downstream response by status (or "timeout"/"network" when there was none).
    response(lane, o) {
      const code = o.kind === 'ok' ? '200' : 'status' in o && o.status ? String(o.status) : o.kind === 'failure' ? o.reason.split(':')[0]! : 'unknown';
      responses.inc({ lane, code });
    },
    // Counts a requeue, split by cause.
    retry(lane, kind) {
      (kind === 'rate_limited' ? rateLimited : retries).inc({ lane });
    },
    // Counts a layout fallback.
    fallback() {
      fallbacks.inc();
    },
  };

  return { registry, events, recovered };
}

export type Metrics = ReturnType<typeof createMetrics>;

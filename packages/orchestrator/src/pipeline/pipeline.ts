import { rm } from 'node:fs/promises';
import { CircuitBreaker } from './breaker.js';
import type { Config, LaneName } from '../config.js';
import type { Db, FinalPage, JobRow, Task } from '../storage/db.js';
import { paths, removeJobFiles } from '../storage/files.js';
import { type ExtractPage } from '../pdf/pdf.js';
import { Lane } from './lane.js';
import { log, withLogContext } from '../observability/log.js';
import { newSpanId, traceparent } from '../observability/trace.js';
import { callModel, type Outcome } from './model-client.js';
import { AdaptiveTokenBucket } from './token-bucket.js';

/** A node of the document tree (the shape /result returns and /evaluate takes). */
export interface TreeNode {
  type: string;
  text?: string;
  bbox?: [number, number, number, number];
  children?: TreeNode[];
}

interface LayoutBody {
  boxes?: { x: number; y: number; w: number; h: number; label: string }[];
  reading_order?: number[];
}
interface VlmBody {
  text?: string;
  tables?: { markdown: string }[];
}

/** Hooks for metrics; every field is optional so tests can pass nothing. */
export interface PipelineEvents {
  response?(lane: LaneName, outcome: Outcome): void;
  retry?(lane: LaneName, kind: 'rate_limited' | 'failure'): void;
  fallback?(): void;
}

// Builds a page node: layout blocks in reading order (type + bbox), then VLM text and tables.
export function pageNode(layout: LayoutBody | null, vlm: VlmBody | null): TreeNode {
  const children: TreeNode[] = [];
  const boxes = layout?.boxes ?? [];
  const order = layout?.reading_order ?? boxes.map((_, i) => i);
  for (const i of order) {
    const b = boxes[i];
    if (b) children.push({ type: b.label, bbox: [b.x, b.y, b.w, b.h] });
  }
  if (vlm?.text) children.push({ type: 'text', text: vlm.text });
  for (const t of vlm?.tables ?? []) children.push({ type: 'table', text: t.markdown });
  return { type: 'page', children };
}

// Full-jitter exponential backoff (AWS): random(0, min(8 s, base · 2^attempt)), base 250 ms by default.
export function backoffMs(attempt: number, baseMs = 250): number {
  return Math.random() * Math.min(8000, baseMs * 2 ** attempt);
}

export interface SchedulerDeps {
  cfg: Config;
  db: Db;
  extract: ExtractPage;
  /** Called after any commit that appended events for a job (wakes its SSE streams). */
  notify(jobId: string): void;
  events?: PipelineEvents;
}

// Creates both lanes and the page handlers that move pages through layout → VLM → done.
export function createScheduler({ cfg, db, extract, notify, events = {} }: SchedulerDeps) {
  // Late-bound so handlers can wake the other lane.
  const lanes = {} as Record<LaneName, Lane>;

  // After a page is final: delete its page file, or the whole job's files if that was the last page.
  async function afterFinal(job: JobRow, page: number, completed: boolean): Promise<void> {
    notify(job.id);
    if (completed) {
      log.info('job completed', { job_id: job.id });
      await removeJobFiles(cfg, job.id);
    } else {
      await rm(paths.page(cfg, job.id, page), { force: true });
    }
  }

  // Finishes a page from its stored layout (low confidence), or as failed if there is no layout either.
  async function fallback(job: JobRow, task: Task): Promise<void> {
    const layout = task.layout_json === null ? null : (JSON.parse(task.layout_json) as LayoutBody);
    const out: FinalPage = layout
      ? { status: 'ok', source: 'layout', low_confidence: true, result: pageNode(layout, null) }
      : { status: 'failed', source: null, low_confidence: true, result: pageNode(null, null) };
    if (layout) events.fallback?.();
    log.warn(layout ? 'page finalized from layout fallback' : 'page failed: both models exhausted');
    const r = db.finalize('vlm', job.id, task.page, out);
    if (r) await afterFinal(job, task.page, r.completed);
  }

  // The stage gave up (attempts exhausted): layout moves on to VLM without a layout; VLM falls back.
  async function exhausted(lane: LaneName, job: JobRow, task: Task): Promise<void> {
    if (lane === 'layout') {
      log.warn('layout exhausted, continuing with vlm only');
      if (db.advanceToVlm(job.id, task.page, null, null)) lanes.vlm.wake();
    } else {
      await fallback(job, task);
    }
  }

  // Applies one model outcome to the page's state.
  async function apply(lane: LaneName, job: JobRow, task: Task, outcome: Outcome): Promise<void> {
    const l = lanes[lane];
    events.response?.(lane, outcome);
    switch (outcome.kind) {
      case 'ok': {
        l.bucket.onSuccess(outcome.ms);
        l.breaker.onSuccess();
        if (lane === 'layout') {
          const layout = outcome.body as LayoutBody;
          if (db.advanceToVlm(job.id, task.page, layout, pageNode(layout, null))) {
            notify(job.id);
            lanes.vlm.wake();
          }
        } else {
          const layout = task.layout_json === null ? null : (JSON.parse(task.layout_json) as LayoutBody);
          const out: FinalPage = { status: 'ok', source: 'vlm', low_confidence: false, result: pageNode(layout, outcome.body as VlmBody) };
          const r = db.finalize('vlm', job.id, task.page, out);
          if (r) await afterFinal(job, task.page, r.completed);
        }
        return;
      }
      case 'backpressure':
        // Not a failure: no attempt spent. Pause, slow down, and requeue for after Retry-After.
        l.bucket.onCongestion(outcome.retryAfterMs);
        l.breaker.release();
        events.retry?.(lane, 'rate_limited');
        db.backoff(lane, job.id, task.page, Date.now() + outcome.retryAfterMs);
        return;
      case 'failure': {
        l.breaker.onFailure();
        log.warn('model call failed', { reason: outcome.reason, status: outcome.status });
        if (task.attempts + 1 >= l.cfg.maxAttempts) return exhausted(lane, job, task);
        events.retry?.(lane, 'failure');
        db.fail(lane, job.id, task.page, Date.now() + backoffMs(task.attempts + 1, cfg.backoffBaseMs));
        return;
      }
      case 'fatal': {
        // A 4xx other than 429 is our bug, not the model's health: record the page as failed.
        l.breaker.release();
        log.error('model rejected request', { status: outcome.status });
        const out: FinalPage = { status: 'failed', source: null, low_confidence: true, result: pageNode(null, null) };
        const r = db.finalize(lane, job.id, task.page, out);
        if (r) await afterFinal(job, task.page, r.completed);
      }
    }
  }

  // Handler for one lane: extract the page if needed, call the model, apply the outcome.
  function handler(lane: LaneName) {
    return {
      // Runs one claimed page under its log context (trace, job, page, lane, attempt).
      run: (task: Task) => {
        const job = db.getJob(task.job_id);
        if (!job) return Promise.resolve(); // purged while queued
        const ctx = { trace_id: job.trace_id, job_id: job.id, page: task.page, lane, attempt: task.attempts + 1 };
        return withLogContext(ctx, async () => {
          let imageRef = paths.page(cfg, job.id, task.page);
          if (lane === 'layout') {
            try {
              imageRef = await extract(job.id, job.pdf_path, task.page);
            } catch (err) {
              return apply(lane, job, task, { kind: 'failure', reason: String(err) });
            }
          }
          // span_id is this call's parent-id in the traceparent; the mock logs it too, pairing the two lines.
          const spanId = newSpanId();
          const start = performance.now();
          const outcome = await callModel(
            cfg.modelUrl + lanes[lane].cfg.path,
            { document_id: job.id, page: task.page, image_ref: imageRef },
            { idempotencyKey: task.call_key, traceparent: traceparent(job.trace_id, spanId), timeoutMs: lanes[lane].cfg.timeoutMs },
          );
          if (cfg.logRequests) {
            const status = outcome.kind === 'ok' ? 200 : outcome.status;
            log.info('model call', { span_id: spanId, outcome: outcome.kind, status, ms: Math.round(performance.now() - start) });
          }
          await apply(lane, job, task, outcome);
        });
      },
      // Degraded mode (VLM only): long outage, so finish from layout instead of waiting.
      degrade:
        lane === 'vlm'
          ? (task: Task) => {
              const job = db.getJob(task.job_id);
              return job ? withLogContext({ trace_id: job.trace_id, job_id: job.id, page: task.page, lane }, () => fallback(job, task)) : Promise.resolve();
            }
          : undefined,
    };
  }

  for (const name of ['layout', 'vlm'] as const) {
    const lc = cfg.lanes[name];
    lanes[name] = new Lane(
      name,
      lc,
      db,
      new AdaptiveTokenBucket(lc.minRps, lc.maxRps, lc.latencyCeilingMs),
      new CircuitBreaker(cfg.breakerThreshold, cfg.breakerOpenMs),
      handler(name),
      name === 'vlm' ? cfg.degradedAfterMs : Infinity,
    );
  }

  return {
    lanes,
    // Starts both dispatchers.
    start: () => Object.values(lanes).forEach((l) => l.start()),
    // Stops both and waits for in-flight calls.
    stop: () => Promise.all(Object.values(lanes).map((l) => l.stop())),
    // New job committed: layout has work.
    wake: () => lanes.layout.wake(),
    // Current adaptive VLM rate (pages/s), for ETAs and admission Retry-After.
    vlmRate: () => lanes.vlm.bucket.rate,
  };
}

export type SchedulerHandle = ReturnType<typeof createScheduler>;

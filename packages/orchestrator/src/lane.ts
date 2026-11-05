import { setTimeout as sleep } from 'node:timers/promises';
import type { CircuitBreaker } from './breaker.js';
import type { LaneConfig, LaneName } from './config.js';
import type { Db, Task } from './db.js';
import { log } from './log.js';
import type { AdaptiveTokenBucket } from './token-bucket.js';

export interface LaneHandler {
  /** Sends one claimed page to the model and records the outcome. */
  run(task: Task): Promise<void>;
  /** Degraded mode: finishes a claimed page without a model call. */
  degrade?(task: Task): Promise<void>;
}

const MAX_IDLE_MS = 1000; // safety net: an idle lane re-checks the queue at least this often

// One dispatcher loop per model ("bulkhead"). It pulls work from SQLite only when it can send it right away,
// so memory is O(in-flight), not O(queued): a waiting page is a row, not a closure or a timer.
export class Lane {
  inFlight = 0;
  private stopped = false;
  private dirty = false;
  private sleeper: AbortController | null = null;
  private loopDone: Promise<void> = Promise.resolve();
  private readonly running = new Set<Promise<void>>();

  // Wires the lane to its queue, pacing, breaker and handler.
  constructor(
    readonly name: LaneName,
    readonly cfg: LaneConfig,
    private readonly db: Db,
    readonly bucket: AdaptiveTokenBucket,
    readonly breaker: CircuitBreaker,
    private readonly handler: LaneHandler,
    private readonly degradedAfterMs = Infinity,
  ) {}

  /** Starts the dispatcher loop. */
  start(): void {
    this.loopDone = this.loop();
  }

  /** Something changed (new work, a freed slot): re-check now instead of finishing the current sleep. */
  wake(): void {
    this.dirty = true;
    this.sleeper?.abort();
  }

  /** Stops dispatching and waits for in-flight calls to finish (graceful shutdown). */
  async stop(): Promise<void> {
    this.stopped = true;
    this.wake();
    await this.loopDone;
    await Promise.allSettled([...this.running]);
  }

  // Sleeps up to ms, returning early on wake(). A wake that arrived since the last check skips the sleep.
  // The timer is cancelled through AbortController, so no sleeps pile up.
  private async idle(ms: number): Promise<void> {
    if (this.dirty || this.stopped) return;
    const ac = new AbortController();
    this.sleeper = ac;
    try {
      await sleep(Math.min(Math.max(ms, 1), MAX_IDLE_MS), undefined, { signal: ac.signal });
    } catch {
      // aborted by wake()
    } finally {
      this.sleeper = null;
    }
  }

  // Tracks a task promise so stop() can wait for it; frees the slot and wakes the loop when it settles.
  private dispatch(work: Promise<void>): void {
    this.inFlight++;
    // Logs anything the handler failed to record itself, then frees the slot (in finally, so it can't leak).
    const p = work
      .catch((err) => log.error('lane task crashed', { lane: this.name, err: String(err), stack: (err as Error)?.stack }))
      .finally(() => {
        this.inFlight--;
        this.running.delete(p);
        this.wake();
      });
    this.running.add(p);
  }

  // The dispatcher: in-flight bound → breaker → token bucket → claim → send (not awaited).
  private async loop(): Promise<void> {
    while (!this.stopped) {
      this.dirty = false;
      if (this.inFlight >= this.cfg.maxInFlight) {
        await this.idle(MAX_IDLE_MS);
        continue;
      }
      // Long outage on a lane that has a fallback: finish its pages without the model.
      if (this.handler.degrade && this.breaker.outageMs() > this.degradedAfterMs) {
        const task = this.db.claim(this.name);
        if (task) {
          this.dispatch(this.handler.degrade(task));
          continue;
        }
      }
      if (!this.breaker.tryAcquire()) {
        await this.idle(this.breaker.msUntilProbe());
        continue;
      }
      const wait = this.bucket.waitMs();
      if (wait > 0) {
        this.breaker.release();
        await this.idle(wait);
        continue;
      }
      const task = this.db.claim(this.name);
      if (!task) {
        this.breaker.release();
        const next = this.db.nextNotBefore(this.name);
        await this.idle(next === null ? MAX_IDLE_MS : next - Date.now());
        continue;
      }
      this.bucket.take();
      this.dispatch(this.handler.run(task));
    }
  }
}

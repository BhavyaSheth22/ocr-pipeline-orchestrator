// Client-side pacing for one model endpoint: a lazy-refill token bucket (burst 1, so dispatches are evenly
// spaced) whose rate adapts AIMD-style, like TCP congestion control. A 429/503 or a latency spike is the
// congestion signal: multiplicative decrease (at most once per window) plus a pause for Retry-After.
// Each success adds roughly maxRps/20 per second back. The clock is injectable so tests never sleep.

export type Clock = () => number;

const BETA = 0.7; // CUBIC's decrease factor
const DECREASE_WINDOW_MS = 1000; // a burst of 429s from one window is one congestion event
const EWMA_ALPHA = 0.2;

export class AdaptiveTokenBucket {
  rate: number;
  private tokens = 1;
  private last: number;
  private pausedUntil = 0;
  private lastDecrease = -Infinity;
  private ewmaMs: number | null = null;

  // Starts full at the maximum rate.
  constructor(
    readonly minRps: number,
    readonly maxRps: number,
    readonly latencyCeilingMs: number,
    private readonly now: Clock = () => performance.now(),
  ) {
    this.rate = maxRps;
    this.last = now();
  }

  // Adds the tokens earned since the last call at the current rate, capped at one (burst 1).
  private refill(): void {
    const t = this.now();
    this.tokens = Math.min(1, this.tokens + (Math.max(0, t - this.last) * this.rate) / 1000);
    this.last = t;
  }

  /** Milliseconds until a dispatch is allowed (0 = now), counting any Retry-After pause. */
  waitMs(): number {
    this.refill();
    const pause = Math.max(0, this.pausedUntil - this.now());
    const token = this.tokens >= 1 ? 0 : ((1 - this.tokens) / this.rate) * 1000;
    return Math.ceil(Math.max(pause, token));
  }

  /** Spends the token; call only when waitMs() returned 0. */
  take(): void {
    this.refill();
    this.tokens -= 1;
  }

  /** A successful response: track latency; additive increase unless latency is over the ceiling. */
  onSuccess(latencyMs: number): void {
    this.ewmaMs = this.ewmaMs === null ? latencyMs : EWMA_ALPHA * latencyMs + (1 - EWMA_ALPHA) * this.ewmaMs;
    if (this.ewmaMs > this.latencyCeilingMs) this.decrease();
    else this.rate = Math.min(this.maxRps, this.rate + this.maxRps / 20 / this.rate);
  }

  /** A 429/503: pause for Retry-After and back off. */
  onCongestion(retryAfterMs: number): void {
    this.pausedUntil = Math.max(this.pausedUntil, this.now() + retryAfterMs);
    this.decrease();
  }

  // Multiplicative decrease, at most once per window.
  private decrease(): void {
    const t = this.now();
    if (t - this.lastDecrease < DECREASE_WINDOW_MS) return;
    this.lastDecrease = t;
    this.rate = Math.max(this.minRps, this.rate * BETA);
  }
}

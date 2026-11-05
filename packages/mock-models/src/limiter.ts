/**
 * Token bucket. Tokens refill continuously at `ratePerSec` up to `capacity`.
 * Refill is lazy: we store the token count and the time of the last update and
 * compute the accumulated tokens on demand, so an idle bucket costs nothing.
 * The clock is injectable so tests never have to sleep.
 */
export type Clock = () => number; // milliseconds, monotonic

export interface TakeResult {
  ok: boolean;
  /** Milliseconds until one token is available (0 when ok). */
  retryAfterMs: number;
  remaining: number;
}

export class TokenBucket {
  private tokens: number;
  private last: number;

  // Creates a full bucket that refills at ratePerSec and holds at most capacity tokens.
  constructor(
    readonly ratePerSec: number,
    readonly capacity: number,
    private readonly now: Clock = () => performance.now(),
  ) {
    if (ratePerSec <= 0) throw new RangeError('ratePerSec must be > 0');
    if (capacity < 1) throw new RangeError('capacity must be >= 1');
    this.tokens = capacity;
    this.last = this.now();
  }

  // Adds the tokens earned since the last call, capped at capacity (lazy refill, no timer).
  private refill(): void {
    const t = this.now();
    const elapsedSec = Math.max(0, t - this.last) / 1000;
    this.tokens = Math.min(this.capacity, this.tokens + elapsedSec * this.ratePerSec);
    this.last = t;
  }

  // Spends cost tokens if available, otherwise reports how long until enough exist.
  tryTake(cost = 1): TakeResult {
    this.refill();
    if (this.tokens >= cost) {
      this.tokens -= cost;
      return { ok: true, retryAfterMs: 0, remaining: Math.floor(this.tokens) };
    }
    const missing = cost - this.tokens;
    return {
      ok: false,
      retryAfterMs: Math.ceil((missing / this.ratePerSec) * 1000),
      remaining: 0,
    };
  }
}

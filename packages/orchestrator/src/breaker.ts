import type { Clock } from './token-bucket.js';

export type BreakerState = 'closed' | 'open' | 'half-open';

// Per-lane circuit breaker. Opens after `threshold` consecutive real failures; while open the lane pauses
// (queued pages wait, no attempts are spent). After `openMs` one probe goes through: success closes it,
// failure reopens it. `outageMs` measures the whole outage, across reopenings, for degraded mode.
export class CircuitBreaker {
  private failures = 0;
  private openedAt: number | null = null;
  private outageSince: number | null = null;
  private probing = false;

  // Starts closed.
  constructor(
    readonly threshold: number,
    readonly openMs: number,
    private readonly now: Clock = () => performance.now(),
  ) {}

  /** Current state; open becomes half-open lazily once openMs has passed. */
  state(): BreakerState {
    if (this.openedAt === null) return 'closed';
    return this.now() - this.openedAt < this.openMs ? 'open' : 'half-open';
  }

  /** May a request go out? In half-open only one probe at a time. */
  tryAcquire(): boolean {
    const s = this.state();
    if (s === 'closed') return true;
    if (s === 'open' || this.probing) return false;
    this.probing = true;
    return true;
  }

  /** The acquired slot was not used, or ended with no health signal (429, 4xx). */
  release(): void {
    this.probing = false;
  }

  /** A healthy response closes the breaker. */
  onSuccess(): void {
    this.failures = 0;
    this.openedAt = null;
    this.outageSince = null;
    this.probing = false;
  }

  /** A real failure: count it, and open (or reopen after a failed probe). */
  onFailure(): void {
    const t = this.now();
    if (this.probing) {
      this.probing = false;
      this.openedAt = t;
      return;
    }
    this.failures++;
    if (this.openedAt === null && this.failures >= this.threshold) {
      this.openedAt = t;
      this.outageSince ??= t;
    }
  }

  /** Milliseconds until an open breaker allows a probe. */
  msUntilProbe(): number {
    return this.openedAt === null ? 0 : Math.max(0, this.openMs - (this.now() - this.openedAt));
  }

  /** How long the current outage has lasted (0 when closed). */
  outageMs(): number {
    return this.outageSince === null ? 0 : this.now() - this.outageSince;
  }
}

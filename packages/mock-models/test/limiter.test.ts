import { test } from 'node:test';
import assert from 'node:assert/strict';
import { TokenBucket } from '../src/limiter.js';

// Manual clock so tests control time instead of sleeping.
function fakeClock() {
  let t = 0;
  return { now: () => t, advance: (ms: number) => { t += ms; } };
}

test('allows a burst up to capacity then rejects', () => {
  const clk = fakeClock();
  const b = new TokenBucket(10, 3, clk.now);
  assert.equal(b.tryTake().ok, true);
  assert.equal(b.tryTake().ok, true);
  assert.equal(b.tryTake().ok, true);
  assert.equal(b.tryTake().ok, false);
});

test('retryAfterMs is the time until the next token', () => {
  const clk = fakeClock();
  const b = new TokenBucket(10, 1, clk.now); // 1 token per 100 ms
  b.tryTake();
  const r = b.tryTake();
  assert.equal(r.ok, false);
  assert.equal(r.retryAfterMs, 100);
  clk.advance(40);
  assert.equal(b.tryTake().retryAfterMs, 60);
});

test('refills over time and never exceeds capacity', () => {
  const clk = fakeClock();
  const b = new TokenBucket(10, 2, clk.now);
  b.tryTake(); b.tryTake();
  clk.advance(100);
  assert.equal(b.tryTake().ok, true);
  assert.equal(b.tryTake().ok, false);
  clk.advance(10_000); // long idle: still capped at 2
  assert.equal(b.tryTake().ok, true);
  assert.equal(b.tryTake().ok, true);
  assert.equal(b.tryTake().ok, false);
});

test('sustained rate over one second matches the configured rate', () => {
  const clk = fakeClock();
  const b = new TokenBucket(100, 20, clk.now);
  let ok = 0;
  for (let ms = 0; ms < 1000; ms++) { // one attempt per ms, 1000 attempts/s
    if (b.tryTake().ok) ok++;
    clk.advance(1);
  }
  assert.ok(ok >= 100 && ok <= 120, `got ${ok}`); // rate + initial burst
});

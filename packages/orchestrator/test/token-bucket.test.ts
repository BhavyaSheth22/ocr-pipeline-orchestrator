import { test } from 'node:test';
import assert from 'node:assert/strict';
import { AdaptiveTokenBucket } from '../src/pipeline/token-bucket.js';

// A bucket with a hand-driven clock.
function mk(min = 1, max = 10, ceiling = 1000) {
  const clock = { t: 0 };
  return { clock, b: new AdaptiveTokenBucket(min, max, ceiling, () => clock.t) };
}

test('burst 1 spaces dispatches evenly at the rate', () => {
  const { clock, b } = mk();
  assert.equal(b.waitMs(), 0);
  b.take();
  assert.equal(b.waitMs(), 100, '10 rps → 100 ms gap');
  clock.t = 60;
  assert.equal(b.waitMs(), 40);
  clock.t = 1000;
  b.take();
  assert.equal(b.waitMs(), 100, 'idle time does not build a burst');
});

test('congestion pauses for Retry-After and decreases by 0.7', () => {
  const { clock, b } = mk();
  b.onCongestion(2000);
  assert.equal(b.rate, 7);
  assert.equal(b.waitMs(), 2000);
  clock.t = 2000;
  assert.equal(b.waitMs(), 0);
});

test('at most one decrease per 1 s window', () => {
  const { clock, b } = mk();
  b.onCongestion(10);
  b.onCongestion(10);
  b.onCongestion(10);
  assert.equal(b.rate, 7);
  clock.t = 1000;
  b.onCongestion(10);
  assert.ok(Math.abs(b.rate - 4.9) < 1e-9);
});

test('rate never drops below the floor', () => {
  const { clock, b } = mk(2, 10);
  for (let i = 0; i < 20; i++) {
    clock.t += 1000;
    b.onCongestion(1);
  }
  assert.equal(b.rate, 2);
});

test('additive increase recovers about max/20 per second of successes', () => {
  const { b } = mk(1, 10);
  b.onCongestion(1); // 7
  // At rate r, one second holds about r successes, each adding (10/20)/r: +0.5 rps per second.
  for (let i = 0; i < 7; i++) b.onSuccess(10);
  assert.ok(b.rate > 7.45 && b.rate < 7.55, `rate ${b.rate}`);
  for (let i = 0; i < 1000; i++) b.onSuccess(10);
  assert.equal(b.rate, 10, 'capped at max');
});

test('EWMA latency over the ceiling counts as congestion', () => {
  const { b } = mk(1, 10, 100);
  b.onSuccess(50);
  assert.equal(b.rate, 10);
  for (let i = 0; i < 10; i++) b.onSuccess(1000);
  assert.equal(b.rate, 7, 'one decrease in the window');
});

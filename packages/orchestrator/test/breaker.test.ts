import { test } from 'node:test';
import assert from 'node:assert/strict';
import { CircuitBreaker } from '../src/breaker.js';

// A breaker (threshold 3, open 1 s) with a hand-driven clock.
function mk() {
  const clock = { t: 0 };
  return { clock, br: new CircuitBreaker(3, 1000, () => clock.t) };
}

test('opens after threshold consecutive failures; a success resets the count', () => {
  const { br } = mk();
  br.onFailure();
  br.onFailure();
  br.onSuccess();
  br.onFailure();
  br.onFailure();
  assert.equal(br.state(), 'closed');
  br.onFailure();
  assert.equal(br.state(), 'open');
  assert.equal(br.tryAcquire(), false);
});

test('half-open lets one probe through; success closes', () => {
  const { clock, br } = mk();
  for (let i = 0; i < 3; i++) br.onFailure();
  assert.equal(br.msUntilProbe(), 1000);
  clock.t = 1000;
  assert.equal(br.state(), 'half-open');
  assert.equal(br.tryAcquire(), true);
  assert.equal(br.tryAcquire(), false, 'only one probe');
  br.onSuccess();
  assert.equal(br.state(), 'closed');
  assert.equal(br.outageMs(), 0);
});

test('a failed probe reopens; outage time spans reopenings', () => {
  const { clock, br } = mk();
  for (let i = 0; i < 3; i++) br.onFailure();
  clock.t = 1000;
  br.tryAcquire();
  br.onFailure();
  assert.equal(br.state(), 'open');
  clock.t = 1500;
  assert.equal(br.outageMs(), 1500);
});

test('a released probe slot can be used again', () => {
  const { clock, br } = mk();
  for (let i = 0; i < 3; i++) br.onFailure();
  clock.t = 1000;
  assert.equal(br.tryAcquire(), true);
  br.release();
  assert.equal(br.tryAcquire(), true);
});

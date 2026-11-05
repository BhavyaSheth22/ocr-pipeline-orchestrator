import { test } from 'node:test';
import assert from 'node:assert/strict';
import { defaultConfig, EndpointState } from '../src/chaos.js';
import { layoutPayload, vlmPayload } from '../src/payloads.js';

test('seeded failure rate is close to configured rate and reproducible', () => {
  // Builds two identically seeded VLM states so their outcomes can be compared.
  const mk = () => new EndpointState('vlm', defaultConfig({}, 'vlm'), 7);
  const a = mk(); const b = mk();
  let fails = 0;
  for (let i = 0; i < 20000; i++) {
    const da = a.draw(); const db = b.draw();
    assert.deepEqual(da, db);
    if (da.fail) fails++;
  }
  assert.ok(Math.abs(fails / 20000 - 0.05) < 0.01, `rate ${fails / 20000}`);
});

test('latency stays inside the configured window', () => {
  const s = new EndpointState('vlm', defaultConfig({}, 'vlm'), 1);
  for (let i = 0; i < 5000; i++) {
    const { latencyMs } = s.draw();
    assert.ok(latencyMs >= 1500 && latencyMs <= 3000);
  }
});

test('spike expires and slow-mode multiplies latency', () => {
  let t = 0;
  const s = new EndpointState('layout', defaultConfig({}, 'layout'), 1, () => t);
  s.startSpike('slow', 1000, 4);
  assert.equal(s.draw().latencyMs, 200);
  t = 1000;
  assert.equal(s.activeSpike(), null);
  assert.equal(s.draw().latencyMs, 50);
});

test('payloads are deterministic per (document, page)', () => {
  assert.deepEqual(layoutPayload('doc', 3), layoutPayload('doc', 3));
  assert.deepEqual(vlmPayload('doc', 3), vlmPayload('doc', 3));
  assert.notDeepEqual(layoutPayload('doc', 3), layoutPayload('doc', 4));
});

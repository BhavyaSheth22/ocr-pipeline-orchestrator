import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildApp } from '../src/app.js';

const env = { ADMIN_TOKEN: 't', MOCK_SEED: '1', LAYOUT_FAILURE_RATE: '0', VLM_FAILURE_RATE: '0' };
const body = JSON.stringify({ document_id: 'd1', page: 1 });
// Helper: POST a fixed valid body to a predict endpoint in-process, no network needed.
const post = (app: ReturnType<typeof buildApp>['app'], url: string) =>
  app.inject({ method: 'POST', url, payload: body, headers: { 'content-type': 'application/json' } });

test('layout returns boxes and reading order', async () => {
  const { app } = buildApp({ env });
  const res = await post(app, '/v1/predict/layout');
  assert.equal(res.statusCode, 200);
  const j = res.json();
  assert.ok(j.boxes.length > 0);
  assert.equal(j.reading_order.length, j.boxes.length);
  await app.close();
});

test('exceeding the layout burst returns 429 with Retry-After', async () => {
  const { app } = buildApp({ env: { ...env, LAYOUT_RPS: '5', LAYOUT_BURST: '1', LAYOUT_LATENCY_MS: '0' } });
  assert.equal((await post(app, '/v1/predict/layout')).statusCode, 200);
  const res = await post(app, '/v1/predict/layout');
  assert.equal(res.statusCode, 429);
  assert.ok(Number(res.headers['retry-after']) >= 1);
  await app.close();
});

test('admin endpoints require the token', async () => {
  const { app } = buildApp({ env });
  assert.equal((await app.inject({ method: 'GET', url: '/admin/state' })).statusCode, 401);
  const ok = await app.inject({ method: 'GET', url: '/admin/state', headers: { 'x-admin-token': 't' } });
  assert.equal(ok.statusCode, 200);
  await app.close();
});

test('spike forces 429 on every request, then reset clears it', async () => {
  const { app } = buildApp({ env: { ...env, LAYOUT_LATENCY_MS: '0' } });
  const h = { 'x-admin-token': 't', 'content-type': 'application/json' };
  await app.inject({ method: 'POST', url: '/admin/spike', headers: h, payload: JSON.stringify({ endpoint: 'layout', durationMs: 60000, mode: '429' }) });
  assert.equal((await post(app, '/v1/predict/layout')).statusCode, 429);
  assert.equal((await post(app, '/v1/predict/layout')).statusCode, 429);
  await app.inject({ method: 'POST', url: '/admin/reset', headers: h, payload: '{}' });
  assert.equal((await post(app, '/v1/predict/layout')).statusCode, 200);
  await app.close();
});

test('invalid body is rejected with 400', async () => {
  const { app } = buildApp({ env });
  const res = await app.inject({ method: 'POST', url: '/v1/predict/vlm', payload: '{"page":0}', headers: { 'content-type': 'application/json' } });
  assert.equal(res.statusCode, 400);
  await app.close();
});

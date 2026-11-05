import { test } from 'node:test';
import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import { buildApp } from '../src/app.js';

const env = { MOCK_SEED: '1', LAYOUT_FAILURE_RATE: '0', VLM_FAILURE_RATE: '0' };
const JSON_H = { 'content-type': 'application/json' };

// Starts the app on a random free port, runs fn with its base URL, then shuts the server down.
async function withServer(e: NodeJS.ProcessEnv, fn: (url: string) => Promise<void>): Promise<void> {
  const { app } = buildApp({ env: e });
  const server = app.listen(0);
  const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  try {
    await fn(url);
  } finally {
    server.closeAllConnections();
    await new Promise((r) => server.close(r));
  }
}

// Helper: POST a fixed valid body to a predict endpoint over real HTTP.
const predict = (url: string, ep: string) =>
  fetch(`${url}/v1/predict/${ep}`, { method: 'POST', headers: JSON_H, body: JSON.stringify({ document_id: 'd1', page: 1 }) });

test('layout returns boxes and reading order', async () => {
  await withServer(env, async (url) => {
    const res = await predict(url, 'layout');
    assert.equal(res.status, 200);
    const j = (await res.json()) as { boxes: unknown[]; reading_order: number[] };
    assert.ok(j.boxes.length > 0);
    assert.equal(j.reading_order.length, j.boxes.length);
  });
});

test('vlm returns text, tables and key values', async () => {
  await withServer({ ...env, VLM_LATENCY_MIN_MS: '0', VLM_LATENCY_MAX_MS: '0' }, async (url) => {
    const res = await predict(url, 'vlm');
    assert.equal(res.status, 200);
    const j = (await res.json()) as { text: string; tables: { markdown: string }[]; key_values: object };
    assert.ok(j.text.length > 0 && j.tables[0]!.markdown.includes('|'));
    assert.ok(Object.keys(j.key_values).length > 0);
  });
});

test('exceeding the layout burst returns 429 with Retry-After', async () => {
  await withServer({ ...env, LAYOUT_RPS: '5', LAYOUT_BURST: '1', LAYOUT_LATENCY_MS: '0' }, async (url) => {
    assert.equal((await predict(url, 'layout')).status, 200);
    const res = await predict(url, 'layout');
    assert.equal(res.status, 429);
    assert.ok(Number(res.headers.get('retry-after')) >= 1);
    assert.equal(((await res.json()) as { error: string }).error, 'rate_limited');
  });
});

test('injected model failure returns 500', async () => {
  await withServer({ ...env, LAYOUT_FAILURE_RATE: '1', LAYOUT_LATENCY_MS: '0' }, async (url) => {
    const res = await predict(url, 'layout');
    assert.equal(res.status, 500);
    assert.equal(((await res.json()) as { error: string }).error, 'model_failure');
  });
});

test('concurrent requests beyond the burst get natural 429s', async () => {
  await withServer({ ...env, VLM_RPS: '2', VLM_BURST: '2', VLM_LATENCY_MIN_MS: '0', VLM_LATENCY_MAX_MS: '0' }, async (url) => {
    const codes = (await Promise.all([1, 2, 3, 4, 5].map(() => predict(url, 'vlm')))).map((r) => r.status).sort();
    assert.equal(codes.filter((c) => c === 429).length, 3);
    assert.equal(codes.filter((c) => c === 200).length, 2);
    const m = (await (await fetch(`${url}/metrics-lite`)).json()) as { vlm: { rateLimited: number } };
    assert.equal(m.vlm.rateLimited, 3);
  });
});

test('invalid bodies are rejected with 400', async () => {
  await withServer(env, async (url) => {
    const post = (body: string) => fetch(`${url}/v1/predict/vlm`, { method: 'POST', headers: JSON_H, body });
    assert.equal((await post('{"page":0}')).status, 400); // missing document_id, bad page
    assert.equal((await post('{"document_id":"d","page":1.5}')).status, 400); // not an integer
    assert.equal((await post('{"document_id":"d","page":1,"extra":true}')).status, 400); // unknown property
    assert.equal((await post('{not json')).status, 400); // malformed JSON
    const none = await fetch(`${url}/v1/predict/vlm`, { method: 'POST' }); // no body at all
    assert.equal(none.status, 400);
  });
});

test('metrics-lite counts outcomes and unknown routes get JSON 404', async () => {
  await withServer({ ...env, LAYOUT_LATENCY_MS: '0' }, async (url) => {
    await predict(url, 'layout');
    const m = (await (await fetch(`${url}/metrics-lite`)).json()) as { layout: { total: number; ok: number } };
    assert.equal(m.layout.total, 1);
    assert.equal(m.layout.ok, 1);
    const nf = await fetch(`${url}/nope`);
    assert.equal(nf.status, 404);
    assert.equal(((await nf.json()) as { error: string }).error, 'not_found');
  });
});

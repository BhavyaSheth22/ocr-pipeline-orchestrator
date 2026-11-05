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

// Helper: POST a fixed valid body with an Idempotency-Key (and any extra headers).
const predictKeyed = (url: string, ep: string, key: string, extra: Record<string, string> = {}) =>
  fetch(`${url}/v1/predict/${ep}`, {
    method: 'POST',
    headers: { ...JSON_H, 'idempotency-key': key, ...extra },
    body: JSON.stringify({ document_id: 'd1', page: 1 }),
  });

// Helper: read one endpoint's counters from /metrics-lite.
const counters = async (url: string, ep: 'layout' | 'vlm') =>
  ((await (await fetch(`${url}/metrics-lite`)).json()) as Record<string, Record<string, number>>)[ep]!;

test('a repeated Idempotency-Key replays the response without a model call or a token', async () => {
  // Burst 1: a second real call would get a 429, so a 200 proves the replay spent no token.
  await withServer({ ...env, LAYOUT_RPS: '1', LAYOUT_BURST: '1', LAYOUT_LATENCY_MS: '0' }, async (url) => {
    const first = await predictKeyed(url, 'layout', 'job1:1:layout:1');
    const again = await predictKeyed(url, 'layout', 'job1:1:layout:1');
    assert.equal(first.status, 200);
    assert.equal(again.status, 200);
    assert.deepEqual(await again.json(), await first.json());
    assert.equal(again.headers.get('x-mock-latency-ms'), first.headers.get('x-mock-latency-ms'));
    const c = await counters(url, 'layout');
    assert.equal(c.ok, 1);
    assert.equal(c.replayed, 1);
  });
});

test('a replay while the first call is in flight joins it instead of running again', async () => {
  await withServer({ ...env, VLM_LATENCY_MIN_MS: '200', VLM_LATENCY_MAX_MS: '200' }, async (url) => {
    const [a, b] = await Promise.all([predictKeyed(url, 'vlm', 'k'), predictKeyed(url, 'vlm', 'k')]);
    assert.equal(a.status, 200);
    assert.equal(b.status, 200);
    assert.deepEqual(await a.json(), await b.json());
    const c = await counters(url, 'vlm');
    assert.equal(c.ok, 1);
    assert.equal(c.replayed, 1);
  });
});

test('a failed call is replayed as the same 500', async () => {
  await withServer({ ...env, LAYOUT_FAILURE_RATE: '1', LAYOUT_LATENCY_MS: '0' }, async (url) => {
    assert.equal((await predictKeyed(url, 'layout', 'k')).status, 500);
    assert.equal((await predictKeyed(url, 'layout', 'k')).status, 500);
    const c = await counters(url, 'layout');
    assert.equal(c.failed, 1);
    assert.equal(c.replayed, 1);
  });
});

test('a rate-limited request is not remembered, so retrying the key runs the model', async () => {
  // 20 RPS with burst 1: one token every 50 ms.
  await withServer({ ...env, LAYOUT_RPS: '20', LAYOUT_BURST: '1', LAYOUT_LATENCY_MS: '0' }, async (url) => {
    assert.equal((await predict(url, 'layout')).status, 200); // spends the only token
    assert.equal((await predictKeyed(url, 'layout', 'k')).status, 429);
    await new Promise((r) => setTimeout(r, 80));
    assert.equal((await predictKeyed(url, 'layout', 'k')).status, 200);
    const c = await counters(url, 'layout');
    assert.equal(c.ok, 2);
    assert.equal(c.replayed, 0);
  });
});

test('the oldest key is evicted once REPLAY_MAX_KEYS is reached', async () => {
  await withServer({ ...env, LAYOUT_LATENCY_MS: '0', REPLAY_MAX_KEYS: '1' }, async (url) => {
    await predictKeyed(url, 'layout', 'k1');
    await predictKeyed(url, 'layout', 'k2'); // evicts k1
    await predictKeyed(url, 'layout', 'k1'); // runs again
    const c = await counters(url, 'layout');
    assert.equal(c.ok, 3);
    assert.equal(c.replayed, 0);
  });
});

test('an empty or oversized Idempotency-Key is rejected with 400', async () => {
  await withServer(env, async (url) => {
    assert.equal((await predictKeyed(url, 'layout', '')).status, 400);
    assert.equal((await predictKeyed(url, 'layout', 'x'.repeat(256))).status, 400);
  });
});

test('the access log carries the caller traceparent', async () => {
  const lines: string[] = [];
  const original = console.log;
  // Captures log lines instead of printing them, so the test can inspect the access log.
  console.log = (line: string) => void lines.push(line);
  try {
    await withServer({ ...env, LAYOUT_LATENCY_MS: '0', LOG_REQUESTS: '1' }, async (url) => {
      const tp = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';
      await (await predictKeyed(url, 'layout', 'k', { traceparent: tp })).json();
      // The log line is written on 'finish', which can land just after the client has the body.
      await new Promise((r) => setTimeout(r, 20));
      const entry = lines.map((l) => JSON.parse(l) as { url: string; traceparent?: string }).find((e) => e.url === '/v1/predict/layout');
      assert.equal(entry?.traceparent, tp);
    });
  } finally {
    console.log = original;
  }
});

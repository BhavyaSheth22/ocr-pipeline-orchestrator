import { test } from 'node:test';
import assert from 'node:assert/strict';
import { get } from 'node:http';
import { createServer, type AddressInfo } from 'node:net';
import { startSystem } from '../src/server.js';
import { cleanup, testConfig } from './helpers.js';

// No model server is needed: warm-up only logs when it is unreachable, and these tests make no model calls.
const offline = () => testConfig({ MODEL_URL: 'http://127.0.0.1:9' });

// GET /healthz on a fresh connection (agent: false), so every request needs its own accept.
const healthz = (port: number) =>
  new Promise<number>((resolve, reject) => {
    get({ host: '127.0.0.1', port, path: '/healthz', agent: false }, (res) => {
      res.resume();
      resolve(res.statusCode ?? 0);
    }).on('error', reject);
  });

test('a burst of new connections is accepted across the listening sockets and served', { timeout: 20_000 }, async () => {
  process.env.LOG_LEVEL = 'silent';
  const cfg = offline();
  const sys = await startSystem(cfg, 0);
  try {
    const codes = await Promise.all(Array.from({ length: 100 }, () => healthz(sys.port)));
    assert.deepEqual(new Set(codes), new Set([200]));
  } finally {
    await sys.stop();
    cleanup(cfg);
  }
});

test('a port held by another process still fails with EADDRINUSE (SO_REUSEPORT does not share it)', { timeout: 20_000 }, async () => {
  process.env.LOG_LEVEL = 'silent';
  const holder = createServer();
  await new Promise<void>((r) => holder.listen({ port: 0, host: '0.0.0.0' }, r));
  const cfg = offline();
  try {
    await assert.rejects(startSystem(cfg, (holder.address() as AddressInfo).port), { code: 'EADDRINUSE' });
  } finally {
    await new Promise((r) => holder.close(r));
    cleanup(cfg);
  }
});

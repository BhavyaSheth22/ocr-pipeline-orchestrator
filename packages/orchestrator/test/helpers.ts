import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { AddressInfo } from 'node:net';
import type { Express } from 'express';
import { loadConfig, type Config } from '../src/config.js';

// qpdf location for tests: QPDF_BIN if set, else qpdf on PATH.
export const QPDF = process.env.QPDF_BIN ?? 'qpdf';

// True when qpdf can run; tests that need it are skipped otherwise (it is a system dependency).
export const hasQpdf = (() => {
  try {
    execFileSync(QPDF, ['--version'], { stdio: 'ignore' });
    return true;
  } catch {
    return false;
  }
})();

// A config pointing at a fresh temp data dir, with test overrides.
export function testConfig(env: Record<string, string> = {}): Config {
  const dir = mkdtempSync(join(tmpdir(), 'orch-test-'));
  return loadConfig({ DATA_DIR: dir, QPDF_BIN: QPDF, ...env });
}

// Removes a test data dir.
export const cleanup = (cfg: Config) => rmSync(cfg.dataDir, { recursive: true, force: true });

// Listens on a random port, runs fn with the base URL, then closes every connection.
export async function withServer(app: Express, fn: (url: string) => Promise<void>): Promise<void> {
  const server = app.listen(0);
  await new Promise((r) => server.once('listening', r));
  const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  try {
    await fn(url);
  } finally {
    server.closeAllConnections();
    await new Promise((r) => server.close(r));
  }
}

// Starts the mock model server as a child process on a free port; resolves once it is listening.
export async function startMock(env: Record<string, string> = {}) {
  const { spawn } = await import('node:child_process');
  const { fileURLToPath } = await import('node:url');
  const script = fileURLToPath(new URL('../../mock-models/src/server.ts', import.meta.url));
  const port = 20000 + Math.floor(Math.random() * 20000);
  const child = spawn(process.execPath, ['--import', 'tsx', script], {
    env: { ...process.env, PORT: String(port), MOCK_SEED: '7', ...env },
    stdio: ['ignore', 'pipe', 'inherit'],
  });
  const url = `http://127.0.0.1:${port}`;
  // Polls /healthz until the mock answers (or gives up after ~10 s).
  for (let i = 0; i < 200; i++) {
    if (await fetch(`${url}/healthz`).then((r) => r.ok, () => false)) break;
    await new Promise((r) => setTimeout(r, 50));
  }
  const lines: string[] = [];
  child.stdout!.setEncoding('utf8').on('data', (d: string) => lines.push(...d.split('\n').filter(Boolean)));
  return {
    url,
    child,
    lines,
    // Reads the mock's counters.
    metrics: async () => (await (await fetch(`${url}/metrics-lite`)).json()) as Record<'layout' | 'vlm', Record<string, number>>,
    // Kills the mock and waits for it to exit.
    stop: async () => {
      if (child.exitCode !== null) return;
      child.kill('SIGKILL');
      await new Promise((r) => child.once('exit', r));
    },
  };
}

// Polls fn until it returns a truthy value or the timeout passes.
export async function waitFor<T>(fn: () => T | Promise<T>, timeoutMs = 10_000, stepMs = 25): Promise<T> {
  const end = Date.now() + timeoutMs;
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() > end) throw new Error('waitFor timed out');
    await new Promise((r) => setTimeout(r, stepMs));
  }
}

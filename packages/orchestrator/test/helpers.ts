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

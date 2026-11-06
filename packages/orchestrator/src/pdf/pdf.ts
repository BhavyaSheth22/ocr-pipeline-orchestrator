import { mkdir, rename, rm, stat } from 'node:fs/promises';
import type { Config } from '../config.js';
import { paths } from '../storage/files.js';
import { qpdf } from './qpdf-pool.js';
import { HttpError } from '../http/errors.js';

// qpdf exits 0 on success and 3 on success with warnings; anything else is a real error.
function qpdfOk(err: unknown): boolean {
  return (err as { code?: number }).code === 3;
}

// Reads the page count with qpdf (it seeks to the xref, so cost does not grow with file size). Runs on a
// qpdf worker thread, so the spawn does not block the event loop.
export async function countPages(cfg: Config, file: string): Promise<number> {
  let stdout: string;
  try {
    stdout = await qpdf.run(cfg.qpdfBin, ['--show-npages', file]);
  } catch (err) {
    if (!qpdfOk(err)) {
      const code = (err as { code?: unknown }).code;
      if (code === 'ENOENT') throw new Error(`qpdf not found at "${cfg.qpdfBin}"`);
      // No exit code means qpdf never ran (spawn or worker failure): a server error, not a bad PDF.
      if (typeof code !== 'number') throw err;
      throw new HttpError(422, 'not_a_pdf', 'qpdf could not read the file');
    }
    stdout = (err as { stdout: string }).stdout;
  }
  const n = Number.parseInt(stdout.trim(), 10);
  if (!Number.isInteger(n) || n < 1) throw new HttpError(422, 'not_a_pdf', 'no pages');
  if (n > cfg.maxPages) throw new HttpError(422, 'too_many_pages', `${n} pages, max ${cfg.maxPages}`);
  return n;
}

// Builds the page extractor: at most `splitConcurrency` qpdf processes at once, so child memory is bounded.
// The slot counter is main-thread state only; the qpdf process itself is started from a worker thread.
export function createSplitter(cfg: Config) {
  let active = 0;
  const waiting: (() => void)[] = [];

  // Waits for a free qpdf slot, runs fn, then hands the slot to the next waiter (always, via finally).
  async function withSlot<T>(fn: () => Promise<T>): Promise<T> {
    if (active >= cfg.splitConcurrency) await new Promise<void>((r) => waiting.push(r));
    else active++;
    try {
      return await fn();
    } finally {
      const next = waiting.shift();
      if (next) next();
      else active--;
    }
  }

  // Extracts page n into its own PDF (temp name then rename, so a crash never leaves a half file). Idempotent.
  return async function extractPage(jobId: string, src: string, n: number): Promise<string> {
    const dest = paths.page(cfg, jobId, n);
    if (await stat(dest).catch(() => null)) return dest;
    return withSlot(async () => {
      await mkdir(paths.pages(cfg, jobId), { recursive: true });
      const tmp = `${dest}.${process.pid}.${Math.random().toString(36).slice(2)}.tmp`;
      try {
        await qpdf.run(cfg.qpdfBin, [src, '--pages', '.', String(n), '--', tmp]);
      } catch (err) {
        if (!qpdfOk(err)) {
          await rm(tmp, { force: true });
          throw new Error(`qpdf failed to extract page ${n}: ${String((err as Error).message).slice(0, 200)}`);
        }
      }
      await rename(tmp, dest);
      return dest;
    });
  };
}

export type ExtractPage = ReturnType<typeof createSplitter>;

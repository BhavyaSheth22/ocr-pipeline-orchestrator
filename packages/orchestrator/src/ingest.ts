import { execFile } from 'node:child_process';
import { createWriteStream } from 'node:fs';
import { mkdir, readdir, rename, rm, stat } from 'node:fs/promises';
import { join } from 'node:path';
import { pipeline } from 'node:stream/promises';
import { promisify } from 'node:util';
import type { Readable } from 'node:stream';
import type { Config } from './config.js';

const run = promisify(execFile);

/** An error that maps straight to an HTTP response. */
export class HttpError extends Error {
  // Carries the status, a machine-readable code, optional response headers and extra body fields.
  constructor(
    readonly status: number,
    readonly code: string,
    message?: string,
    readonly headers: Record<string, string> = {},
    readonly details: Record<string, unknown> = {},
  ) {
    super(message ?? code);
  }
}

// Directory layout under DATA_DIR.
export const paths = {
  // Uploads land here first, so a crash mid-upload leaves only a temp file.
  tmp: (cfg: Config) => join(cfg.dataDir, 'tmp'),
  // Accepted source PDFs.
  job: (cfg: Config, id: string) => join(cfg.dataDir, 'jobs', `${id}.pdf`),
  // Per-job directory of extracted single-page PDFs.
  pages: (cfg: Config, id: string) => join(cfg.dataDir, 'pages', id),
  // One extracted page.
  page: (cfg: Config, id: string, n: number) => join(cfg.dataDir, 'pages', id, `${n}.pdf`),
};

// Creates the data directories (idempotent).
export async function ensureDirs(cfg: Config): Promise<void> {
  for (const d of ['tmp', 'jobs', 'pages']) await mkdir(join(cfg.dataDir, d), { recursive: true });
}

// Boot cleanup: half-written uploads and source files whose job row never committed are deleted.
export async function cleanOrphans(cfg: Config, jobExists: (id: string) => boolean): Promise<number> {
  let removed = 0;
  for (const f of await readdir(paths.tmp(cfg))) {
    await rm(join(paths.tmp(cfg), f), { force: true, recursive: true });
    removed++;
  }
  for (const f of await readdir(join(cfg.dataDir, 'jobs'))) {
    const id = f.replace(/\.pdf$/, '');
    if (!jobExists(id)) {
      await rm(join(cfg.dataDir, 'jobs', f), { force: true });
      await rm(paths.pages(cfg, id), { force: true, recursive: true });
      removed++;
    }
  }
  return removed;
}

// Deletes a job's files (after completion or retention).
export async function removeJobFiles(cfg: Config, id: string): Promise<void> {
  await rm(paths.job(cfg, id), { force: true });
  await rm(paths.pages(cfg, id), { force: true, recursive: true });
}

// Streams the request body to `dest` with a byte cap. Memory per upload is one pipeline buffer (~64 KB).
export async function receiveUpload(req: Readable & { headers: Record<string, unknown> }, dest: string, maxBytes: number): Promise<number> {
  const declared = Number(req.headers['content-length']);
  if (declared > maxBytes) throw new HttpError(413, 'payload_too_large', `max ${maxBytes} bytes`);
  let bytes = 0;
  // Counts bytes as they pass; throws past the cap. destroyOnReturn:false keeps the socket so a 413 can be sent.
  async function* capped(src: Readable): AsyncGenerator<Buffer> {
    for await (const chunk of src.iterator({ destroyOnReturn: false }) as AsyncIterable<Buffer>) {
      bytes += chunk.length;
      if (bytes > maxBytes) throw new HttpError(413, 'payload_too_large', `max ${maxBytes} bytes`);
      yield chunk;
    }
  }
  try {
    await pipeline(capped(req), createWriteStream(dest));
  } catch (err) {
    await rm(dest, { force: true });
    if (err instanceof HttpError) throw err;
    throw new HttpError(400, 'upload_failed', String(err));
  }
  if (bytes === 0) {
    await rm(dest, { force: true });
    throw new HttpError(422, 'not_a_pdf', 'empty body');
  }
  return bytes;
}

// qpdf exits 0 on success and 3 on success with warnings; anything else is a real error.
function qpdfOk(err: unknown): boolean {
  return (err as { code?: number }).code === 3;
}

// Reads the page count with qpdf (it seeks to the xref, so cost does not grow with file size).
export async function countPages(cfg: Config, file: string): Promise<number> {
  let stdout: string;
  try {
    ({ stdout } = await run(cfg.qpdfBin, ['--show-npages', file]));
  } catch (err) {
    if (!qpdfOk(err)) {
      if ((err as { code?: string }).code === 'ENOENT') throw new Error(`qpdf not found at "${cfg.qpdfBin}"`);
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
        await run(cfg.qpdfBin, [src, '--pages', '.', String(n), '--', tmp]);
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

import { mkdir, readdir, rm } from 'node:fs/promises';
import { join } from 'node:path';
import type { Config } from '../config.js';

// Directory layout under DATA_DIR.
export const paths = {
  // Source PDFs. Uploads stream straight here; a file without a job row is an orphan (see cleanOrphans).
  job: (cfg: Config, id: string) => join(cfg.dataDir, 'jobs', `${id}.pdf`),
  // Per-job directory of extracted single-page PDFs.
  pages: (cfg: Config, id: string) => join(cfg.dataDir, 'pages', id),
  // One extracted page.
  page: (cfg: Config, id: string, n: number) => join(cfg.dataDir, 'pages', id, `${n}.pdf`),
};

// Creates the data directories (idempotent).
export async function ensureDirs(cfg: Config): Promise<void> {
  for (const d of ['jobs', 'pages']) await mkdir(join(cfg.dataDir, d), { recursive: true });
}

// Boot cleanup: source files whose job row never committed (half-written or rejected mid-crash) are deleted.
export async function cleanOrphans(cfg: Config, jobExists: (id: string) => boolean): Promise<number> {
  let removed = 0;
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

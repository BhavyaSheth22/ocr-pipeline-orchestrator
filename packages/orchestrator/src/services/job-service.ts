import { randomBytes, randomUUID } from 'node:crypto';
import { rename, rm } from 'node:fs/promises';
import { join } from 'node:path';
import type { Readable } from 'node:stream';
import type { Config } from '../config.js';
import { HttpError } from '../http/errors.js';
import type { JobAccepted, JobResultResponse, PageFinalEventData, ResultPage } from '../http/types.js';
import { receiveUpload } from '../http/upload.js';
import { log } from '../observability/log.js';
import { countPages } from '../pdf/pdf.js';
import type { Db, JobRow } from '../storage/db.js';
import { paths } from '../storage/files.js';

/** What the job service needs from the scheduler. */
export interface Scheduler {
  /** A new job was committed: wake the layout lane. */
  wake(): void;
  /** Current adaptive VLM rate in pages/s, the throughput bottleneck used for ETAs. */
  vlmRate(): number;
}

export type UploadRequest = Readable & { headers: Record<string, unknown> };

function accepted(job: JobRow, backlogPages: number, etaS: number): JobAccepted {
  return { job_id: job.id, events_url: `/jobs/${job.id}/events`, total_pages: job.total_pages, backlog_pages: backlogPages, eta_s: etaS };
}

// Job use-cases (ingestion and result assembly), kept free of Express so routes stay thin.
export function createJobService({ cfg, db, scheduler }: { cfg: Config; db: Db; scheduler: Scheduler }) {
  // Seconds to drain `pages` at the current VLM rate.
  const etaFor = (pages: number) => Math.ceil(pages / Math.max(scheduler.vlmRate(), 0.1));

  return {
    // Ingestion: admission check before reading the body, stream to disk, count pages, commit in one transaction.
    async submit(req: UploadRequest, idemKey: string | null): Promise<JobAccepted> {
      if (idemKey !== null && (idemKey.length === 0 || idemKey.length > 255)) {
        throw new HttpError(400, 'bad_request', 'Idempotency-Key must be 1-255 characters');
      }
      // A retried upload gets the original job back; its body is never read.
      const existing = idemKey === null ? undefined : db.jobByIdemKey(idemKey);
      if (existing) {
        const backlog = db.backlogPages();
        return accepted(existing, backlog, etaFor(backlog));
      }

      // Front-door backpressure: when the backlog is full, tell the client when there will be room.
      const backlogBefore = db.backlogPages();
      if (backlogBefore >= cfg.maxQueuedPages) {
        const base = (backlogBefore - cfg.maxQueuedPages + cfg.maxPages) / Math.max(scheduler.vlmRate(), 0.1);
        const retryAfter = Math.min(600, Math.max(1, Math.ceil(base * (1 + 0.2 * Math.random()))));
        throw new HttpError(503, 'backlog_full', 'queue is full, retry later', { 'Retry-After': String(retryAfter) });
      }

      const id = randomUUID();
      const tmp = join(paths.tmp(cfg), id);
      await receiveUpload(req, tmp, cfg.maxUploadBytes);
      let pages: number;
      try {
        pages = await countPages(cfg, tmp);
      } catch (err) {
        await rm(tmp, { force: true });
        throw err;
      }
      const pdfPath = paths.job(cfg, id);
      await rename(tmp, pdfPath);

      const backlog = db.backlogPages() + pages;
      const etaS = etaFor(backlog);
      try {
        db.createJob({
          id,
          idemKey,
          traceId: randomBytes(16).toString('hex'),
          pdfPath,
          totalPages: pages,
          window: cfg.window,
          header: { backlog_pages: backlog, eta_s: etaS },
        });
      } catch (err) {
        await rm(pdfPath, { force: true });
        // Two concurrent uploads with one Idempotency-Key: the loser returns the winner's job.
        const winner = idemKey === null ? undefined : db.jobByIdemKey(idemKey);
        if (winner) return accepted(winner, backlog, etaS);
        throw err;
      }
      const job = db.getJob(id)!;
      log.info('job accepted', { job_id: id, trace_id: job.trace_id, pages, backlog_pages: backlog });
      scheduler.wake();
      return accepted(job, backlog, etaS);
    },

    // The assembled document tree, once every page is final.
    result(id: string): JobResultResponse {
      const job = db.getJob(id);
      if (!job) throw new HttpError(404, 'not_found');
      if (job.state !== 'done') throw new HttpError(409, 'not_ready', `job is still running (${job.watermark - 1}/${job.total_pages} pages in order)`);
      const children = db.finals(job.id).map((r): ResultPage => {
        const d = JSON.parse(r.data) as PageFinalEventData;
        return { ...d.result, page: r.page, status: d.status, source: d.source, low_confidence: d.low_confidence };
      });
      return { type: 'document', job_id: job.id, children };
    },
  };
}

export type JobService = ReturnType<typeof createJobService>;

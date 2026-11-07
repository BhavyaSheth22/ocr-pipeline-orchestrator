import Database from 'better-sqlite3';
import type { LaneName } from '../config.js';

// SQLite is the queue, the result store and the SSE event log. Every state change, its result and its
// event commit in one transaction, guarded by the state the caller expects, so a stale outcome is a no-op.
//
// Several processes may open the same file (the pipeline process and the HTTP workers; WAL lets readers
// and one writer work at once). Multi-statement writes therefore use BEGIN IMMEDIATE: a deferred transaction
// that reads first and then writes fails with SQLITE_BUSY instead of waiting when another process committed
// in between. IMMEDIATE takes the write lock up front, so the busy timeout (5 s) applies and it just waits.

const SCHEMA = `
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY, seq INTEGER NOT NULL UNIQUE, idem_key TEXT UNIQUE, trace_id TEXT NOT NULL,
  pdf_path TEXT NOT NULL, total_pages INTEGER NOT NULL, window INTEGER NOT NULL,
  watermark INTEGER NOT NULL DEFAULT 1, next_event_seq INTEGER NOT NULL DEFAULT 1,
  state TEXT NOT NULL CHECK (state IN ('running','done')), created_at INTEGER NOT NULL);

CREATE TABLE IF NOT EXISTS pages (
  job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE, page INTEGER NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('layout_ready','layout_running','vlm_ready','vlm_running','done')),
  attempts INTEGER NOT NULL DEFAULT 0,
  not_before INTEGER NOT NULL DEFAULT 0, ready_at INTEGER NOT NULL,
  call_key TEXT, layout_json TEXT,
  PRIMARY KEY (job_id, page)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS pages_claim ON pages (state, page, not_before);

CREATE TABLE IF NOT EXISTS events (
  job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE, seq INTEGER NOT NULL,
  type TEXT NOT NULL, page INTEGER, data TEXT NOT NULL,
  PRIMARY KEY (job_id, seq)) WITHOUT ROWID;
`;

export interface JobRow {
  id: string;
  seq: number;
  idem_key: string | null;
  trace_id: string;
  pdf_path: string;
  total_pages: number;
  window: number;
  watermark: number;
  next_event_seq: number;
  state: 'running' | 'done';
  created_at: number;
}

export interface Task {
  job_id: string;
  page: number;
  state: string;
  attempts: number;
  not_before: number;
  ready_at: number;
  call_key: string;
  layout_json: string | null;
}

export interface EventRow {
  seq: number;
  type: string;
  page: number | null;
  data: string;
}

export interface NewJob {
  id: string;
  idemKey: string | null;
  traceId: string;
  pdfPath: string;
  totalPages: number;
  window: number;
  /** Extra fields for the `job` header event (backlog_pages, eta_s). */
  header: Record<string, unknown>;
}

/** What a finished page looks like in its `final` event. */
export interface FinalPage {
  status: 'ok' | 'failed';
  source: 'vlm' | 'layout' | null;
  low_confidence: boolean;
  result: unknown;
}

export type Db = ReturnType<typeof openDb>;

// Opens (or creates) the database, applies pragmas and schema, and returns the typed operations.
export function openDb(path: string) {
  const db = new Database(path);
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma('cache_size = -16000');
  db.pragma('foreign_keys = ON');
  db.exec(SCHEMA);

  const s = {
    insertJob: db.prepare(`INSERT INTO jobs (id, seq, idem_key, trace_id, pdf_path, total_pages, window, state, created_at)
      VALUES (@id, (SELECT COALESCE(MAX(seq), 0) + 1 FROM jobs), @idemKey, @traceId, @pdfPath, @totalPages, @window, 'running', @now)`),
    insertPage: db.prepare(`INSERT INTO pages (job_id, page, state, ready_at) VALUES (?, ?, 'layout_ready', ?)`),
    nextSeq: db.prepare(`UPDATE jobs SET next_event_seq = next_event_seq + 1 WHERE id = ? RETURNING next_event_seq - 1 AS seq`),
    insertEvent: db.prepare(`INSERT INTO events (job_id, seq, type, page, data) VALUES (?, ?, ?, ?, ?)`),
    jobById: db.prepare(`SELECT * FROM jobs WHERE id = ?`),
    jobByIdem: db.prepare(`SELECT * FROM jobs WHERE idem_key = ?`),
    // IN, not != 'done': it counts only unfinished pages through the pages_claim index instead of scanning
    // every page kept for retention.
    backlog: db.prepare(`SELECT COUNT(*) AS n FROM pages
      WHERE state IN ('layout_ready', 'layout_running', 'vlm_ready', 'vlm_running')`),
    // One statement claims the next page, so there is no read-then-write race. Page index first, then job
    // arrival order: every job's page 1 goes before anyone's page 2. Only pages inside the job's window qualify.
    claim: db.prepare(`UPDATE pages SET state = @running,
        call_key = COALESCE(call_key, job_id || ':' || page || ':' || @lane || ':' || (attempts + 1))
      WHERE (job_id, page) = (
        SELECT p.job_id, p.page FROM pages p JOIN jobs j ON j.id = p.job_id
        WHERE p.state = @ready AND p.not_before <= @now AND p.page < j.watermark + j.window
        ORDER BY p.page, j.seq LIMIT 1)
      RETURNING *`),
    nextNotBefore: db.prepare(`SELECT MIN(p.not_before) AS t FROM pages p JOIN jobs j ON j.id = p.job_id
      WHERE p.state = ? AND p.page < j.watermark + j.window`),
    // A 429/503 or a released claim: back to ready, attempts and call_key untouched (the mock stored nothing).
    backoff: db.prepare(`UPDATE pages SET state = @ready, not_before = @notBefore, ready_at = @now
      WHERE job_id = @job AND page = @page AND state = @running`),
    // A real failure: one attempt spent, and a new idempotency key for the next try.
    fail: db.prepare(`UPDATE pages SET state = @ready, not_before = @notBefore, ready_at = @now,
        attempts = attempts + 1, call_key = NULL
      WHERE job_id = @job AND page = @page AND state = @running RETURNING attempts`),
    toVlm: db.prepare(`UPDATE pages SET state = 'vlm_ready', attempts = 0, call_key = NULL, not_before = 0,
        ready_at = @now, layout_json = @layout
      WHERE job_id = @job AND page = @page AND state = 'layout_running'`),
    toDone: db.prepare(`UPDATE pages SET state = 'done', call_key = NULL, layout_json = NULL
      WHERE job_id = @job AND page = @page AND state = @from`),
    watermark: db.prepare(`UPDATE jobs SET watermark =
        (SELECT COALESCE(MIN(page), jobs.total_pages + 1) FROM pages WHERE job_id = jobs.id AND state != 'done')
      WHERE id = ? RETURNING watermark, total_pages`),
    completeJob: db.prepare(`UPDATE jobs SET state = 'done' WHERE id = ?`),
    finalStats: db.prepare(`SELECT
        SUM(json_extract(data, '$.status') = 'failed') AS failed,
        SUM(json_extract(data, '$.low_confidence') = 1) AS low_confidence
      FROM events WHERE job_id = ? AND type = 'page' AND json_extract(data, '$.stage') = 'final'`),
    recover: db.prepare(`UPDATE pages SET state = REPLACE(state, '_running', '_ready'), not_before = 0
      WHERE state IN ('layout_running', 'vlm_running')`),
    events: db.prepare(`SELECT seq, type, page, data FROM events WHERE job_id = ? AND seq > ? ORDER BY seq LIMIT ?`),
    finals: db.prepare(`SELECT page, data FROM events
      WHERE job_id = ? AND type = 'page' AND json_extract(data, '$.stage') = 'final' ORDER BY page`),
    expired: db.prepare(`SELECT id FROM jobs WHERE created_at < ? AND state = 'done'`),
    deleteJob: db.prepare(`DELETE FROM jobs WHERE id = ?`),
    laneDepth: db.prepare(`SELECT COUNT(*) AS n, MIN(ready_at) AS oldest FROM pages WHERE state = ?`),
  };

  // Allocates the job's next gap-free event seq and appends the event; must run inside a transaction.
  function appendEvent(jobId: string, type: string, page: number | null, payload: Record<string, unknown>): number {
    const { seq } = s.nextSeq.get(jobId) as { seq: number };
    s.insertEvent.run(jobId, seq, type, page, JSON.stringify({ seq, ...payload }));
    return seq;
  }

  // A write transaction that takes the write lock at BEGIN (see the header).
  const tx = <A extends unknown[], R>(fn: (...args: A) => R) => db.transaction(fn).immediate;

  return {
    raw: db,

    // Inserts the job, its N pages and the `job` header event atomically.
    createJob: tx((j: NewJob, now: number = Date.now()) => {
      s.insertJob.run({ id: j.id, idemKey: j.idemKey, traceId: j.traceId, pdfPath: j.pdfPath, totalPages: j.totalPages, window: j.window, now });
      for (let p = 1; p <= j.totalPages; p++) s.insertPage.run(j.id, p, now);
      appendEvent(j.id, 'job', null, { job_id: j.id, total_pages: j.totalPages, window: j.window, ...j.header });
    }),

    // Looks up a job by id.
    getJob: (id: string) => s.jobById.get(id) as JobRow | undefined,

    // Looks up the job an ingestion Idempotency-Key created, if any.
    jobByIdemKey: (key: string) => s.jobByIdem.get(key) as JobRow | undefined,

    // Pages not yet done, across all jobs (the admission signal).
    backlogPages: () => (s.backlog.get() as { n: number }).n,

    // Atomically moves the next eligible page of this lane to running and returns it.
    claim: (lane: LaneName, now: number = Date.now()) =>
      s.claim.get({ lane, ready: `${lane}_ready`, running: `${lane}_running`, now }) as Task | undefined,

    // Earliest not_before among this lane's ready pages inside their windows, so an idle lane knows when to wake.
    nextNotBefore: (lane: LaneName) => (s.nextNotBefore.get(`${lane}_ready`) as { t: number | null }).t,

    // Returns a page to ready without spending an attempt (backpressure, or a claim the lane could not send).
    backoff: (lane: LaneName, job: string, page: number, notBefore: number, now: number = Date.now()) =>
      s.backoff.run({ ready: `${lane}_ready`, running: `${lane}_running`, job, page, notBefore, now }).changes > 0,

    // Records a real failure; returns attempts used so far, or undefined if the outcome was stale.
    fail: (lane: LaneName, job: string, page: number, notBefore: number, now: number = Date.now()) =>
      (s.fail.get({ ready: `${lane}_ready`, running: `${lane}_running`, job, page, notBefore, now }) as { attempts: number } | undefined)?.attempts,

    // Layout finished (or was exhausted when layout is null): page moves to the VLM lane, partial event appended.
    advanceToVlm: tx((job: string, page: number, layout: unknown | null, partial: unknown, now: number = Date.now()): boolean => {
      const ok = s.toVlm.run({ job, page, now, layout: layout === null ? null : JSON.stringify(layout) }).changes > 0;
      if (ok && layout !== null) {
        const { watermark } = s.jobById.get(job) as JobRow;
        appendEvent(job, 'page', page, { page, stage: 'layout', watermark, result: partial });
      }
      return ok;
    }),

    // Marks the page done, appends its final event, moves the watermark and completes the job on its last page.
    // Returns undefined for a stale outcome, else whether the job just completed.
    finalize: tx((from: LaneName, job: string, page: number, out: FinalPage): { completed: boolean } | undefined => {
      if (s.toDone.run({ job, page, from: `${from}_running` }).changes === 0) return undefined;
      const { watermark, total_pages } = s.watermark.get(job) as { watermark: number; total_pages: number };
      appendEvent(job, 'page', page, { page, stage: 'final', watermark, ...out });
      if (watermark <= total_pages) return { completed: false };
      s.completeJob.run(job);
      const stats = s.finalStats.get(job) as { failed: number | null; low_confidence: number | null };
      appendEvent(job, 'job.completed', null, {
        job_id: job,
        total_pages,
        failed_pages: stats.failed ?? 0,
        low_confidence_pages: stats.low_confidence ?? 0,
      });
      return { completed: true };
    }),

    // Boot recovery: everything running belonged to the dead process. call_key is kept so a resend replays.
    recover: () => s.recover.run().changes,

    // Events after a cursor, in order (the SSE replay and live path).
    events: (job: string, afterSeq: number, limit = 100) => s.events.all(job, afterSeq, limit) as EventRow[],

    // Final page events in page order, for GET /jobs/:id/result.
    finals: (job: string) => s.finals.all(job) as { page: number; data: string }[],

    // Deletes finished jobs created before the cutoff (cascades to pages and events); returns their ids.
    purge: tx((before: number): string[] => {
      const ids = (s.expired.all(before) as { id: string }[]).map((r) => r.id);
      for (const id of ids) s.deleteJob.run(id);
      return ids;
    }),

    // Ready-page count and oldest ready_at for one lane (queue depth and worker lag metrics).
    laneDepth: (lane: LaneName) => s.laneDepth.get(`${lane}_ready`) as { n: number; oldest: number | null },

    // Closes the database (shutdown).
    close: () => db.close(),
  };
}

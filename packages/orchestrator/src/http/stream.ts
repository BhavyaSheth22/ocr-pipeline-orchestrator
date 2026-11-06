import { once } from 'node:events';
import type { Request, Response } from 'express';
import type { Db } from '../storage/db.js';

// SSE fan-out with O(1) memory per client: the event log lives in SQLite and each connection keeps only a
// cursor (like a Kafka consumer offset). Replay after reconnect and live delivery are the same loop.

const PAGE_SIZE = 100;
const DRAIN_TIMEOUT_MS = 30_000; // a reader this slow is dropped; it resumes with Last-Event-ID
const PING_MS = 15_000;

// Wakes streams waiting on a job when new events are committed. One notify resolves every current waiter,
// so a burst of commits coalesces into one re-read per stream.
export function createNotifier() {
  const waiters = new Map<string, Set<() => void>>();
  let clients = 0;
  return {
    // Wakes every stream waiting on this job.
    notify(jobId: string): void {
      const set = waiters.get(jobId);
      if (!set) return;
      waiters.delete(jobId);
      for (const done of set) done();
    },
    // Resolves on the next notify for this job, or when the signal aborts (client gone).
    wait(jobId: string, signal: AbortSignal): Promise<void> {
      return new Promise((resolve) => {
        if (signal.aborted) return resolve();
        let set = waiters.get(jobId);
        if (!set) waiters.set(jobId, (set = new Set()));
        const s = set;
        // Removes this waiter (and its abort listener) and resolves.
        const done = () => {
          s.delete(done);
          if (s.size === 0 && waiters.get(jobId) === s) waiters.delete(jobId);
          signal.removeEventListener('abort', done);
          resolve();
        };
        s.add(done);
        signal.addEventListener('abort', done, { once: true });
      });
    },
    /** Open SSE connections (for metrics). */
    get clients() {
      return clients;
    },
    // Tracks connection count.
    track(delta: 1 | -1): void {
      clients += delta;
    },
  };
}

export type Notifier = ReturnType<typeof createNotifier>;

// Formats one log row as an SSE frame. The id is the per-job seq, so Last-Event-ID resumes exactly here.
const frame = (r: { seq: number; type: string; data: string }) => `id: ${r.seq}\nevent: ${r.type}\ndata: ${r.data}\n\n`;

// GET /jobs/:id/events. Streams from the cursor until job.completed, waiting for notifies when caught up.
export async function streamEvents(req: Request, res: Response, db: Db, notifier: Notifier): Promise<void> {
  const jobId = String(req.params.id);
  if (!db.getJob(jobId)) {
    res.status(404).json({ error: 'not_found' });
    return;
  }
  let cursor = Number(req.get('Last-Event-ID') ?? req.query.last_event_id ?? 0);
  if (!Number.isInteger(cursor) || cursor < 0) cursor = 0;

  res.writeHead(200, {
    'content-type': 'text/event-stream; charset=utf-8',
    'cache-control': 'no-cache',
    connection: 'keep-alive',
    'x-accel-buffering': 'no',
  });
  res.flushHeaders();
  res.write('retry: 1000\n\n');

  const ac = new AbortController();
  // Client went away: stop the loop and release its waiter.
  res.once('close', () => ac.abort());
  // Keep-alive comment. No id: an id here would overwrite the client's resume cursor.
  const ping = setInterval(() => res.write(': ping\n\n'), PING_MS);
  notifier.track(1);
  try {
    while (!ac.signal.aborted) {
      const rows = db.events(jobId, cursor, PAGE_SIZE);
      for (const row of rows) {
        if (!res.write(frame(row))) {
          // Backpressure: wait for the socket to drain instead of buffering; drop a reader that never drains.
          try {
            await once(res, 'drain', { signal: AbortSignal.any([ac.signal, AbortSignal.timeout(DRAIN_TIMEOUT_MS)]) });
          } catch {
            res.end();
            return;
          }
        }
        cursor = row.seq;
        if (row.type === 'job.completed') {
          res.end();
          return;
        }
      }
      if (rows.length < PAGE_SIZE) {
        // Caught up. Registering the waiter is synchronous after the read, so no notify can slip between.
        if (!db.getJob(jobId)) break; // purged
        await notifier.wait(jobId, ac.signal);
      }
    }
    res.end();
  } finally {
    clearInterval(ping);
    notifier.track(-1);
  }
}

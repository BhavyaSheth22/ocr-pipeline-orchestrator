// A small pool of worker threads that run qpdf. Spawning a child process costs ~3 ms of synchronous work on
// the calling thread (measured: 80 spawns took 231 ms of the event loop in a 50-upload burst), so the spawns
// happen on worker threads and the main thread stays free for HTTP, SSE and dispatch.
//
// Thread safety:
// - Workers share no memory with the main thread. Tasks and replies are plain objects copied by
//   postMessage (structured clone); there is no SharedArrayBuffer and no state touched by two threads.
// - The pending-task maps live only on the main thread, which runs one callback at a time, so they need
//   no locks. Each reply carries its task id and settles exactly that task's promise, once.
// - The SQLite connection never leaves the main thread (a better-sqlite3 connection must not be shared
//   across threads); workers only run qpdf.
// - A worker that crashes or exits rejects every task it held, so no caller waits forever. The page goes
//   through the normal retry path, and the next task starts a fresh worker in that slot.
import { extname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Worker } from 'node:worker_threads';
import { log } from '../observability/log.js';

export interface QpdfTask {
  id: number;
  bin: string;
  args: string[];
}
export type QpdfReply =
  | { id: number; ok: true; stdout: string }
  | { id: number; ok: false; code: number | string | null; stdout: string; message: string };

/** Shaped like execFile's error, so callers read the exit code (or ENOENT) and stdout as before. */
export class QpdfError extends Error {
  // Carries qpdf's exit code (or a spawn error code such as ENOENT) and whatever it printed.
  constructor(
    message: string,
    readonly code: number | string | null,
    readonly stdout: string,
  ) {
    super(message);
  }
}

interface Pending {
  resolve: (stdout: string) => void;
  reject: (err: Error) => void;
}
interface Slot {
  worker: Worker;
  pending: Map<number, Pending>;
}

// Two workers: one handles ~300 spawns/s at ~3 ms each, peak demand is ~150/s (100 page extracts + an
// upload burst), so two leaves headroom, and a crash in one does not stall the other.
const POOL_SIZE = 2;
// Same extension as this file, so it works compiled (dist/*.js) and under tsx in tests (src/*.ts).
const WORKER_URL = new URL(`./qpdf-worker${extname(fileURLToPath(import.meta.url))}`, import.meta.url);

// Builds the pool. Workers start lazily (or via warm) and are unref'd while idle, so they never hold the
// process open by themselves.
export function createQpdfPool(size = POOL_SIZE) {
  const slots: (Slot | undefined)[] = Array.from({ length: size }, () => undefined);
  let nextId = 0;
  let stopping = false;

  // Starts the worker for slot i and wires its replies and failures.
  function spawn(i: number): Slot {
    const slot: Slot = { worker: new Worker(WORKER_URL), pending: new Map() };
    slot.worker.unref();
    // Settles the task this reply belongs to; drops the ref once the worker has nothing outstanding.
    slot.worker.on('message', (r: QpdfReply) => {
      const p = slot.pending.get(r.id);
      if (!p) return;
      slot.pending.delete(r.id);
      if (slot.pending.size === 0) slot.worker.unref();
      if (r.ok) p.resolve(r.stdout);
      else p.reject(new QpdfError(r.message, r.code, r.stdout));
    });
    // Frees the slot and rejects every task the worker held. 'error' is followed by 'exit', so this can
    // run twice; the second time finds nothing pending.
    const fail = (reason: string) => {
      if (slots[i] === slot) slots[i] = undefined;
      if (!stopping && slot.pending.size > 0) log.error('qpdf worker died', { reason, tasks: slot.pending.size });
      for (const p of slot.pending.values()) p.reject(new QpdfError(`qpdf worker died: ${reason}`, null, ''));
      slot.pending.clear();
    };
    slot.worker.on('error', (err: Error) => fail(err.message));
    slot.worker.on('exit', (code) => fail(`exit code ${code}`));
    slots[i] = slot;
    return slot;
  }

  // Sends one task to a given worker and returns its stdout.
  function send(slot: Slot, bin: string, args: string[]): Promise<string> {
    const id = nextId++;
    return new Promise((resolve, reject) => {
      slot.pending.set(id, { resolve, reject });
      slot.worker.ref(); // an outstanding task keeps the process alive until it settles
      const task: QpdfTask = { id, bin, args };
      slot.worker.postMessage(task);
    });
  }

  // Runs qpdf on the least-busy worker (an empty slot wins and gets a fresh worker).
  function run(bin: string, args: string[]): Promise<string> {
    let pick = 0;
    let load = Infinity;
    for (let i = 0; i < size; i++) {
      const n = slots.at(i)?.pending.size ?? -1;
      if (n < load) {
        pick = i;
        load = n;
      }
    }
    return send(slots.at(pick) ?? spawn(pick), bin, args);
  }

  // Starts every worker and runs `qpdf --version` on each, so the first upload burst pays no start-up cost.
  async function warm(bin: string): Promise<void> {
    await Promise.all(Array.from({ length: size }, (_, i) => send(slots.at(i) ?? spawn(i), bin, ['--version'])));
  }

  // Stops all workers; call after in-flight work has drained. A later run() starts fresh workers.
  async function close(): Promise<void> {
    stopping = true;
    const live = slots.filter((s): s is Slot => s !== undefined);
    slots.fill(undefined);
    await Promise.all(live.map((s) => s.worker.terminate()));
    stopping = false;
  }

  return { run, warm, close };
}

// The process-wide pool: one set of qpdf workers shared by ingestion and page extraction.
export const qpdf = createQpdfPool();

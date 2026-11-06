// Worker thread: runs qpdf for the main thread. It keeps no state of its own and talks to the main thread
// only through copied messages, one reply per task, tagged with the task's id (see qpdf-pool.ts).
import { execFile } from 'node:child_process';
import { parentPort } from 'node:worker_threads';
import type { QpdfReply, QpdfTask } from './qpdf-pool.js';

const port = parentPort!;

// Runs one qpdf command and posts exactly one reply. Exit code and stdout travel as plain fields, because
// structured clone does not reliably copy extra properties of an Error.
port.on('message', ({ id, bin, args }: QpdfTask) => {
  execFile(bin, args, (err, stdout) => {
    const reply: QpdfReply = err
      ? { id, ok: false, code: (err as { code?: number | string }).code ?? null, stdout: String(stdout ?? ''), message: err.message }
      : { id, ok: true, stdout };
    port.postMessage(reply);
  });
});

import { createWriteStream, writeFileSync } from 'node:fs';
import { rm } from 'node:fs/promises';
import { pipeline } from 'node:stream/promises';
import type { Readable } from 'node:stream';
import { HttpError } from './errors.js';

// Bodies up to this size are buffered and written with one synchronous write. A streamed write costs an
// open, the writes and a close on the 4-thread libuv pool, and in an upload burst each of those callbacks
// waits a whole event-loop iteration (5-45 ms under load); a sync write of a few KB to the page cache costs
// tens of µs. 50 concurrent uploads at this cap hold at most 12.5 MB.
const BUFFER_MAX = 256 * 1024;

export interface Upload {
  bytes: number;
  /** The whole body when it was small enough to buffer (see BUFFER_MAX), else null. */
  body: Buffer | null;
}

// Receives the request body into `dest` with a byte cap. A small body comes back in memory as well;
// a large one streams to disk with one pipeline buffer (~64 KB) in memory.
export async function receiveUpload(req: Readable & { headers: Record<string, unknown> }, dest: string, maxBytes: number): Promise<Upload> {
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
  const src = capped(req);
  const head: Buffer[] = [];
  try {
    for (;;) {
      const next = await src.next();
      if (next.done) {
        if (bytes === 0) throw new HttpError(422, 'not_a_pdf', 'empty body');
        const body = Buffer.concat(head, bytes);
        writeFileSync(dest, body);
        return { bytes, body };
      }
      head.push(next.value);
      if (bytes > BUFFER_MAX) break;
    }
    // Too big to buffer: write what was read so far, then stream the rest.
    async function* rest(): AsyncGenerator<Buffer> {
      yield* head.splice(0);
      yield* src;
    }
    await pipeline(rest(), createWriteStream(dest));
  } catch (err) {
    await rm(dest, { force: true });
    if (err instanceof HttpError) throw err;
    throw new HttpError(400, 'upload_failed', String(err));
  }
  return { bytes, body: null };
}

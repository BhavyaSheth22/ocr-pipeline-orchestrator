import { createWriteStream } from 'node:fs';
import { rm } from 'node:fs/promises';
import { pipeline } from 'node:stream/promises';
import type { Readable } from 'node:stream';
import { HttpError } from './errors.js';

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

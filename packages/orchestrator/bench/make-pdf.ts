import { createWriteStream } from 'node:fs';
import { once } from 'node:events';

// Writes a minimal valid PDF with `pages` pages. `padBytes` adds an unreferenced stream object so a file can
// reach 100 MB without 100 MB of pages. Streams to disk, so the generator itself never holds the file.
export async function makePdf(path: string, pages: number, padBytes = 0): Promise<void> {
  const out = createWriteStream(path);
  const offsets: number[] = [];
  let pos = 0;

  // Writes a chunk, tracks the byte offset and respects backpressure.
  const write = async (chunk: string | Buffer): Promise<void> => {
    pos += typeof chunk === 'string' ? Buffer.byteLength(chunk, 'latin1') : chunk.length;
    if (!out.write(chunk)) await once(out, 'drain');
  };
  // Starts object `n` and records where it begins (for the xref table).
  const obj = async (n: number, body: string): Promise<void> => {
    offsets[n] = pos;
    await write(`${n} 0 obj\n${body}\nendobj\n`);
  };

  await write('%PDF-1.4\n');
  // Objects: 1 catalog, 2 page tree, then per page i: page (3+2i) and its content stream (4+2i), then pad.
  const kids = Array.from({ length: pages }, (_, i) => `${3 + 2 * i} 0 R`).join(' ');
  await obj(1, '<< /Type /Catalog /Pages 2 0 R >>');
  await obj(2, `<< /Type /Pages /Kids [${kids}] /Count ${pages} >>`);
  for (let i = 0; i < pages; i++) {
    const text = `BT /F1 24 Tf 72 720 Td (Page ${i + 1}) Tj ET`;
    await obj(3 + 2 * i, `<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents ${4 + 2 * i} 0 R >>`);
    await obj(4 + 2 * i, `<< /Length ${text.length} >>\nstream\n${text}\nendstream`);
  }
  let size = 3 + 2 * pages;
  if (padBytes > 0) {
    const n = size++;
    offsets[n] = pos;
    await write(`${n} 0 obj\n<< /Length ${padBytes} >>\nstream\n`);
    const block = Buffer.alloc(1024 * 1024, 0x20);
    for (let left = padBytes; left > 0; left -= block.length) await write(left >= block.length ? block : block.subarray(0, left));
    await write('\nendstream\nendobj\n');
  }
  const xref = pos;
  let table = `xref\n0 ${size}\n0000000000 65535 f \n`;
  for (let n = 1; n < size; n++) table += `${String(offsets[n]).padStart(10, '0')} 00000 n \n`;
  await write(`${table}trailer\n<< /Size ${size} /Root 1 0 R >>\nstartxref\n${xref}\n%%EOF\n`);
  out.end();
  await once(out, 'finish');
}

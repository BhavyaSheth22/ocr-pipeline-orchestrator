import { test } from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { deflateSync } from 'node:zlib';
import { readPageCount } from '../src/pdf/page-count.js';
import { countPages } from '../src/pdf/pdf.js';
import { loadConfig } from '../src/config.js';
import { makePdf } from '../bench/make-pdf.js';
import { hasQpdf, QPDF } from './helpers.js';

const dir = mkdtempSync(join(tmpdir(), 'page-count-'));
test.after(() => rmSync(dir, { recursive: true, force: true }));
let seq = 0;

// Writes bytes to a fresh file and returns its path.
function file(bytes: Buffer | string): string {
  const path = join(dir, `${seq++}.pdf`);
  writeFileSync(path, bytes);
  return path;
}

// A makePdf file as bytes.
async function classicPdf(pages: number): Promise<Buffer> {
  const path = join(dir, `${seq++}.pdf`);
  await makePdf(path, pages);
  return readFileSync(path);
}

// PNG-filters rows of `cols` bytes, cycling through filter types 0-4 so the decoder sees every one.
function pngEncode(raw: Buffer, cols: number): Buffer {
  const rows = raw.length / cols;
  const out = Buffer.alloc(rows * (cols + 1));
  for (let r = 0; r < rows; r++) {
    const type = r % 5;
    out[r * (cols + 1)] = type;
    for (let i = 0; i < cols; i++) {
      const x = raw[r * cols + i]!;
      const a = i > 0 ? raw[r * cols + i - 1]! : 0;
      const b = r > 0 ? raw[(r - 1) * cols + i]! : 0;
      const c = r > 0 && i > 0 ? raw[(r - 1) * cols + i - 1]! : 0;
      const p = a + b - c;
      const paeth = Math.abs(p - a) <= Math.abs(p - b) && Math.abs(p - a) <= Math.abs(p - c) ? a : Math.abs(p - b) <= Math.abs(p - c) ? b : c;
      const pred = [0, a, b, (a + b) >> 1, paeth][type]!;
      out[r * (cols + 1) + 1 + i] = (x - pred) & 0xff;
    }
  }
  return out;
}

// A PDF 1.5 file with every object in one object stream and a cross-reference stream (no xref table),
// optionally Flate-compressed with PNG predictors, as qpdf, Acrobat and most modern writers produce.
function objStreamPdf(pages: number, { compress = true } = {}): Buffer {
  const objs = [
    '<< /Type /Catalog /Pages 2 0 R >>',
    `<< /Type /Pages /Kids [${Array.from({ length: pages }, (_, i) => `${3 + i} 0 R`).join(' ')}] /Count ${pages} >>`,
    ...Array.from({ length: pages }, () => '<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] >>'),
  ];
  const stmNum = objs.length + 1;
  const xrefNum = stmNum + 1;
  let body = '';
  const header: string[] = [];
  objs.forEach((o, i) => {
    header.push(`${i + 1} ${body.length}`);
    body += `${o}\n`;
  });
  const head = `${header.join(' ')}\n`;
  const stmData = Buffer.from(head + body, 'latin1');
  const stmBytes = compress ? deflateSync(stmData) : stmData;
  const parts: Buffer[] = [Buffer.from('%PDF-1.5\n%\xe2\xe3\xcf\xd3\n', 'latin1')];
  const len = () => parts.reduce((n, p) => n + p.length, 0);
  const stmAt = len();
  parts.push(
    Buffer.from(`${stmNum} 0 obj\n<< /Type /ObjStm /N ${objs.length} /First ${head.length} /Length ${stmBytes.length}${compress ? ' /Filter /FlateDecode' : ''} >>\nstream\n`, 'latin1'),
    stmBytes,
    Buffer.from('\nendstream\nendobj\n', 'latin1'),
  );
  const xrefAt = len();
  // Rows of W [1 4 2]: type, field 2 (offset or object stream number), field 3 (index in the stream).
  const rows = Buffer.alloc((xrefNum + 1) * 7);
  rows.writeUInt8(0, 0);
  rows.writeUInt16BE(65535, 5);
  objs.forEach((_, i) => {
    const at = (i + 1) * 7;
    rows.writeUInt8(2, at);
    rows.writeUInt32BE(stmNum, at + 1);
    rows.writeUInt16BE(i, at + 5);
  });
  rows.writeUInt8(1, stmNum * 7);
  rows.writeUInt32BE(stmAt, stmNum * 7 + 1);
  rows.writeUInt8(1, xrefNum * 7);
  rows.writeUInt32BE(xrefAt, xrefNum * 7 + 1);
  const xrefBytes = compress ? deflateSync(pngEncode(rows, 7)) : rows;
  const filter = compress ? ' /Filter /FlateDecode /DecodeParms << /Predictor 12 /Columns 7 >>' : '';
  parts.push(
    Buffer.from(`${xrefNum} 0 obj\n<< /Type /XRef /Size ${xrefNum + 1} /W [1 4 2] /Root 1 0 R /Length ${xrefBytes.length}${filter} >>\nstream\n`, 'latin1'),
    xrefBytes,
    Buffer.from(`\nendstream\nendobj\nstartxref\n${xrefAt}\n%%EOF\n`, 'latin1'),
  );
  return Buffer.concat(parts);
}

// Appends an incremental update that replaces the page tree root with one claiming `count` pages.
function withUpdate(pdf: Buffer, count: number): Buffer {
  const prev = Number(/startxref\s+(\d+)/.exec(pdf.subarray(-64).toString('latin1'))![1]);
  const objAt = pdf.length;
  const obj = `2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count ${count} >>\nendobj\n`;
  const xrefAt = objAt + obj.length;
  const xref = `xref\n0 1\n0000000000 65535 f \n2 1\n${String(objAt).padStart(10, '0')} 00000 n \n`;
  return Buffer.concat([pdf, Buffer.from(`${obj}${xref}trailer\n<< /Size 9999 /Root 1 0 R /Prev ${prev} >>\nstartxref\n${xrefAt}\n%%EOF\n`, 'latin1')]);
}

test('classic xref tables (makePdf output)', async () => {
  for (const n of [1, 7, 100]) assert.equal(readPageCount(file(await classicPdf(n))), n);
});

test('object streams and a Flate xref stream with PNG predictors', () => {
  for (const n of [1, 12, 60]) assert.equal(readPageCount(file(objStreamPdf(n))), n);
});

test('uncompressed object and xref streams', () => {
  assert.equal(readPageCount(file(objStreamPdf(9, { compress: false }))), 9);
});

test('an incremental update wins over the original page tree', async () => {
  assert.equal(readPageCount(file(withUpdate(await classicPdf(5), 3))), 3);
  assert.equal(readPageCount(file(withUpdate(objStreamPdf(5), 4))), 4);
});

test('anything it does not fully understand returns null (qpdf decides)', async () => {
  const good = await classicPdf(4);
  const text = good.toString('latin1');
  const cases: Record<string, Buffer | string> = {
    'not a PDF': 'hello',
    empty: '',
    'truncated tail': good.subarray(0, good.length - 30),
    'startxref past the end': text.replace(/startxref\n\d+/, 'startxref\n99999999'),
    // Bytes inserted before the objects shift every offset: the object-number check catches it.
    'shifted offsets': text.replace('%PDF-1.4\n', '%PDF-1.4\n% junk junk junk\n'),
    encrypted: text.replace('/Root 1 0 R', '/Root 1 0 R /Encrypt 99 0 R'),
    // Same byte length, so every offset stays valid and only the filter is unsupported.
    'unsupported filter': objStreamPdf(3).toString('latin1').replace('/Filter /FlateDecode >>', '/Filter /LZWDecode   >>'),
    'page tree root without /Count': text.replace('/Pages 2 0 R', '/Pages 1 0 R'),
  };
  for (const [name, bytes] of Object.entries(cases)) {
    assert.equal(readPageCount(file(typeof bytes === 'string' ? Buffer.from(bytes, 'latin1') : bytes)), null, name);
  }
});

test('countPages answers well-formed files without qpdf and falls back to it otherwise', async () => {
  const cfg = loadConfig({ DATA_DIR: dir, QPDF_BIN: join(dir, 'no-such-qpdf') });
  assert.equal(await countPages(cfg, file(await classicPdf(6))), 6);
  await assert.rejects(countPages(cfg, file('hello')), /qpdf not found/, 'non-PDF went to qpdf');
  await assert.rejects(countPages(cfg, file(await classicPdf(101))), (err: { code?: string }) => err.code === 'too_many_pages');
});

test('agrees with qpdf on files qpdf itself rewrites', { skip: hasQpdf ? false : 'qpdf not available (set QPDF_BIN)' }, async () => {
  const src = file(await classicPdf(17));
  for (const args of [['--object-streams=generate'], ['--linearize'], ['--object-streams=generate', '--linearize'], ['--qdf']]) {
    const out = join(dir, `${seq++}.pdf`);
    execFileSync(QPDF, [...args, src, out]);
    const expected = Number(execFileSync(QPDF, ['--show-npages', out]).toString().trim());
    assert.equal(readPageCount(out), expected, args.join(' '));
  }
});

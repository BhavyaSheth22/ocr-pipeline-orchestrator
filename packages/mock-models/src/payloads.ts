import { createHash } from 'node:crypto';
import { mulberry32, type Rng } from './rng.js';

/** Same (documentId, page) always yields the same payload, so tests are reproducible. */
function rngFor(documentId: string, page: number, salt: string): Rng {
  const h = createHash('sha256').update(`${salt}:${documentId}:${page}`).digest();
  return mulberry32(h.readUInt32BE(0));
}

const LABELS = ['title', 'paragraph', 'table', 'figure', 'key_value', 'footer'] as const;
const WORDS = ['invoice', 'total', 'amount', 'due', 'date', 'party', 'clause', 'sample', 'patient', 'value'];

export interface Box { x: number; y: number; w: number; h: number; label: string; confidence: number }

export function layoutPayload(documentId: string, page: number) {
  const r = rngFor(documentId, page, 'layout');
  const n = 4 + Math.floor(r() * 9); // 4..12 regions
  const boxes: Box[] = [];
  for (let i = 0; i < n; i++) {
    const w = 80 + Math.floor(r() * 400);
    const h = 20 + Math.floor(r() * 200);
    boxes.push({
      x: Math.floor(r() * (612 - w)),
      y: Math.floor(r() * (792 - h)),
      w,
      h,
      label: LABELS[Math.floor(r() * LABELS.length)]!,
      confidence: Math.round((0.8 + r() * 0.19) * 1000) / 1000,
    });
  }
  // Reading order: top-to-bottom, then left-to-right.
  const reading_order = boxes
    .map((b, i) => ({ i, y: b.y, x: b.x }))
    .sort((a, b) => a.y - b.y || a.x - b.x)
    .map((o) => o.i);
  return { document_id: documentId, page, boxes, reading_order };
}

export function vlmPayload(documentId: string, page: number) {
  const r = rngFor(documentId, page, 'vlm');
  const word = () => WORDS[Math.floor(r() * WORDS.length)]!;
  const sentence = () => Array.from({ length: 6 + Math.floor(r() * 8) }, word).join(' ');
  const text = Array.from({ length: 3 + Math.floor(r() * 4) }, sentence).join('\n');
  const cols = 2 + Math.floor(r() * 3);
  const header = Array.from({ length: cols }, (_, i) => `${word()}_${i}`);
  const rows = Array.from({ length: 2 + Math.floor(r() * 4) }, () =>
    Array.from({ length: cols }, () => String(Math.floor(r() * 10000) / 100)),
  );
  const markdown = [
    `| ${header.join(' | ')} |`,
    `| ${header.map(() => '---').join(' | ')} |`,
    ...rows.map((row) => `| ${row.join(' | ')} |`),
  ].join('\n');
  const key_values: Record<string, string> = {};
  for (let i = 0; i < 2 + Math.floor(r() * 4); i++) key_values[`${word()}_${i}`] = word() + String(Math.floor(r() * 1000));
  return { document_id: documentId, page, text, tables: [{ markdown }], key_values };
}

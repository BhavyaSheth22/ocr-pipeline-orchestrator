import { HttpError } from '../http/errors.js';
import { codePoints, editDistance, normalize, wordIds } from './edit-distance.js';
import { greedyMatch } from './iou.js';
import { treeEditDistance } from './ted.js';
import { parseTree } from './tree.js';

// POST /evaluate: CER, WER, IoU and TED between a predicted and a ground-truth document tree.
// Text metrics are page-aligned (pages matched by index) and micro-averaged: Σ edits / Σ reference units.

// Ratio with a defined value when the denominator is zero.
const ratio = (num: number, den: number) => (den > 0 ? num / den : num === 0 ? 0 : 1);

// Runs every metric; throws 413 before any TED work when the step budget would be exceeded.
export function evaluate(body: unknown, tedMaxSteps: number) {
  const t0 = performance.now();
  if (typeof body !== 'object' || body === null) throw new HttpError(400, 'bad_request', 'body must be {prediction, ground_truth}');
  const { prediction, ground_truth } = body as Record<string, unknown>;
  if (prediction === undefined || ground_truth === undefined) throw new HttpError(400, 'bad_request', 'body must be {prediction, ground_truth}');

  const labels = new Map<string, number>();
  const pred = parseTree(prediction, labels, 'prediction');
  const gt = parseTree(ground_truth, labels, 'ground_truth');
  const t1 = performance.now();

  // CER and WER: same routine, code points vs interned words. Missing pages count as all deletions,
  // extra predicted pages as all insertions.
  const words = new Map<string, number>();
  let charEdits = 0;
  let charRef = 0;
  let wordEdits = 0;
  let wordRef = 0;
  const pages = Math.max(pred.pageText.length, gt.pageText.length);
  for (let p = 0; p < pages; p++) {
    const ps = normalize(pred.pageText[p] ?? '');
    const gs = normalize(gt.pageText[p] ?? '');
    const gc = codePoints(gs);
    const gw = wordIds(gs, words);
    charEdits += editDistance(codePoints(ps), gc);
    charRef += gc.length;
    wordEdits += editDistance(wordIds(ps, words), gw);
    wordRef += gw.length;
  }
  const t2 = performance.now();

  // IoU: greedy matching per page, totals across pages.
  let matched = 0;
  let iouSum = 0;
  let predBoxes = 0;
  let gtBoxes = 0;
  for (let p = 0; p < pages; p++) {
    const a = pred.pageBoxes[p] ?? [];
    const b = gt.pageBoxes[p] ?? [];
    const r = greedyMatch(a, b);
    matched += r.matched;
    iouSum += r.iouSum;
    predBoxes += a.length;
    gtBoxes += b.length;
  }
  const precision = ratio(matched, predBoxes);
  const recall = ratio(matched, gtBoxes);
  const t3 = performance.now();

  // TED: the exact step count is known before running, so oversized inputs are refused up front.
  const steps = pred.work * gt.work;
  if (steps > tedMaxSteps) {
    throw new HttpError(413, 'ted_too_large', `tree edit distance needs ${steps} steps, max ${tedMaxSteps}`, {}, { steps, max_steps: tedMaxSteps });
  }
  const distance = treeEditDistance(pred, gt);
  const t4 = performance.now();

  return {
    cer: { value: ratio(charEdits, charRef), edits: charEdits, ref_chars: charRef },
    wer: { value: ratio(wordEdits, wordRef), edits: wordEdits, ref_words: wordRef },
    iou: {
      mean_iou: matched ? iouSum / matched : 0,
      precision,
      recall,
      f1: precision + recall > 0 ? (2 * precision * recall) / (precision + recall) : 0,
      matched,
      pred: predBoxes,
      gt: gtBoxes,
      threshold: 0.5,
    },
    ted: { distance, normalized: distance / (pred.size + gt.size), nodes_pred: pred.size, nodes_gt: gt.size, steps },
    timing_ms: {
      parse: +(t1 - t0).toFixed(3),
      text: +(t2 - t1).toFixed(3),
      iou: +(t3 - t2).toFixed(3),
      ted: +(t4 - t3).toFixed(3),
      total: +(t4 - t0).toFixed(3),
    },
  };
}

// Box overlap and per-page greedy matching (the COCO / PASCAL VOC convention at IoU ≥ 0.5).

export type Box = [number, number, number, number]; // x, y, w, h

// Intersection over union of two boxes; 0 when the union is empty.
export function iou(a: Box, b: Box): number {
  const iw = Math.max(0, Math.min(a[0] + a[2], b[0] + b[2]) - Math.max(a[0], b[0]));
  const ih = Math.max(0, Math.min(a[1] + a[3], b[1] + b[3]) - Math.max(a[1], b[1]));
  const inter = iw * ih;
  const union = a[2] * a[3] + b[2] * b[3] - inter;
  return union > 0 ? inter / union : 0;
}

export interface MatchTotals {
  matched: number;
  iouSum: number;
}

// Greedy 1-to-1 matching on one page: all pairs at or above the threshold, best first, each box used once.
export function greedyMatch(pred: Box[], gt: Box[], threshold = 0.5): MatchTotals {
  const pairs: [number, number, number][] = [];
  for (let i = 0; i < pred.length; i++) {
    for (let j = 0; j < gt.length; j++) {
      const v = iou(pred[i]!, gt[j]!);
      if (v >= threshold) pairs.push([v, i, j]);
    }
  }
  pairs.sort((x, y) => y[0] - x[0]);
  const usedP = new Uint8Array(pred.length);
  const usedG = new Uint8Array(gt.length);
  let matched = 0;
  let iouSum = 0;
  for (const [v, i, j] of pairs) {
    if (usedP[i] || usedG[j]) continue;
    usedP[i] = usedG[j] = 1;
    matched++;
    iouSum += v;
  }
  return { matched, iouSum };
}

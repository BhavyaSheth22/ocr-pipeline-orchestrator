import type { ParsedTree } from './tree.js';

// Zhang-Shasha (1989) tree edit distance with unit costs (rename is free when labels match).
// O(n·m·min(depth, leaves)²) time; document trees are shallow, so this is near O(n·m) in practice.
// Exactly W(A)·W(B) inner-loop steps, which the caller checks against a budget before calling.
// treedist is n·m; the forest table is one buffer reused across keyroot pairs (each pair only reads
// cells it wrote itself). The inner loop compares interned ints and allocates nothing.
export function treeEditDistance(a: ParsedTree, b: ParsedTree): number {
  const n = a.size;
  const m = b.size;
  if (n === 0 || m === 0) return n + m;
  const td = new Int32Array(n * m);
  const fd = new Int32Array((n + 1) * (m + 1));
  const al = a.lml;
  const bl = b.lml;
  const alab = a.labels;
  const blab = b.labels;

  for (const i of a.keyroots) {
    for (const j of b.keyroots) {
      const li = al[i]!;
      const lj = bl[j]!;
      const rows = i - li + 2;
      const cols = j - lj + 2;
      fd[0] = 0;
      for (let x = 1; x < rows; x++) fd[x * cols] = fd[(x - 1) * cols]! + 1; // delete
      for (let y = 1; y < cols; y++) fd[y] = fd[y - 1]! + 1; // insert
      for (let x = 1; x < rows; x++) {
        const ii = li + x - 1;
        const aLml = al[ii]!;
        for (let y = 1; y < cols; y++) {
          const jj = lj + y - 1;
          const del = fd[(x - 1) * cols + y]! + 1;
          const ins = fd[x * cols + y - 1]! + 1;
          let v: number;
          if (aLml === li && bl[jj] === lj) {
            // Both prefixes are whole trees: a rename (or match) of the two roots.
            const ren = fd[(x - 1) * cols + y - 1]! + (alab[ii] === blab[jj] ? 0 : 1);
            v = Math.min(del, ins, ren);
            td[ii * m + jj] = v;
          } else {
            // Forests: reuse the already-computed distance of the subtrees rooted at ii and jj.
            const p = aLml - li;
            const q = bl[jj]! - lj;
            v = Math.min(del, ins, fd[p * cols + q]! + td[ii * m + jj]!);
          }
          fd[x * cols + y] = v;
        }
      }
    }
  }
  return td[(n - 1) * m + (m - 1)]!;
}

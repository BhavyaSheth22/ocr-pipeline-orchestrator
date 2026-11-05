// One Levenshtein routine for both CER (code points) and WER (interned word ids).
// Common prefix and suffix are trimmed first; then a single-row DP: O(n·m) time, O(min(n, m)) space.

// Edit distance between two int sequences.
export function editDistance(a: Int32Array, b: Int32Array): number {
  let start = 0;
  let endA = a.length;
  let endB = b.length;
  while (start < endA && start < endB && a[start] === b[start]) start++;
  while (endA > start && endB > start && a[endA - 1] === b[endB - 1]) {
    endA--;
    endB--;
  }
  let s = a.subarray(start, endA);
  let t = b.subarray(start, endB);
  if (s.length < t.length) [s, t] = [t, s]; // the row runs over the shorter one
  if (t.length === 0) return s.length;
  const row = new Int32Array(t.length + 1);
  for (let j = 0; j <= t.length; j++) row[j] = j;
  for (let i = 1; i <= s.length; i++) {
    let diag = row[0]!;
    row[0] = i;
    const si = s[i - 1];
    for (let j = 1; j <= t.length; j++) {
      const up = row[j]!;
      row[j] = Math.min(up + 1, row[j - 1]! + 1, diag + (si === t[j - 1] ? 0 : 1));
      diag = up;
    }
  }
  return row[t.length]!;
}

// Standard OCR-eval normalization: NFC, whitespace collapsed, trimmed.
export const normalize = (s: string) => s.normalize('NFC').replace(/\s+/g, ' ').trim();

// A string as code points (so a non-BMP character counts as one).
export function codePoints(s: string): Int32Array {
  return Int32Array.from(s, (ch) => ch.codePointAt(0)!);
}

// A string as word ids, interned through the shared dictionary.
export function wordIds(s: string, dict: Map<string, number>): Int32Array {
  const words = s === '' ? [] : s.split(' ');
  return Int32Array.from(words, (w) => {
    let id = dict.get(w);
    if (id === undefined) dict.set(w, (id = dict.size));
    return id;
  });
}

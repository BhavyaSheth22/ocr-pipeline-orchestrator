import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildApp } from '../src/http/app.js';
import { openDb } from '../src/storage/db.js';
import { codePoints, editDistance, wordIds } from '../src/eval/edit-distance.js';
import { evaluate } from '../src/eval/evaluate.js';
import { greedyMatch, iou } from '../src/eval/iou.js';
import { treeEditDistance } from '../src/eval/ted.js';
import { parseTree } from '../src/eval/tree.js';
import { createNotifier } from '../src/http/stream.js';
import { combTree, docTree, rng } from '../bench/trees.js';
import { testConfig, withServer } from './helpers.js';

type T = { type: string; text?: string; bbox?: number[]; children?: T[] };
// Builds a node: n('f', n('a'), n('b')).
const n = (type: string, ...children: T[]): T => (children.length ? { type, children } : { type });
// TED between two trees.
const ted = (a: T, b: T) => {
  const m = new Map<string, number>();
  return treeEditDistance(parseTree(a, m, 'a'), parseTree(b, m, 'b'));
};

test('Zhang-Shasha golden example: f(d(a c(b)) e) vs f(c(d(a b)) e) = 2', () => {
  const a = n('f', n('d', n('a'), n('c', n('b'))), n('e'));
  const b = n('f', n('c', n('d', n('a'), n('b'))), n('e'));
  assert.equal(ted(a, b), 2);
  assert.equal(ted(b, a), 2, 'symmetric');
});

test('TED basics: identical = 0, single node vs n-node chain = n - 1, rename = 1', () => {
  const t = n('doc', n('page', n('para'), n('table')));
  assert.equal(ted(t, t), 0);
  assert.equal(ted(n('x'), n('x', n('y', n('z')))), 2);
  assert.equal(ted(n('x', n('a')), n('x', n('b'))), 1);
  assert.equal(ted(n('a'), n('b')), 1);
});

test('TED is symmetric on random document trees', () => {
  const r = rng(1);
  for (let i = 0; i < 30; i++) {
    const a = docTree(30, r) as T;
    const b = docTree(30, r) as T;
    assert.equal(ted(a, b), ted(b, a));
  }
});

test('work W(T) = Σ keyroot subtree sizes (golden tree: keyroots c, e, f → 2 + 1 + 6)', () => {
  const p = parseTree(n('f', n('d', n('a'), n('c', n('b'))), n('e')), new Map(), 't');
  assert.equal(p.size, 6);
  assert.deepEqual([...p.keyroots], [2, 4, 5]);
  assert.equal(p.work, 9);
});

test('parsing is iterative: a 100k-deep chain does not overflow the stack', () => {
  let t: T = n('leaf');
  for (let i = 0; i < 100_000; i++) t = n('x', t);
  const p = parseTree(t, new Map(), 't');
  assert.equal(p.size, 100_001);
});

test('edit distance: classic values, and CER/WER share one routine', () => {
  assert.equal(editDistance(codePoints('kitten'), codePoints('sitting')), 3);
  assert.equal(editDistance(codePoints(''), codePoints('abc')), 3);
  assert.equal(editDistance(codePoints('same'), codePoints('same')), 0);
  assert.equal(editDistance(codePoints('😀a'), codePoints('😀b')), 1, 'code points, not UTF-16 units');
  const d = new Map<string, number>();
  assert.equal(editDistance(wordIds('the total amount due', d), wordIds('the amount is due', d)), 2);
});

test('IoU: known values and a zero-area guard', () => {
  assert.equal(iou([0, 0, 10, 10], [0, 0, 10, 10]), 1);
  assert.equal(iou([0, 0, 10, 10], [5, 0, 10, 10]), 50 / 150);
  assert.equal(iou([0, 0, 10, 10], [20, 20, 5, 5]), 0);
  assert.equal(iou([0, 0, 0, 0], [0, 0, 0, 0]), 0);
});

test('greedy matching takes the best pair first, one-to-one, at IoU ≥ 0.5', () => {
  const gt: [number, number, number, number][] = [[0, 0, 10, 10]];
  const pred: [number, number, number, number][] = [[1, 0, 10, 10], [0, 0, 10, 10]];
  const r = greedyMatch(pred, gt);
  assert.equal(r.matched, 1);
  assert.equal(r.iouSum, 1, 'the exact box wins');
  assert.equal(greedyMatch([[6, 0, 10, 10]], gt).matched, 0, 'IoU 0.25 is below threshold');
});

test('evaluate: page-aligned micro-averaged CER/WER, IoU totals, TED', () => {
  const gt = { type: 'document', children: [
    { type: 'page', children: [{ type: 'line', text: 'hello world', bbox: [0, 0, 10, 10] }] },
    { type: 'page', children: [{ type: 'line', text: 'abc' }] },
  ] };
  const pred = { type: 'document', children: [
    { type: 'page', children: [{ type: 'line', text: 'hello  wurld', bbox: [0, 0, 10, 10] }] },
  ] };
  const out = evaluate({ prediction: pred, ground_truth: gt }, 5e6);
  assert.equal(out.cer.edits, 1 + 3, 'one substitution, plus missing page 2 as deletions');
  assert.equal(out.cer.ref_chars, 11 + 3);
  assert.equal(out.wer.edits, 1 + 1);
  assert.equal(out.wer.ref_words, 3);
  assert.equal(out.iou.matched, 1);
  assert.equal(out.iou.recall, 1);
  assert.equal(out.ted.distance, 2, 'page 2 and its line are missing');
});

test('evaluate refuses over-budget TED with 413 details, before running it', () => {
  const big = combTree(1000);
  assert.throws(
    () => evaluate({ prediction: big, ground_truth: big }, 5e6),
    (e: { status: number; code: string; details: { steps: number } }) => e.status === 413 && e.code === 'ted_too_large' && e.details.steps > 5e6,
  );
});

test('every 50-node pair fits the budget, including the worst comb shape', () => {
  const c = combTree(50);
  const p = parseTree(c, new Map(), 'c');
  assert.ok(p.work * p.work <= 1.6e6, `steps ${p.work * p.work}`);
});

test('POST /evaluate over HTTP: 200, 400 on bad input, 413 on oversize TED', async () => {
  const cfg = testConfig({ TED_MAX_STEPS: '1000' });
  const app = buildApp({ cfg, db: openDb(':memory:'), scheduler: { wake() {}, vlmRate: () => 10 }, notifier: createNotifier() });
  await withServer(app, async (url) => {
    // POSTs a JSON body to /evaluate.
    const post = (body: unknown) => fetch(`${url}/evaluate`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
    const ok = await post({ prediction: n('doc'), ground_truth: n('doc', n('page')) });
    assert.equal(ok.status, 200);
    assert.equal(((await ok.json()) as { ted: { distance: number } }).ted.distance, 1);
    assert.equal((await post({ prediction: { type: 3 }, ground_truth: n('d') })).status, 400);
    assert.equal((await post({ prediction: n('d') })).status, 400);
    const big = await post({ prediction: combTree(60), ground_truth: combTree(60) });
    assert.equal(big.status, 413);
    const j = (await big.json()) as { error: string; max_steps: number };
    assert.equal(j.error, 'ted_too_large');
    assert.equal(j.max_steps, 1000);
  });
});

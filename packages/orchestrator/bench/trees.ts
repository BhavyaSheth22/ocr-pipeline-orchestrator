import type { TreeNode } from '../src/pipeline.js';

const BLOCKS = ['title', 'paragraph', 'table', 'figure', 'key_value', 'footer'];
const WORDS = ['invoice', 'total', 'amount', 'due', 'date', 'party', 'clause', 'patient', 'value'];

// Deterministic PRNG (mulberry32) so bench runs are reproducible.
export function rng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// A realistic document tree with exactly `nodes` nodes: document → pages → blocks → lines (depth ≤ 4).
export function docTree(nodes: number, r: () => number): TreeNode {
  const root: TreeNode = { type: 'document', children: [] };
  let count = 1;
  const pages: TreeNode[] = [];
  // Adds a child under parent and counts it.
  const add = (parent: TreeNode, node: TreeNode) => {
    (parent.children ??= []).push(node);
    count++;
    return node;
  };
  pages.push(add(root, { type: 'page', children: [] }));
  const blocks: TreeNode[] = [];
  while (count < nodes) {
    const roll = r();
    if (roll < 0.08 && count < nodes) pages.push(add(root, { type: 'page', children: [] }));
    else if (roll < 0.55 || blocks.length === 0) {
      const page = pages[Math.floor(r() * pages.length)]!;
      const x = Math.floor(r() * 400);
      const y = Math.floor(r() * 600);
      blocks.push(add(page, { type: BLOCKS[Math.floor(r() * BLOCKS.length)]!, bbox: [x, y, 50 + Math.floor(r() * 150), 20 + Math.floor(r() * 80)] }));
    } else {
      const block = blocks[Math.floor(r() * blocks.length)]!;
      const text = Array.from({ length: 3 + Math.floor(r() * 6) }, () => WORDS[Math.floor(r() * WORDS.length)]).join(' ');
      add(block, { type: 'line', text });
    }
  }
  return root;
}

// The adversarial shape for Zhang-Shasha: a "comb" where every spine node has a leaf on its left,
// so nearly every node is a keyroot. depth ≈ n/2.
export function combTree(nodes: number): TreeNode {
  let node: TreeNode = { type: 'leaf' };
  let count = 1;
  while (count + 2 <= nodes) {
    node = { type: 'spine', children: [{ type: 'leaf' }, node] };
    count += 2;
  }
  return count < nodes ? { type: 'spine', children: [node] } : node;
}

// Counts nodes in a tree (iteratively).
export function countNodes(t: TreeNode): number {
  let n = 0;
  const stack = [t];
  while (stack.length) {
    const x = stack.pop()!;
    n++;
    for (const c of x.children ?? []) stack.push(c);
  }
  return n;
}

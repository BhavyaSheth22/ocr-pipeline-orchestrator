import { HttpError } from '../http/errors.js';

// One iterative pass per input tree: validates it and builds everything the metrics need. An explicit stack
// (no recursion) means a deep tree cannot overflow the call stack.

/** A parsed tree in the flat form Zhang-Shasha wants: postorder arrays, 0-indexed. */
export interface ParsedTree {
  size: number;
  /** Interned label of each node, by postorder index. */
  labels: Int32Array;
  /** Postorder index of each node's leftmost leaf. */
  lml: Int32Array;
  /** Keyroots in ascending postorder (the root and every node with a left sibling). */
  keyroots: Int32Array;
  /** Σ over keyroots of subtree size; TED runs exactly W(A)·W(B) inner steps. */
  work: number;
  /** Text of each page (root's children of type "page"), in document order. */
  pageText: string[];
  /** Boxes [x, y, w, h] of each page. */
  pageBoxes: [number, number, number, number][][];
}

interface Frame {
  node: Record<string, unknown>;
  kids: unknown[];
  next: number;
  firstLml: number;
  page: number; // index into pageText, or -1 outside any page
}

// Validates one node's fields; returns its children array.
function check(n: unknown, where: string): { node: Record<string, unknown>; kids: unknown[] } {
  if (typeof n !== 'object' || n === null || Array.isArray(n)) throw new HttpError(400, 'bad_request', `${where}: node must be an object`);
  const node = n as Record<string, unknown>;
  if (typeof node.type !== 'string' || node.type.length === 0) throw new HttpError(400, 'bad_request', `${where}: "type" must be a non-empty string`);
  if (node.text !== undefined && typeof node.text !== 'string') throw new HttpError(400, 'bad_request', `${where}: "text" must be a string`);
  if (node.bbox !== undefined) {
    const b = node.bbox;
    if (!Array.isArray(b) || b.length !== 4 || !b.every((v) => typeof v === 'number' && Number.isFinite(v)) || b[2] < 0 || b[3] < 0) {
      throw new HttpError(400, 'bad_request', `${where}: "bbox" must be [x, y, w, h] with w, h >= 0`);
    }
  }
  if (node.children !== undefined && !Array.isArray(node.children)) throw new HttpError(400, 'bad_request', `${where}: "children" must be an array`);
  return { node, kids: (node.children as unknown[] | undefined) ?? [] };
}

// Parses and validates a tree; labels are interned through the shared map so both trees use the same ids.
export function parseTree(root: unknown, intern: Map<string, number>, name: string): ParsedTree {
  const labels: number[] = [];
  const lml: number[] = [];
  const pageText: string[][] = [];
  const pageBoxes: [number, number, number, number][][] = [];

  // Opens a frame for a node (preorder visit): records page membership, text and box.
  const open = (n: unknown, where: string, parentPage: number, isPageChildOfRoot: boolean): Frame => {
    const { node, kids } = check(n, where);
    let page = parentPage;
    if (isPageChildOfRoot && node.type === 'page') {
      page = pageText.length;
      pageText.push([]);
      pageBoxes.push([]);
    }
    if (page >= 0) {
      if (typeof node.text === 'string') pageText[page]!.push(node.text);
      if (node.bbox) pageBoxes[page]!.push(node.bbox as [number, number, number, number]);
    }
    return { node, kids, next: 0, firstLml: -1, page };
  };

  const stack: Frame[] = [open(root, name, -1, false)];
  while (stack.length) {
    const top = stack[stack.length - 1]!;
    if (top.next < top.kids.length) {
      const i = top.next++;
      stack.push(open(top.kids[i], `${name}…children[${i}]`, top.page, stack.length === 1));
      continue;
    }
    // Postorder visit: assign this node's index.
    stack.pop();
    const idx = labels.length;
    let id = intern.get(top.node.type as string);
    if (id === undefined) intern.set(top.node.type as string, (id = intern.size));
    labels.push(id);
    const myLml = top.firstLml === -1 ? idx : top.firstLml;
    lml.push(myLml);
    const parent = stack[stack.length - 1];
    if (parent && parent.firstLml === -1) parent.firstLml = myLml;
  }

  // A node is a keyroot when no later node shares its leftmost leaf (scan from the root down).
  const size = labels.length;
  const seen = new Uint8Array(size);
  const keyroots: number[] = [];
  let work = 0;
  for (let i = size - 1; i >= 0; i--) {
    if (!seen[lml[i]!]) {
      seen[lml[i]!] = 1;
      keyroots.push(i);
      work += i - lml[i]! + 1;
    }
  }
  keyroots.reverse();
  return {
    size,
    labels: Int32Array.from(labels),
    lml: Int32Array.from(lml),
    keyroots: Int32Array.from(keyroots),
    work,
    pageText: pageText.map((t) => t.join(' ')),
    pageBoxes,
  };
}

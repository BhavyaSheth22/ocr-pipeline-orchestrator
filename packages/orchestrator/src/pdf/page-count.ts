// Reads a PDF's page count in-process: startxref → xref chain → trailer /Root → catalog /Pages → /Count.
// This is ingestion's fast path. qpdf costs a fork of ~3 ms per upload, and in a 50-upload burst the forks
// queued every page count behind each other (the main TTFP cost). Like pdf.js, it trusts the root page
// tree's /Count. Anything it does not fully understand returns null, and the caller asks qpdf instead:
// encrypted files, filters other than Flate, broken or shifted xref offsets, oversized structures.
// So a malformed upload still gets qpdf's verdict (422), and this reader never has to repair a file.
//
// Reads are synchronous preads of a few KB. The upload was written by this process milliseconds earlier,
// so they are page-cache hits (microseconds), cheaper than libuv threadpool round trips during a burst.
import { closeSync, fstatSync, openSync, readSync } from 'node:fs';
import { inflateSync } from 'node:zlib';

/** Names keep their slash ('/Pages'); string literals are not needed and parse as null. */
type Val = number | string | boolean | null | Ref | Val[] | Dict;
type Dict = Map<string, Val>;
interface Ref {
  ref: number;
}
interface Cursor {
  buf: Buffer;
  pos: number;
}
/** Where the newest xref entry for an object points: a file offset, a slot in an object stream, or free. */
type Loc = { at: number } | { stm: number; idx: number } | 'free';
interface Section {
  trailer: Dict;
  find(num: number): Loc | undefined;
}
type Read = (pos: number, len: number) => Buffer;

const TRUNCATED = new Error('value runs past the read window');
const BAD = new Error('unsupported or malformed PDF structure');
const WINDOWS = [4096, 65536, 1 << 20];
const MAX_STREAM = 8 << 20;
const MAX_SECTIONS = 32;
const WS = new Set([0, 9, 10, 12, 13, 32]);
const DELIM = new Set([...'()<>[]{}/%'].map((ch) => ch.charCodeAt(0)));

/** The page count from the file's own structures, or null when qpdf must decide. Never throws. */
export function readPageCount(file: string): number | null {
  let fd: number | undefined;
  try {
    fd = openSync(file, 'r');
    const size = fstatSync(fd).size;
    const handle = fd;
    // Positioned read of up to len bytes; shorter at end of file.
    const read: Read = (pos, len) => {
      const buf = Buffer.allocUnsafe(Math.max(0, Math.min(len, size - pos)));
      return buf.subarray(0, readSync(handle, buf, 0, buf.length, pos));
    };
    return pageCount(read, size);
  } catch {
    return null;
  } finally {
    if (fd !== undefined) closeSync(fd);
  }
}

// Walks header → startxref → xref chain → catalog → page tree root.
function pageCount(read: Read, size: number): number {
  if (read(0, 1024).indexOf('%PDF-') < 0) throw BAD;
  const tail = read(Math.max(0, size - 1024), 1024).toString('latin1');
  const m = /startxref\s+(\d+)\s+%%EOF\s*$/.exec(tail.slice(tail.lastIndexOf('startxref')));
  if (!m) throw BAD;
  const chain = xrefChain(read, Number(m[1]));
  const trailer = chain[0]!.trailer;
  if (trailer.has('/Encrypt')) throw BAD; // qpdf decides whether it can open it
  const resolver = createResolver(read, chain);
  const root = trailer.get('/Root');
  if (!isRef(root)) throw BAD;
  const catalog = resolver.deref(root);
  if (!(catalog instanceof Map)) throw BAD;
  const pages = resolver.deref(catalog.get('/Pages') ?? null);
  if (!(pages instanceof Map)) throw BAD;
  const count = pages.get('/Count');
  if (typeof count !== 'number' || !Number.isInteger(count) || count < 0) throw BAD;
  return count;
}

// ---- lexer / parser ----------------------------------------------------------------------------------

// Skips whitespace and comments; running off the buffer means the window was too small.
function skipWs(c: Cursor): void {
  for (;;) {
    if (c.pos >= c.buf.length) throw TRUNCATED;
    const b = c.buf[c.pos]!;
    if (WS.has(b)) c.pos++;
    else if (b === 0x25) while (c.pos < c.buf.length && c.buf[c.pos] !== 10 && c.buf[c.pos] !== 13) c.pos++;
    else return;
  }
}

// Reads a regular token (number, keyword or name body) up to the next delimiter or whitespace.
function word(c: Cursor): string {
  const start = c.pos;
  while (c.pos < c.buf.length && !WS.has(c.buf[c.pos]!) && !DELIM.has(c.buf[c.pos]!)) c.pos++;
  if (c.pos >= c.buf.length) throw TRUNCATED;
  return c.buf.toString('latin1', start, c.pos);
}

// Parses one value: dictionary, array, name, number, indirect reference, string, boolean or null.
function value(c: Cursor): Val {
  skipWs(c);
  const b = c.buf[c.pos]!;
  if (b === 0x3c && c.buf[c.pos + 1] === 0x3c) {
    c.pos += 2;
    const d: Dict = new Map();
    for (;;) {
      skipWs(c);
      if (c.buf[c.pos] === 0x3e && c.buf[c.pos + 1] === 0x3e) {
        c.pos += 2;
        return d;
      }
      const key = value(c);
      if (typeof key !== 'string') throw BAD;
      d.set(key, value(c));
    }
  }
  if (b === 0x5b) {
    c.pos++;
    const a: Val[] = [];
    for (;;) {
      skipWs(c);
      if (c.buf[c.pos] === 0x5d) {
        c.pos++;
        return a;
      }
      a.push(value(c));
    }
  }
  if (b === 0x2f) {
    c.pos++;
    return `/${word(c)}`;
  }
  if (b === 0x28) {
    // Literal string: balanced parentheses, backslash escapes. Content is not needed.
    for (let depth = 0; ; ) {
      if (c.pos >= c.buf.length) throw TRUNCATED;
      const ch = c.buf[c.pos++];
      if (ch === 0x5c) c.pos++;
      else if (ch === 0x28) depth++;
      else if (ch === 0x29 && --depth === 0) return null;
    }
  }
  if (b === 0x3c) {
    const end = c.buf.indexOf(0x3e, c.pos);
    if (end < 0) throw TRUNCATED;
    c.pos = end + 1;
    return null;
  }
  const w = word(c);
  if (w === 'true' || w === 'false') return w === 'true';
  if (w === 'null') return null;
  if (!/^[+-]?(\d+\.?\d*|\.\d+)$/.test(w)) throw BAD; // a stray keyword or delimiter
  const n = Number(w);
  // An integer followed by "<gen> R" is an indirect reference.
  if (/^\d+$/.test(w)) {
    const save = c.pos;
    skipWs(c);
    const gen = word(c);
    if (/^\d+$/.test(gen)) {
      skipWs(c);
      if (c.buf[c.pos] === 0x52 && c.pos + 1 < c.buf.length && (WS.has(c.buf[c.pos + 1]!) || DELIM.has(c.buf[c.pos + 1]!))) {
        c.pos++;
        return { ref: n };
      }
    }
    c.pos = save;
  }
  return n;
}

// Narrows a value to an indirect reference.
function isRef(v: Val | undefined): v is Ref {
  return typeof v === 'object' && v !== null && !Array.isArray(v) && !(v instanceof Map);
}

// Reads a non-negative integer field from a dictionary.
function int(d: Dict, key: string, fallback?: number): number {
  const v = d.get(key) ?? fallback;
  if (typeof v !== 'number' || !Number.isInteger(v) || v < 0) throw BAD;
  return v;
}

// Runs a parse on a window of the file at pos, growing the window when the value runs past its end.
function parseAt<T>(read: Read, pos: number, fn: (c: Cursor) => T): T {
  for (const size of WINDOWS) {
    const buf = read(pos, size);
    try {
      return fn({ buf, pos: 0 });
    } catch (err) {
      if (err !== TRUNCATED || buf.length < size) throw err === TRUNCATED ? BAD : err;
    }
  }
  throw BAD;
}

// Parses "<num> <gen> obj <value>" at a file offset. Checks the object number (when known), so a wrong
// offset fails here instead of returning another object. For a stream, also returns where its data starts.
function objectAt(read: Read, offset: number, num?: number): { val: Val; dataAt?: number } {
  return parseAt(read, offset, (c) => {
    const n = value(c);
    value(c); // generation
    skipWs(c);
    if (word(c) !== 'obj' || (num !== undefined && n !== num)) throw BAD;
    const val = value(c);
    skipWs(c);
    if (!(val instanceof Map) || word(c) !== 'stream') return { val };
    if (c.buf[c.pos] === 0x0d) c.pos++;
    if (c.buf[c.pos] === 0x0a) c.pos++;
    return { val, dataAt: offset + c.pos };
  });
}

// ---- streams ------------------------------------------------------------------------------------------

// Decodes stream bytes: no filter or Flate, plus PNG predictors (what xref and object streams use).
function decode(dict: Dict, raw: Buffer): Buffer {
  let filter = dict.get('/Filter') ?? null;
  let parms = dict.get('/DecodeParms') ?? null;
  if (Array.isArray(filter)) {
    if (filter.length > 1) throw BAD;
    filter = filter[0] ?? null;
    parms = Array.isArray(parms) ? (parms[0] ?? null) : parms;
  }
  if (filter === null) return raw;
  if (filter !== '/FlateDecode') throw BAD;
  const data = inflateSync(raw, { maxOutputLength: MAX_STREAM });
  const p = parms instanceof Map ? parms : new Map<string, Val>();
  const predictor = int(p, '/Predictor', 1);
  if (predictor === 1) return data;
  if (predictor < 10 || int(p, '/Colors', 1) !== 1 || int(p, '/BitsPerComponent', 8) !== 8) throw BAD;
  return unpng(data, int(p, '/Columns', 1));
}

// Undoes PNG row filters: each row is a filter-type byte followed by `cols` bytes (1 byte per pixel).
function unpng(data: Buffer, cols: number): Buffer {
  const rows = Math.floor(data.length / (cols + 1));
  const out = Buffer.alloc(rows * cols);
  for (let r = 0; r < rows; r++) {
    const type = data[r * (cols + 1)];
    for (let i = 0; i < cols; i++) {
      const x = data[r * (cols + 1) + 1 + i]!;
      const a = i > 0 ? out[r * cols + i - 1]! : 0;
      const b = r > 0 ? out[(r - 1) * cols + i]! : 0;
      const ul = r > 0 && i > 0 ? out[(r - 1) * cols + i - 1]! : 0;
      let v: number;
      if (type === 0) v = x;
      else if (type === 1) v = x + a;
      else if (type === 2) v = x + b;
      else if (type === 3) v = x + ((a + b) >> 1);
      else if (type === 4) {
        const pa = Math.abs(b - ul);
        const pb = Math.abs(a - ul);
        const pc = Math.abs(a + b - 2 * ul);
        v = x + (pa <= pb && pa <= pc ? a : pb <= pc ? b : ul);
      } else throw BAD;
      out[r * cols + i] = v & 0xff;
    }
  }
  return out;
}

// ---- xref ---------------------------------------------------------------------------------------------

// Loads the xref chain from startxref, newest first: each section, then its /XRefStm (hybrid files), then /Prev.
function xrefChain(read: Read, start: number): Section[] {
  const chain: Section[] = [];
  const seen = new Set<number>();
  // Loads one section, refusing cycles and absurdly long chains.
  const load = (off: number) => {
    if (seen.has(off) || seen.size >= MAX_SECTIONS) throw BAD;
    seen.add(off);
    const s = read(off, 4).toString('latin1') === 'xref' ? tableAt(read, off) : xrefStreamAt(read, off);
    chain.push(s);
    return s;
  };
  for (let off: number | undefined = start; off !== undefined; ) {
    const s = load(off);
    const stm = s.trailer.get('/XRefStm');
    if (typeof stm === 'number') load(stm);
    const prev = s.trailer.get('/Prev');
    off = typeof prev === 'number' ? prev : undefined;
  }
  return chain;
}

// Parses a classic xref table and its trailer. Entries are a fixed 20 bytes, so lookups compute the
// entry's offset and read it; the table itself is never read whole.
function tableAt(read: Read, offset: number): Section {
  const subs: { start: number; count: number; at: number }[] = [];
  let pos = offset + 4;
  for (;;) {
    if (subs.length > 10_000) throw BAD;
    // Reads either a "start count" subsection header or the trailer dictionary.
    const step = parseAt(read, pos, (c) => {
      skipWs(c);
      const at = c.pos;
      if (word(c) === 'trailer') return { trailer: value(c) };
      c.pos = at;
      const start = value(c);
      const count = value(c);
      if (typeof start !== 'number' || typeof count !== 'number' || !Number.isInteger(start) || !Number.isInteger(count) || start < 0 || count < 0) throw BAD;
      skipWs(c); // stops on the first entry's digit
      return { start, count, at: pos + c.pos };
    });
    if ('trailer' in step) {
      if (!(step.trailer instanceof Map)) throw BAD;
      return { trailer: step.trailer, find: (num) => findInTable(read, subs, num) };
    }
    subs.push(step);
    pos = step.at + step.count * 20;
  }
}

// Looks an object up in a classic table's subsections by reading its one 20-byte entry.
function findInTable(read: Read, subs: { start: number; count: number; at: number }[], num: number): Loc | undefined {
  const sub = subs.find((s) => num >= s.start && num < s.start + s.count);
  if (!sub) return undefined;
  const m = /^(\d{10}) (\d{5}) ([nf])/.exec(read(sub.at + (num - sub.start) * 20, 20).toString('latin1'));
  if (!m) throw BAD;
  return m[3] === 'f' ? 'free' : { at: Number(m[1]) };
}

// Parses a cross-reference stream (PDF 1.5+): its dictionary is the trailer, its rows are the entries.
function xrefStreamAt(read: Read, offset: number): Section {
  const { val: dict, dataAt } = objectAt(read, offset);
  if (!(dict instanceof Map) || dataAt === undefined || dict.get('/Type') !== '/XRef') throw BAD;
  const length = int(dict, '/Length');
  if (length > MAX_STREAM) throw BAD;
  const data = decode(dict, read(dataAt, length));
  const w = dict.get('/W');
  if (!Array.isArray(w) || w.length !== 3 || !w.every((x) => typeof x === 'number' && Number.isInteger(x) && x >= 0 && x <= 8)) throw BAD;
  const [w0, w1, w2] = w as number[];
  const index = dict.get('/Index') ?? [0, int(dict, '/Size')];
  if (!Array.isArray(index) || index.length % 2 !== 0 || !index.every((x) => typeof x === 'number')) throw BAD;
  const rowLen = w0! + w1! + w2!;
  // Reads a big-endian field of `width` bytes at `pos`.
  const field = (pos: number, width: number) => {
    let v = 0;
    for (let i = 0; i < width; i++) v = v * 256 + data[pos + i]!;
    return v;
  };
  return {
    trailer: dict,
    // Finds the object's row through the /Index subsections and decodes its type and fields.
    find(num) {
      let row = 0;
      for (let i = 0; i < index.length; i += 2) {
        const [start, count] = [index[i] as number, index[i + 1] as number];
        if (num >= start && num < start + count) {
          const at = (row + num - start) * rowLen;
          if (at + rowLen > data.length) throw BAD;
          const type = w0 === 0 ? 1 : field(at, w0!);
          const f2 = field(at + w0!, w1!);
          if (type === 0) return 'free';
          if (type === 1) return { at: f2 };
          if (type === 2) return { stm: f2, idx: field(at + w0! + w1!, w2!) };
          throw BAD;
        }
        row += count;
      }
      return undefined;
    },
  };
}

// ---- object resolution --------------------------------------------------------------------------------

// Resolves objects through the xref chain (newest entry wins), reading object streams as needed.
function createResolver(read: Read, chain: Section[]) {
  // Finds the newest xref entry for an object.
  const locate = (num: number): Loc => {
    for (const s of chain) {
      const loc = s.find(num);
      if (loc !== undefined) return loc;
    }
    throw BAD;
  };

  // Loads object `num`, from its file offset or from inside its object stream.
  function load(num: number, depth: number): Val {
    const loc = locate(num);
    if (loc === 'free') return null;
    if ('at' in loc) return objectAt(read, loc.at, num).val;
    // Object stream: a header of "num offset" pairs, then the objects from /First on.
    const stm = locate(loc.stm);
    if (stm === 'free' || !('at' in stm)) throw BAD; // object streams cannot nest
    const { val: dict, dataAt } = objectAt(read, stm.at, loc.stm);
    if (!(dict instanceof Map) || dataAt === undefined) throw BAD;
    const length = deref(dict.get('/Length') ?? null, depth + 1);
    if (typeof length !== 'number' || !Number.isInteger(length) || length < 0 || length > MAX_STREAM) throw BAD;
    // A sentinel token after the last object lets its final number finish parsing (the reference lookahead
    // reads one token past it); running off the end after that means malformed, not truncated.
    const data = Buffer.concat([decode(dict, read(dataAt, length)), Buffer.from(' endobj ')]);
    const n = int(dict, '/N');
    const first = int(dict, '/First');
    const c: Cursor = { buf: data, pos: 0 };
    for (let i = 0; i < n; i++) {
      const objNum = value(c);
      const off = value(c);
      if (typeof off !== 'number') throw BAD;
      if (objNum === num) {
        c.pos = first + off;
        return value(c);
      }
    }
    throw BAD;
  }

  // Follows indirect references to a direct value (bounded, so a reference cycle fails instead of looping).
  function deref(v: Val, depth = 0): Val {
    for (; isRef(v); depth++) {
      if (depth > 16) throw BAD;
      v = load(v.ref, depth);
    }
    return v;
  }

  return { deref: (v: Val) => deref(v) };
}

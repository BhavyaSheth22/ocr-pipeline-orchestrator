import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createQpdfPool, QpdfError } from '../src/pdf/qpdf-pool.js';

// The pool runs any binary; tests use node itself so they need no qpdf.
const NODE = process.execPath;
// Args that make node print `out` and exit with `code` after `delayMs`.
const script = (out: string, code = 0, delayMs = 0) => ['-e', `setTimeout(() => { process.stdout.write(${JSON.stringify(out)}); process.exit(${code}); }, ${delayMs})`];

test('concurrent tasks each get their own reply (ids correlate across workers)', async () => {
  const pool = createQpdfPool(2);
  // Reverse delays so replies arrive out of order relative to submission.
  const outs = await Promise.all(Array.from({ length: 40 }, (_, i) => pool.run(NODE, script(`r${i}`, 0, (40 - i) * 2))));
  assert.deepEqual(outs, Array.from({ length: 40 }, (_, i) => `r${i}`));
  await pool.close();
});

test('a non-zero exit rejects with the exit code and stdout, like execFile', async () => {
  const pool = createQpdfPool(1);
  await assert.rejects(pool.run(NODE, script('7', 3)), (err: unknown) => {
    assert.ok(err instanceof QpdfError);
    assert.equal(err.code, 3);
    assert.equal(err.stdout, '7');
    return true;
  });
  await pool.close();
});

test('a missing binary rejects with ENOENT and the pool keeps working', async () => {
  const pool = createQpdfPool(1);
  await assert.rejects(pool.run('/nonexistent/qpdf', []), (err: unknown) => err instanceof QpdfError && err.code === 'ENOENT');
  assert.equal(await pool.run(NODE, script('ok')), 'ok');
  await pool.close();
});

test('stopping workers rejects their outstanding tasks (no caller waits forever), then new work starts fresh workers', async () => {
  const pool = createQpdfPool(2);
  const pending = Promise.allSettled(Array.from({ length: 4 }, () => pool.run(NODE, script('late', 0, 5_000))));
  await new Promise((r) => setTimeout(r, 200)); // let the tasks reach the workers
  await pool.close();
  const settled = await pending;
  assert.ok(settled.every((s) => s.status === 'rejected' && s.reason instanceof QpdfError));
  assert.equal(await pool.run(NODE, script('again')), 'again');
  await pool.close();
});

test('warm starts every worker and runs the binary once on each', async () => {
  const pool = createQpdfPool(2);
  await pool.warm(NODE); // `node --version`
  assert.equal(await pool.run(NODE, script('warm')), 'warm');
  await pool.close();
});

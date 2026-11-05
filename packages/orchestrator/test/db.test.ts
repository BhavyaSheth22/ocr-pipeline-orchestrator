import { test } from 'node:test';
import assert from 'node:assert/strict';
import { openDb, type Db } from '../src/db.js';

// Creates a job with n pages in the given db.
function addJob(db: Db, id: string, pages: number, window = 32, idemKey: string | null = null): void {
  db.createJob({ id, idemKey, traceId: 't-' + id, pdfPath: `/x/${id}.pdf`, totalPages: pages, window, header: { backlog_pages: 0, eta_s: 0 } });
}

// Parsed event payloads for a job.
const events = (db: Db, job: string) => db.events(job, 0, 1000).map((e) => ({ type: e.type, ...JSON.parse(e.data) }));

// A finished-page payload for tests.
const ok = { status: 'ok' as const, source: 'vlm' as const, low_confidence: false, result: {} };

test('createJob writes pages and a job header event with seq 1', () => {
  const db = openDb(':memory:');
  addJob(db, 'a', 3);
  const ev = events(db, 'a');
  assert.equal(ev.length, 1);
  assert.equal(ev[0].type, 'job');
  assert.equal(ev[0].seq, 1);
  assert.equal(ev[0].total_pages, 3);
  assert.equal(db.backlogPages(), 3);
});

test('claim goes page-index first across jobs (round robin) and sets a call key', () => {
  const db = openDb(':memory:');
  addJob(db, 'a', 2);
  addJob(db, 'b', 2);
  const order = [1, 2, 3, 4].map(() => {
    const t = db.claim('layout', 0)!;
    return `${t.job_id}${t.page}`;
  });
  assert.deepEqual(order, ['a1', 'b1', 'a2', 'b2']);
  assert.equal(db.claim('layout', 0), undefined);
  assert.equal(openDb(':memory:').claim('layout', 0), undefined);
});

test('call key encodes job, page, lane and attempt', () => {
  const db = openDb(':memory:');
  addJob(db, 'a', 1);
  assert.equal(db.claim('layout', 0)!.call_key, 'a:1:layout:1');
});

test('the window limits how far ahead of the watermark a job can run', () => {
  const db = openDb(':memory:');
  addJob(db, 'a', 5, 2);
  assert.equal(db.claim('layout', 0)!.page, 1);
  assert.equal(db.claim('layout', 0)!.page, 2);
  assert.equal(db.claim('layout', 0), undefined, 'page 3 is outside watermark 1 + window 2');
  db.advanceToVlm('a', 1, { boxes: [] }, {});
  db.claim('vlm', 0);
  db.finalize('vlm', 'a', 1, ok);
  assert.equal(db.claim('layout', 0)!.page, 3, 'watermark moved to 2');
});

test('not_before delays a page and backoff keeps attempts and call key', () => {
  const db = openDb(':memory:');
  addJob(db, 'a', 1);
  db.claim('layout', 0);
  assert.equal(db.backoff('layout', 'a', 1, 1000), true);
  assert.equal(db.claim('layout', 999), undefined);
  assert.equal(db.nextNotBefore('layout'), 1000);
  const t = db.claim('layout', 1000)!;
  assert.equal(t.attempts, 0);
  assert.equal(t.call_key, 'a:1:layout:1');
});

test('a real failure spends an attempt and gets a fresh call key', () => {
  const db = openDb(':memory:');
  addJob(db, 'a', 1);
  db.claim('layout', 0);
  assert.equal(db.fail('layout', 'a', 1, 0), 1);
  assert.equal(db.claim('layout', 0)!.call_key, 'a:1:layout:2');
});

test('stale outcomes are ignored (guarded by expected state)', () => {
  const db = openDb(':memory:');
  addJob(db, 'a', 1);
  db.claim('layout', 0);
  assert.equal(db.advanceToVlm('a', 1, { boxes: [] }, {}), true);
  assert.equal(db.advanceToVlm('a', 1, { boxes: [] }, {}), false, 'duplicate layout completion');
  assert.equal(db.fail('layout', 'a', 1, 0), undefined);
  assert.equal(db.finalize('vlm', 'a', 1, ok), undefined, 'not claimed by vlm yet');
  db.claim('vlm', 0);
  assert.deepEqual(db.finalize('vlm', 'a', 1, ok), { completed: true });
  assert.equal(db.finalize('vlm', 'a', 1, ok), undefined, 'duplicate final');
  const finals = events(db, 'a').filter((e) => e.stage === 'final');
  assert.equal(finals.length, 1, 'exactly one final event per page');
});

test('one event per transition, gap-free seqs, watermark and job.completed last', () => {
  const db = openDb(':memory:');
  addJob(db, 'a', 2);
  db.claim('layout', 0);
  db.claim('layout', 0);
  db.advanceToVlm('a', 2, { boxes: [] }, { p: 2 });
  db.advanceToVlm('a', 1, null, null); // layout exhausted: no partial event
  db.claim('vlm', 0); // page 1 (page index order)
  db.claim('vlm', 0);
  db.finalize('vlm', 'a', 2, ok);
  db.finalize('vlm', 'a', 1, { ...ok, low_confidence: true, source: 'layout' });
  const ev = events(db, 'a');
  assert.deepEqual(ev.map((e) => e.seq), [1, 2, 3, 4, 5]);
  assert.deepEqual(ev.map((e) => `${e.type}:${e.stage ?? ''}:${e.page ?? ''}`), [
    'job::', 'page:layout:2', 'page:final:2', 'page:final:1', 'job.completed::',
  ]);
  assert.equal(ev[2].watermark, 1, 'page 1 not done yet');
  assert.equal(ev[3].watermark, 3, 'all pages done');
  assert.equal(ev[4].low_confidence_pages, 1);
  assert.equal(db.getJob('a')!.state, 'done');
  assert.equal(db.backlogPages(), 0);
});

test('recovery resets running pages, keeps call keys and leaves done pages alone', () => {
  const db = openDb(':memory:');
  addJob(db, 'a', 3);
  db.claim('layout', 0); // page 1 running
  db.claim('layout', 0); // page 2
  db.advanceToVlm('a', 2, { boxes: [] }, {});
  db.claim('vlm', 0); // page 2 vlm running
  db.backoff('layout', 'a', 1, 5000); // page 1 has a not_before
  db.claim('layout', 0); // page 3
  assert.equal(db.recover(), 2);
  const p1 = db.claim('layout', 0)!; // page 1 was ready (not running), so it keeps its not_before
  assert.equal(p1.page, 3, 'page 3 is ready again with its key');
  assert.equal(p1.call_key, 'a:3:layout:1');
  const v = db.claim('vlm', 0)!;
  assert.equal(v.page, 2);
  assert.equal(v.call_key, 'a:2:vlm:1');
  assert.equal(JSON.parse(v.layout_json!).boxes.length, 0, 'resumes at vlm with stored layout');
});

test('idempotency key on ingestion is unique and looked up', () => {
  const db = openDb(':memory:');
  addJob(db, 'a', 1, 32, 'upload-1');
  assert.equal(db.jobByIdemKey('upload-1')!.id, 'a');
  assert.throws(() => addJob(db, 'b', 1, 32, 'upload-1'), /UNIQUE/);
  assert.equal(db.getJob('b'), undefined, 'failed create rolled back');
});

test('purge removes finished jobs past the cutoff with their pages and events', () => {
  const db = openDb(':memory:');
  addJob(db, 'a', 1);
  addJob(db, 'b', 1);
  db.claim('layout', 0);
  db.advanceToVlm('a', 1, null, null);
  db.claim('vlm', 0);
  db.finalize('vlm', 'a', 1, ok);
  assert.deepEqual(db.purge(Date.now() + 1), ['a'], 'running job b is kept');
  assert.equal(db.events('a', 0).length, 0);
  assert.equal(db.getJob('b')!.state, 'running');
});

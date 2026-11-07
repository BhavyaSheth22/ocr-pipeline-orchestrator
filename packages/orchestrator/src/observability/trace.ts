import { randomBytes, randomUUID } from 'node:crypto';

// W3C Trace Context (https://www.w3.org/TR/trace-context/): `traceparent: 00-<trace-id>-<parent-id>-<flags>`.
// One trace id follows a job from the client's POST /jobs through every model call to the mock, so
// `grep <trace_id>` across both services' logs shows the whole story.

const TRACEPARENT = /^[0-9a-f]{2}-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}$/;
const ZERO_TRACE = '0'.repeat(32);
const ZERO_SPAN = '0'.repeat(16);

export interface TraceParent {
  traceId: string;
  parentId: string;
}

// Parses a traceparent header; null when absent or invalid (all-zero ids are invalid per the spec).
export function parseTraceparent(header: string | undefined): TraceParent | null {
  const m = header === undefined ? null : TRACEPARENT.exec(header.trim().toLowerCase());
  if (!m || m[1] === ZERO_TRACE || m[2] === ZERO_SPAN) return null;
  return { traceId: m[1]!, parentId: m[2]! };
}

// 32 hex chars. randomUUID draws from a cached entropy pool; randomBytes made a synchronous getrandom call
// per upload.
export function newTraceId(): string {
  return randomUUID().replaceAll('-', '');
}

// 16 hex chars: the id of one outgoing call, sent as the traceparent's parent-id.
export function newSpanId(): string {
  return randomBytes(8).toString('hex');
}

export function traceparent(traceId: string, spanId: string): string {
  return `00-${traceId}-${spanId}-01`;
}

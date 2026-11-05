// One model call: fetch with a timeout, Idempotency-Key and traceparent, classified into what the pipeline
// does next. The response body is always consumed or cancelled so keep-alive sockets are never leaked.

export type Outcome =
  | { kind: 'ok'; body: unknown; ms: number }
  | { kind: 'backpressure'; status: number; retryAfterMs: number }
  | { kind: 'failure'; status?: number; reason: string }
  | { kind: 'fatal'; status: number; reason: string };

export interface CallOptions {
  idempotencyKey: string;
  traceparent: string;
  timeoutMs: number;
}

// Retry-After wait: the mock's precise retry_after_ms when present, else the header (seconds), else 1 s.
function retryAfterMs(res: Response, body: { retry_after_ms?: unknown } | null): number {
  if (body && typeof body.retry_after_ms === 'number') return Math.max(1, body.retry_after_ms);
  const h = Number(res.headers.get('retry-after'));
  return Number.isFinite(h) && h > 0 ? h * 1000 : 1000;
}

// POSTs the payload and classifies the result. Never throws.
export async function callModel(url: string, payload: unknown, opts: CallOptions): Promise<Outcome> {
  const start = performance.now();
  let res: Response;
  try {
    res = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'idempotency-key': opts.idempotencyKey, traceparent: opts.traceparent },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(opts.timeoutMs),
    });
  } catch (err) {
    const name = (err as Error).name;
    return { kind: 'failure', reason: name === 'TimeoutError' ? 'timeout' : `network: ${(err as Error).message}` };
  }
  if (res.ok) {
    try {
      return { kind: 'ok', body: await res.json(), ms: performance.now() - start };
    } catch (err) {
      return { kind: 'failure', status: res.status, reason: `bad body: ${(err as Error).name}` };
    }
  }
  if (res.status === 429 || res.status === 503) {
    const body = (await res.json().catch(() => null)) as { retry_after_ms?: unknown } | null;
    return { kind: 'backpressure', status: res.status, retryAfterMs: retryAfterMs(res, body) };
  }
  await res.body?.cancel().catch(() => {});
  if (res.status >= 500) return { kind: 'failure', status: res.status, reason: `http ${res.status}` };
  return { kind: 'fatal', status: res.status, reason: `http ${res.status}` };
}

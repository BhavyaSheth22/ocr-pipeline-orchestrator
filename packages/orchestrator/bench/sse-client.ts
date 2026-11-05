// A minimal SSE client over fetch plus the reorder buffer a real client needs. Used by tests and the bench
// as the verifier: it proves pages can be assembled in order from an out-of-order stream.

export interface SseEvent {
  id: number | null;
  event: string;
  data: string;
}

// Yields parsed SSE events from a URL, resuming after lastEventId when given.
export async function* readSse(url: string, lastEventId?: number, signal?: AbortSignal): AsyncGenerator<SseEvent> {
  const headers: Record<string, string> = { accept: 'text/event-stream' };
  if (lastEventId !== undefined) headers['last-event-id'] = String(lastEventId);
  const res = await fetch(url, { headers, signal });
  if (!res.ok || !res.body) throw new Error(`SSE ${res.status}`);
  const decoder = new TextDecoder();
  let buf = '';
  for await (const chunk of res.body as unknown as AsyncIterable<Uint8Array>) {
    buf += decoder.decode(chunk, { stream: true });
    let idx: number;
    while ((idx = buf.indexOf('\n\n')) >= 0) {
      const block = buf.slice(0, idx);
      buf = buf.slice(idx + 2);
      let id: number | null = null;
      let event = 'message';
      const data: string[] = [];
      for (const line of block.split('\n')) {
        if (line.startsWith('id: ')) id = Number(line.slice(4));
        else if (line.startsWith('event: ')) event = line.slice(7);
        else if (line.startsWith('data: ')) data.push(line.slice(6));
      }
      if (data.length) yield { id, event, data: data.join('\n') };
    }
  }
}

// Client-side assembly: drop duplicates (seq ≤ last seen, after a reconnect), buffer final pages by number,
// and emit contiguously from the next expected page. The buffer never exceeds the job's window.
export class Assembler {
  lastSeq = 0;
  next = 1;
  totalPages = 0;
  maxBuffered = 0;
  done = false;
  readonly emitted: number[] = [];
  readonly arrival: number[] = [];
  readonly duplicates: number[] = [];
  private readonly buffer = new Map<number, unknown>();

  // Feeds one event; returns the pages that became emittable, in order.
  push(ev: SseEvent): number[] {
    if (ev.id !== null && ev.id <= this.lastSeq) {
      this.duplicates.push(ev.id);
      return [];
    }
    if (ev.id !== null) {
      if (ev.id !== this.lastSeq + 1) throw new Error(`gap: got seq ${ev.id} after ${this.lastSeq}`);
      this.lastSeq = ev.id;
    }
    const d = JSON.parse(ev.data) as { total_pages?: number; stage?: string; page?: number; result?: unknown };
    if (ev.event === 'job') this.totalPages = d.total_pages!;
    if (ev.event === 'job.completed') this.done = true;
    if (ev.event !== 'page' || d.stage !== 'final') return [];
    this.arrival.push(d.page!);
    this.buffer.set(d.page!, d.result);
    this.maxBuffered = Math.max(this.maxBuffered, this.buffer.size);
    const out: number[] = [];
    while (this.buffer.has(this.next)) {
      this.buffer.delete(this.next);
      out.push(this.next++);
    }
    this.emitted.push(...out);
    return out;
  }
}

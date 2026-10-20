import type { LoggedEvent, LogPage } from "./contracts";

type Event = LoggedEvent & { [key: string]: unknown };

export class EventHub {
  private readonly listeners = new Set<() => void>();
  private revision = 0;
  private highest = -1;
  private tail: LoggedEvent[] = [];

  constructor(private readonly retained: () => number) {}

  get version(): number { return this.revision; }

  notify(): void {
    this.revision++;
    for (const resolve of [...this.listeners]) resolve();
  }

  publish(events: LoggedEvent[]): void {
    if (!events.length) return;
    const first = events[0].seq;
    const last = events[events.length - 1].seq;
    this.tail = this.highest < 0 || first === this.highest + 1 ? [...this.tail, ...events].slice(-this.retained()) : [];
    this.highest = Math.max(this.highest, last);
    this.notify();
  }

  recent(after: number, requestId?: string): LogPage | undefined {
    if (!this.tail.length || after < this.tail[0].seq - 1) return undefined;
    const events = this.tail.filter(event => event.seq > after && (requestId === undefined || event.request_id === requestId));
    return { events, next_cursor: Math.max(after, this.tail[this.tail.length - 1].seq), has_more: false };
  }

  async wait(milliseconds: number, version = this.revision): Promise<void> {
    if (version !== this.revision) return;
    let finish!: () => void;
    const ready = new Promise<void>(resolve => { finish = resolve; });
    const timer = setTimeout(finish, milliseconds);
    this.listeners.add(finish);
    try { await ready; } finally { clearTimeout(timer); this.listeners.delete(finish); }
  }

  stream(after: number, interval: number, read: (after: number) => Promise<LogPage>, finished?: (event: Event) => boolean): Response {
    let cursor = after;
    let closed = false;
    let complete = false;
    let release: (() => void) | undefined;
    const encoder = new TextEncoder();
    const body = new ReadableStream<Uint8Array>({
      pull: async controller => {
        try {
          if (complete) { controller.close(); closed = true; return; }
          while (!closed) {
            let timedOut = false;
            let finish!: () => void;
            const ready = new Promise<void>(resolve => { finish = resolve; });
            const timer = setTimeout(() => { timedOut = true; finish(); }, interval);
            release = () => { clearTimeout(timer); this.listeners.delete(finish); finish(); };
            this.listeners.add(finish);
            const page = await read(cursor);
            if (closed) { release(); release = undefined; return; }
            if (page.events.length) {
              release();
              release = undefined;
              cursor = page.next_cursor;
              complete = !!finished && page.events.some(event => finished(event as Event));
              const streamedAt = Date.now();
              controller.enqueue(encoder.encode(page.events.map(event => `id: ${event.seq}\ndata: ${JSON.stringify({ ...event, streamed_at: streamedAt })}\n\n`).join("")));
              return;
            }
            cursor = page.next_cursor;
            await ready;
            release();
            release = undefined;
            if (closed) return;
            if (timedOut) { controller.enqueue(encoder.encode(": keepalive\n\n")); return; }
          }
        } catch {
          release?.();
          release = undefined;
          if (!closed) controller.error(new Error("event stream unavailable"));
          closed = true;
        }
      },
      cancel: () => { closed = true; release?.(); release = undefined; },
    });
    return new Response(body, { headers: { "content-type": "text/event-stream", "cache-control": "no-store" } });
  }
}

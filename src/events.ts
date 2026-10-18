import { json } from "./contracts";

type Page = { events: { seq: number; [key: string]: unknown }[]; next_cursor: number; has_more: boolean };

export class EventHub {
  private readonly listeners = new Set<() => void>();
  private revision = 0;

  get version(): number { return this.revision; }

  notify(): void {
    this.revision++;
    for (const resolve of [...this.listeners]) resolve();
  }

  async wait(milliseconds: number, version = this.revision): Promise<void> {
    if (version !== this.revision) return;
    let finish!: () => void;
    const ready = new Promise<void>(resolve => { finish = resolve; });
    const timer = setTimeout(finish, milliseconds);
    this.listeners.add(finish);
    try { await ready; } finally { clearTimeout(timer); this.listeners.delete(finish); }
  }

  stream(after: number, interval: number, read: (after: number) => Page, allowed: () => boolean, finished?: (event: Page["events"][number]) => boolean): Response {
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
            if (!allowed()) { controller.close(); closed = true; return; }
            let timedOut = false;
            let finish!: () => void;
            const ready = new Promise<void>(resolve => { finish = resolve; });
            const timer = setTimeout(() => { timedOut = true; finish(); }, interval);
            release = () => { clearTimeout(timer); this.listeners.delete(finish); finish(); };
            this.listeners.add(finish);
            const page = read(cursor);
            if (page.events.length) {
              release();
              release = undefined;
              cursor = page.next_cursor;
              complete = !!finished && page.events.some(finished);
              controller.enqueue(encoder.encode(page.events.map(event => `id: ${event.seq}\ndata: ${JSON.stringify(event)}\n\n`).join("")));
              return;
            }
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

export function eventResponse(value: Page): Response { return json(value); }

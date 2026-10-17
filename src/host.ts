import module from "../dist/nanocode_worker.wasm";
import { CheckpointStore, StoreFailure } from "./store";

type Exports = {
  memory: WebAssembly.Memory;
  allocate(length: number): number;
  start(pointer: number, length: number): void;
  complete(id: number, pointer: number, length: number): void;
  complete_bytes(id: number, pointer: number, length: number): void;
  poll(): void;
};

export type Auth = { headers(request: unknown): Promise<[string, string][]> };

export async function runAgent(input: unknown, store: CheckpointStore, auth: Auth) {
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();
  const cancellations = new Map<number, () => void>();
  const bodies = new Map<number, { reader: ReadableStreamDefaultReader<Uint8Array>; controller: AbortController }>();
  let nextBody = 0;
  let stopped = false;
  let wasm: Exports;
  let resolve!: (value: any) => void;
  let reject!: (reason: unknown) => void;
  const result = new Promise<any>((yes, no) => { resolve = yes; reject = no; });
  const read = (pointer: number, length: number) => decoder.decode(new Uint8Array(wasm.memory.buffer, pointer, length));
  const write = (value: unknown) => {
    const bytes = encoder.encode(JSON.stringify(value));
    return writeBytes(bytes);
  };
  const writeBytes = (bytes: Uint8Array) => {
    const pointer = wasm.allocate(bytes.length);
    new Uint8Array(wasm.memory.buffer, pointer, bytes.length).set(bytes);
    return [pointer, bytes.length] as const;
  };
  const fail = (error: unknown) => { stopped = true; reject(error); };

  async function execute(id: number, request: any): Promise<unknown> {
    switch (request.op) {
      case "sleep": return new Promise(resolve => {
        const timer = setTimeout(() => resolve(null), request.milliseconds);
        cancellations.set(id, () => clearTimeout(timer));
      });
      case "auth_headers": return auth.headers(request);
      case "http_start": {
        const controller = new AbortController();
        cancellations.set(id, () => controller.abort());
        const response = await fetch(request.endpoint, { method: "POST", headers: request.headers, body: request.body, signal: controller.signal, redirect: "manual" });
        if (response.status >= 300 && response.status < 400) throw new Error("provider redirect rejected");
        if (!response.body) throw new Error("provider response has no body");
        const bodyId = ++nextBody;
        bodies.set(bodyId, { reader: response.body.getReader(), controller });
        return { id: bodyId, status: response.status, headers: [...response.headers] };
      }
      case "http_read": {
        const body = bodies.get(request.id);
        if (!body) throw new Error("provider response body is closed");
        cancellations.set(id, () => body.controller.abort());
        const chunk = await body.reader.read();
        if (chunk.done) {
          bodies.delete(request.id);
          body.reader.releaseLock();
          return null;
        }
        return chunk.value;
      }
      case "http_close": {
        const body = bodies.get(request.id);
        if (body) { body.controller.abort(); bodies.delete(request.id); }
        return null;
      }
      default: return store.execute(request);
    }
  }

  const instance = await WebAssembly.instantiate(module, { host: {
    dispatch(id: number, pointer: number, length: number) {
      const request = JSON.parse(read(pointer, length));
      void execute(id, request).then(ok => ({ ok }), error => ({ error: error instanceof StoreFailure
        ? { kind: error.kind, detail: error.message, expected: error.expected, actual: error.actual }
        : { kind: "backend", detail: error instanceof Error ? error.message : String(error) } })).then(reply => {
        cancellations.delete(id);
        if (stopped) return;
        if ("ok" in reply && reply.ok instanceof Uint8Array) wasm.complete_bytes(id, ...writeBytes(reply.ok));
        else wasm.complete(id, ...write(reply));
        wasm.poll();
      }).catch(fail);
    },
    cancel(id: number) { cancellations.get(id)?.(); cancellations.delete(id); },
    clock(monotonic: number) { return monotonic ? performance.now() : Date.now(); },
    random(pointer: number, length: number) { crypto.getRandomValues(new Uint8Array(wasm.memory.buffer, pointer, length)); },
    finished(pointer: number, length: number) { resolve(JSON.parse(read(pointer, length))); },
    panicked(pointer: number, length: number) { fail(new Error(read(pointer, length))); },
  } });
  wasm = instance.exports as unknown as Exports;
  try {
    wasm.start(...write(input));
    wasm.poll();
    return await result;
  } finally {
    stopped = true;
    for (const cancel of cancellations.values()) cancel();
    for (const body of bodies.values()) body.controller.abort();
  }
}

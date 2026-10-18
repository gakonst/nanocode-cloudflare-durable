import { Failure } from "./common";

function bounded<T>(pending: Promise<T>, milliseconds: number, signal: AbortSignal, expire: () => void, late?: (value: T) => void): Promise<T> {
  return new Promise((resolve, reject) => {
    let settled = false;
    const cleanup = () => { clearTimeout(timer); signal.removeEventListener("abort", aborted); };
    const aborted = () => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(new Failure(502, "upstream_cancelled"));
    };
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      cleanup();
      expire();
      reject(new Failure(504, "upstream_timeout"));
    }, milliseconds);
    pending.then(value => {
      if (settled) { try { late?.(value); } catch {} return; }
      settled = true;
      cleanup();
      resolve(value);
    }, error => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(error);
    });
    signal.addEventListener("abort", aborted, { once: true });
    if (signal.aborted) aborted();
  });
}

export async function modelFetch(request: Request, send: (request: Request) => Promise<Response>, headersMs: number, idleMs: number, timedOut: () => void): Promise<Response> {
  for (const value of [headersMs, idleMs]) if (!Number.isSafeInteger(value) || value <= 0 || value > 2_147_483_647) throw new Failure(500, "configuration_error");
  const controller = new AbortController();
  const abort = () => controller.abort();
  const expire = () => { timedOut(); abort(); };
  request.signal.addEventListener("abort", abort, { once: true });
  if (request.signal.aborted) abort();
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  const stop = () => {
    request.signal.removeEventListener("abort", abort);
    abort();
    if (reader) {
      const closing = reader;
      reader = undefined;
      void closing.cancel().catch(() => {}).then(() => { try { closing.releaseLock(); } catch {} });
    }
  };
  try {
    if (controller.signal.aborted) throw new Failure(502, "upstream_cancelled");
    const response = await bounded(send(new Request(request, { signal: controller.signal })), headersMs, controller.signal, expire, late => { void late.body?.cancel().catch(() => {}); });
    if (!response.body) { stop(); return response; }
    reader = response.body.getReader();
    const body = new ReadableStream<Uint8Array>({
      async pull(sink) {
        if (!reader) return;
        try {
          const next = await bounded(reader.read(), idleMs, controller.signal, expire);
          if (next.done) { stop(); sink.close(); }
          else sink.enqueue(next.value);
        } catch (error) {
          stop();
          sink.error(error instanceof Failure ? error : new Failure(502, "upstream_unavailable"));
        }
      },
      cancel() { stop(); },
    });
    return new Response(body, { status: response.status, statusText: response.statusText, headers: response.headers });
  } catch (error) {
    stop();
    throw error instanceof Failure ? error : new Failure(502, "upstream_unavailable");
  }
}

import module from "../dist/nanocode_worker.wasm";
import type { AgentConfiguration, AgentInput, AgentResult, RuntimeOptions } from "./contracts";
import { CheckpointStore, StoreFailure } from "./store";

type Exports = {
  memory: WebAssembly.Memory;
  allocate(length: number): number;
  command(id: number, pointer: number, length: number): void;
  complete(id: number, pointer: number, length: number): void;
  complete_bytes(id: number, pointer: number, length: number): void;
  poll(): void;
};

type Pending = { resolve(value: unknown): void; reject(reason: unknown): void };

export type TurnOutcome = AgentResult & { waiting?: boolean };

export type RuntimeActivity = { pending: string[]; quiescent: boolean; activity: unknown };

export type RuntimeMetrics = {
  initializations: number;
  runs: number;
  completed: number;
  failed: number;
  cancelled: number;
  retries: number;
  waits: number;
  resumes: number;
  rejected_runs: number;
  cancellations: number;
  host_calls: number;
  store_calls: number;
  model_requests: number;
  body_reads: number;
  event_batches: number;
  events: number;
  host_cancellations: number;
  dropped_replies: number;
  disposals: number;
};

const STORE_OPERATIONS = new Set(["peek", "read_record", "read_records", "list_states", "acquire", "replace"]);

export class AgentRuntime {
  private wasm!: Exports;
  private readonly encoder = new TextEncoder();
  private readonly decoder = new TextDecoder();
  private readonly inflight = new Map<number, () => void>();
  private readonly bodies = new Map<number, ReadableStreamDefaultReader<Uint8Array>>();
  private readonly commands = new Map<number, Pending>();
  private readonly watchers = new Set<() => void>();
  private readonly counters: RuntimeMetrics = {
    initializations: 0, runs: 0, completed: 0, failed: 0, cancelled: 0, retries: 0, waits: 0, resumes: 0, rejected_runs: 0, cancellations: 0,
    host_calls: 0, store_calls: 0, model_requests: 0, body_reads: 0, event_batches: 0, events: 0,
    host_cancellations: 0, dropped_replies: 0, disposals: 0,
  };
  private nextBody = 0;
  private nextCommand = 0;
  private depth = 0;
  private stopped = false;
  private running = false;
  private failure?: Error;
  private disposal?: Promise<void>;
  private pendingAtOpen: string[] = [];

  private constructor(private readonly store: CheckpointStore, private readonly options: RuntimeOptions) {}

  static async create(config: AgentConfiguration, store: CheckpointStore, options: RuntimeOptions): Promise<AgentRuntime> {
    const runtime = new AgentRuntime(store, options);
    const instance = await WebAssembly.instantiate(module, { host: runtime.imports() });
    runtime.wasm = instance.exports as unknown as Exports;
    try {
      const opened = await runtime.command({ op: "init", config: { session_id: config.session_id, subscription: config.subscription, endpoint: config.endpoint, remote: config.remote ?? false } }) as { pending: string[] };
      runtime.pendingAtOpen = opened.pending;
    } catch (error) {
      runtime.terminate(error);
      throw error;
    }
    runtime.counters.initializations++;
    return runtime;
  }

  get metrics(): Readonly<RuntimeMetrics> {
    return { ...this.counters };
  }

  get closed(): boolean {
    return this.stopped || this.disposal !== undefined;
  }

  get opened(): readonly string[] {
    return this.pendingAtOpen;
  }

  run(input: AgentInput): Promise<TurnOutcome> {
    this.counters.runs++;
    return this.turn({ op: "run", input: { request_id: input.request_id, prompt: input.prompt } });
  }

  resume(request_id: string): Promise<TurnOutcome> {
    this.counters.resumes++;
    return this.turn({ op: "resume", request_id });
  }

  async cancel(request_id?: string): Promise<TurnOutcome> {
    if (this.closed) return { retry: true };
    this.counters.cancellations++;
    return this.record(await this.command({ op: "cancel", request_id }) as TurnOutcome);
  }

  async pending(): Promise<RuntimeActivity> {
    return await this.command({ op: "pending" }) as RuntimeActivity;
  }

  async settle(): Promise<boolean> {
    return await this.command({ op: "settle" }) === true;
  }

  async revoke(): Promise<void> {
    await this.command({ op: "revoke" });
  }

  changed(signal?: AbortSignal): Promise<void> {
    if (this.stopped || signal?.aborted) return Promise.resolve();
    return new Promise(resolve => {
      const done = () => {
        this.watchers.delete(done);
        signal?.removeEventListener("abort", done);
        resolve();
      };
      this.watchers.add(done);
      signal?.addEventListener("abort", done, { once: true });
    });
  }

  private async turn(command: unknown): Promise<TurnOutcome> {
    if (this.closed) throw this.failure ?? new Error("agent runtime is closed");
    if (this.running) {
      this.counters.rejected_runs++;
      this.counters.waits++;
      return { waiting: true };
    }
    this.running = true;
    let result: TurnOutcome;
    try {
      result = await this.command(command) as TurnOutcome;
    } catch {
      result = { retry: true };
    } finally {
      this.running = false;
    }
    return this.record(result);
  }

  private async record(result: TurnOutcome): Promise<TurnOutcome> {
    if (result.ok) this.counters.completed++;
    else if (result.cancelled) this.counters.cancelled++;
    else if (result.retry) this.counters.retries++;
    else if (result.waiting) this.counters.waits++;
    else if (result.error !== undefined) this.counters.failed++;
    if (result.retry) await this.dispose().catch(() => undefined);
    return result;
  }

  dispose(): Promise<void> {
    this.disposal ??= this.shutdown();
    return this.disposal;
  }

  private async shutdown(): Promise<void> {
    this.counters.disposals++;
    let failure: unknown;
    if (!this.stopped) {
      try { await this.command({ op: "dispose" }); } catch (error) { failure = error; }
    }
    this.terminate(new Error("agent runtime disposed"));
    await this.options.remote?.dispose();
    if (failure !== undefined) throw failure;
  }

  private imports() {
    return {
      dispatch: (id: number, pointer: number, length: number) => {
        const request = JSON.parse(this.read(pointer, length));
        this.counters.host_calls++;
        this.inflight.set(id, () => {});
        void Promise.resolve()
          .then(() => this.execute(id, request))
          .then(ok => ({ ok }), error => ({ error: AgentRuntime.encodeError(error) }))
          .then(reply => this.complete(id, reply));
      },
      cancel: (id: number) => {
        const cancel = this.inflight.get(id);
        if (!cancel) return;
        this.inflight.delete(id);
        this.counters.host_cancellations++;
        cancel();
      },
      clock: (monotonic: number) => monotonic ? performance.now() : Date.now(),
      random: (pointer: number, length: number) => { crypto.getRandomValues(new Uint8Array(this.wasm.memory.buffer, pointer, length)); },
      reply: (id: number, pointer: number, length: number) => {
        const reply = JSON.parse(this.read(pointer, length));
        const pending = this.commands.get(id);
        if (!pending) return;
        this.commands.delete(id);
        if ("error" in reply) pending.reject(new Error(String(reply.error)));
        else pending.resolve(reply.ok);
        this.notify();
      },
      panicked: (pointer: number, length: number) => { this.fail(new Error(this.read(pointer, length))); },
    };
  }

  private onCancel(id: number, cancel: () => void) {
    if (this.inflight.has(id)) this.inflight.set(id, cancel);
    else cancel();
  }

  private live(id: number) {
    return !this.stopped && this.inflight.has(id);
  }

  private async execute(id: number, request: any): Promise<unknown> {
    if (!this.live(id)) return null;
    if (request.op.startsWith("remote_")) {
      if (!this.options.remote) throw new Error("remote executor is not configured");
      this.onCancel(id, () => this.options.remote!.cancel(id));
      return this.options.remote.execute(request, id);
    }
    switch (request.op) {
      case "sleep": return new Promise(resolve => {
        const timer = setTimeout(() => resolve(null), request.milliseconds);
        this.onCancel(id, () => { clearTimeout(timer); resolve(null); });
      });
      case "http_start": {
        this.counters.model_requests++;
        const controller = new AbortController();
        this.onCancel(id, () => controller.abort());
        const response = await this.options.dispatch({ endpoint: request.endpoint, headers: request.headers, body: request.body }, controller.signal);
        if (!this.live(id)) {
          void response.body?.cancel().catch(() => undefined);
          return null;
        }
        if (response.status >= 300 && response.status < 400) {
          void response.body?.cancel().catch(() => undefined);
          throw new Error("provider redirect rejected");
        }
        if (!response.body) throw new Error("provider response has no body");
        const body = ++this.nextBody;
        this.bodies.set(body, response.body.getReader());
        return { id: body, status: response.status, headers: [...response.headers] };
      }
      case "http_read": {
        const reader = this.bodies.get(request.id);
        if (!reader) throw new Error("provider response body is closed");
        this.counters.body_reads++;
        this.onCancel(id, () => {
          this.bodies.delete(request.id);
          void reader.cancel().catch(() => undefined);
        });
        const chunk = await reader.read();
        if (chunk.done) {
          this.bodies.delete(request.id);
          reader.releaseLock();
          return null;
        }
        return chunk.value;
      }
      case "http_close": {
        const reader = this.bodies.get(request.id);
        if (reader) {
          this.bodies.delete(request.id);
          await reader.cancel().catch(() => undefined);
        }
        return null;
      }
      case "events": {
        this.counters.event_batches++;
        if (this.options.events) {
          await this.options.events(request.events);
          this.counters.events += request.events.length;
        } else {
          for (const value of request.events) {
            if (this.stopped) throw new Error("agent runtime disposed");
            await this.options.event(value);
            this.counters.events++;
          }
        }
        this.notify();
        return null;
      }
      case "connection_catalog":
      case "connection_request": {
        if (!this.options.connection) throw new Error("account connections are not configured");
        return this.options.connection(request);
      }
      default: {
        if (!STORE_OPERATIONS.has(request.op)) throw new Error("unsupported host operation");
        this.counters.store_calls++;
        return this.store.execute(request);
      }
    }
  }

  private notify() {
    for (const watcher of [...this.watchers]) watcher();
  }

  private complete(id: number, reply: { ok: unknown } | { error: unknown }) {
    if (!this.live(id)) {
      this.counters.dropped_replies++;
      return;
    }
    this.inflight.delete(id);
    this.enter(() => {
      if ("ok" in reply && reply.ok instanceof Uint8Array) this.wasm.complete_bytes(id, ...this.writeBytes(reply.ok));
      else this.wasm.complete(id, ...this.write(reply));
    });
  }

  private command(value: unknown): Promise<unknown> {
    if (this.stopped) return Promise.reject(this.failure ?? new Error("agent runtime is closed"));
    const id = ++this.nextCommand;
    return new Promise((resolve, reject) => {
      this.commands.set(id, { resolve, reject });
      this.enter(() => this.wasm.command(id, ...this.write(value)));
    });
  }

  private enter(call: () => void) {
    if (this.depth > 0) {
      queueMicrotask(() => this.enter(call));
      return;
    }
    if (this.stopped) return;
    this.depth++;
    try {
      call();
      this.wasm.poll();
    } catch (error) {
      this.fail(error);
    } finally {
      this.depth--;
    }
  }

  private fail(error: unknown) {
    this.failure ??= error instanceof Error ? error : new Error(String(error));
    this.terminate(this.failure);
  }

  private terminate(reason: unknown) {
    if (this.stopped) return;
    this.stopped = true;
    const cancellations = [...this.inflight.values()];
    this.counters.host_cancellations += cancellations.length;
    this.inflight.clear();
    for (const cancel of cancellations) cancel();
    for (const reader of this.bodies.values()) void reader.cancel().catch(() => undefined);
    this.bodies.clear();
    const commands = [...this.commands.values()];
    this.commands.clear();
    for (const pending of commands) pending.reject(reason);
    this.notify();
  }

  private read(pointer: number, length: number) {
    return this.decoder.decode(new Uint8Array(this.wasm.memory.buffer, pointer, length));
  }

  private write(value: unknown) {
    return this.writeBytes(this.encoder.encode(JSON.stringify(value)));
  }

  private writeBytes(bytes: Uint8Array) {
    const pointer = this.wasm.allocate(bytes.length);
    new Uint8Array(this.wasm.memory.buffer, pointer, bytes.length).set(bytes);
    return [pointer, bytes.length] as const;
  }

  private static encodeError(error: unknown) {
    return error instanceof StoreFailure
      ? { kind: error.kind, detail: error.message, expected: error.expected, actual: error.actual }
      : { kind: "backend", detail: error instanceof Error ? error.message : String(error) };
  }
}

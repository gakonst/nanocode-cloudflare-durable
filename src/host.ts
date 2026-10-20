import module from "../dist/nanocode_worker.wasm";
import { RECENT_MODEL_LIMIT, type AgentConfiguration, type AgentInput, type AgentResult, type ModelTiming, type RuntimeOptions } from "./contracts";
import { CheckpointStore } from "./store";
import { Timings, type TimingSnapshot } from "./timing";

type Exports = {
  memory: WebAssembly.Memory;
  allocate(length: number): number;
  command(id: number, pointer: number, length: number): void;
  complete(id: number, pointer: number, length: number): void;
  complete_bytes(id: number, pointer: number, length: number): void;
  poll(): void;
};

type Pending = { resolve(value: unknown): void; reject(reason: unknown): void };

type Socket = { socket: WebSocket; idle: number; frames: (string | null)[]; closed?: string; waiter?: { id: number; resolve(value: unknown): void }; watchers: (() => void)[]; timing: ModelTiming };

export type TurnOutcome = AgentResult & { waiting?: boolean };

export type RuntimeActivity = { pending: string[]; quiescent: boolean; activity: unknown };

export type RuntimeMetrics = {
  wasm_memory_bytes: number;
  timing: { runtime_id: string; started_at: number; elapsed_ms: number; spans: TimingSnapshot; checkpoints: TimingSnapshot; models: ModelTiming[] };
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
  model_sockets: number;
  socket_sends: number;
  socket_frames: number;
  tool_calls: number;
  body_reads: number;
  event_batches: number;
  events: number;
  host_cancellations: number;
  dropped_replies: number;
  disposals: number;
  bridge_request_bytes: number;
  bridge_reply_bytes: number;
};

export class AgentRuntime {
  private readonly runtimeId = crypto.randomUUID();
  private readonly startedAt = Date.now();
  private readonly started = performance.now();
  private readonly timings = new Timings();
  private readonly checkpointTimings = new Timings();
  private readonly models: (ModelTiming & { runtime_id: string })[] = [];
  private wasm!: Exports;
  private readonly encoder = new TextEncoder();
  private readonly decoder = new TextDecoder();
  private readonly inflight = new Map<number, () => void>();
  private readonly bodies = new Map<number, { reader: ReadableStreamDefaultReader<Uint8Array>; timing: ModelTiming }>();
  private readonly sockets = new Map<number, Socket>();
  private readonly commands = new Map<number, Pending>();
  private readonly watchers = new Set<() => void>();
  private readonly counters: Omit<RuntimeMetrics, "timing" | "wasm_memory_bytes"> = {
    initializations: 0, runs: 0, completed: 0, failed: 0, cancelled: 0, retries: 0, waits: 0, resumes: 0, rejected_runs: 0, cancellations: 0,
    host_calls: 0, store_calls: 0, model_requests: 0, model_sockets: 0, socket_sends: 0, socket_frames: 0, tool_calls: 0, body_reads: 0, event_batches: 0, events: 0,
    host_cancellations: 0, dropped_replies: 0, disposals: 0,
    bridge_request_bytes: 0, bridge_reply_bytes: 0,
  };
  private nextBody = 0;
  private nextSocket = 0;
  private nextCommand = 0;
  private depth = 0;
  private stopped = false;
  private running = false;
  private failure?: Error;
  private disposal?: Promise<void>;
  private pendingAtOpen: string[] = [];
  private readonly attached = new Set<string>();
  hands = false;
  remote = false;
  subscription = false;

  private constructor(private readonly store: CheckpointStore, private readonly options: RuntimeOptions) {}

  static async create(config: AgentConfiguration, store: CheckpointStore, options: RuntimeOptions): Promise<AgentRuntime> {
    const runtime = new AgentRuntime(store, options);
    runtime.hands = config.hands ?? false;
    runtime.remote = config.remote ?? false;
    runtime.subscription = config.subscription;
    const instantiateStarted = performance.now();
    const instance = await WebAssembly.instantiate(module, { host: runtime.imports() });
    runtime.timings.record("wasm.instantiate", performance.now() - instantiateStarted);
    runtime.wasm = instance.exports as unknown as Exports;
    try {
      const initStarted = performance.now();
      const opened = await runtime.command({ op: "init", config: { session_id: config.session_id, provider: config.provider ?? "claude", subscription: config.subscription, endpoint: config.endpoint, remote: config.remote ?? false, executor: config.executor, hands: runtime.hands, event_coalesce_ms: config.event_coalesce_ms ?? 0, tools: config.tools ?? [] } }) as { pending: string[] };
      runtime.timings.record("sdk.open", performance.now() - initStarted);
      runtime.pendingAtOpen = opened.pending;
    } catch (error) {
      runtime.terminate(error);
      throw error;
    }
    runtime.counters.initializations++;
    return runtime;
  }

  get metrics(): Readonly<RuntimeMetrics> {
    return { ...this.counters, wasm_memory_bytes: this.wasm.memory.buffer.byteLength, timing: { runtime_id: this.runtimeId, started_at: this.startedAt, elapsed_ms: this.elapsed(), spans: this.timings.snapshot(), checkpoints: this.checkpointTimings.snapshot(), models: this.models.map(value => ({ ...value })) } };
  }

  get closed(): boolean {
    return this.stopped || this.disposal !== undefined;
  }

  get reusable(): boolean {
    return !this.closed && !this.options.remote?.failed?.();
  }

  get memoryBytes(): number {
    return this.wasm.memory.buffer.byteLength;
  }

  get opened(): readonly string[] {
    return this.pendingAtOpen;
  }

  run(input: AgentInput): Promise<TurnOutcome> {
    this.counters.runs++;
    return this.turn({ op: "run", input: { request_id: input.request_id, prompt: input.prompt, hand: input.hand } });
  }

  resume(request_id: string, hand?: string): Promise<TurnOutcome> {
    this.counters.resumes++;
    return this.turn({ op: "resume", request_id, hand });
  }

  async attach(hand: string, connection: string): Promise<void> {
    if (this.closed || this.attached.has(connection)) return;
    this.attached.add(connection);
    try {
      await this.command({ op: "attach", hand, connection });
    } catch (error) {
      this.attached.delete(connection);
      throw error;
    }
  }

  async detach(connection: string): Promise<void> {
    if (this.closed || !this.attached.delete(connection)) return;
    await this.command({ op: "detach", connection });
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

  async preconnect(): Promise<void> {
    await this.command({ op: "preconnect" });
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

  private accept(id: number, pointer: number, length: number, body?: Uint8Array) {
    const received = performance.now();
    const request = JSON.parse(this.read(pointer, length));
    if (body) request.body = body;
    this.counters.bridge_request_bytes += length + (body?.byteLength ?? 0);
    this.timings.record("bridge.decode", performance.now() - received);
    if (request.op === "events") request.events = request.events.map((event: object) => ({ ...event, host_timing: { runtime_id: this.runtimeId, received_ms: received - this.started } }));
    this.counters.host_calls++;
    this.inflight.set(id, () => {});
    void Promise.resolve()
      .then(async () => {
        const started = performance.now();
        this.timings.record(`queue.${request.op}`, started - received);
        try { return await this.execute(id, request); }
        finally { this.timings.record(`host.${request.op}`, performance.now() - started); }
      })
      .then(ok => ({ ok }), error => ({ error: AgentRuntime.encodeError(error) }))
      .then(reply => this.complete(id, reply));
  }

  private imports() {
    return {
      dispatch: (id: number, pointer: number, length: number) => this.accept(id, pointer, length),
      dispatch_body: (id: number, pointer: number, length: number, body: number, bodyLength: number) => this.accept(id, pointer, length, new Uint8Array(this.wasm.memory.buffer, body, bodyLength).slice()),
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
      case "wake":
        this.notify();
        this.options.wake?.();
        return null;
      case "sleep": return new Promise(resolve => {
        const timer = setTimeout(() => resolve(null), request.milliseconds);
        this.onCancel(id, () => { clearTimeout(timer); resolve(null); });
      });
      case "http_start": {
        this.counters.model_requests++;
        const controller = new AbortController();
        this.onCancel(id, () => controller.abort());
        const timing: ModelTiming & { runtime_id: string } = { runtime_id: this.runtimeId, dispatch_id: id, started_ms: this.elapsed(), body_read_wait_ms: 0, body_reads: 0, bytes: 0, max_chunk_gap_ms: 0 };
        this.models.push(timing);
        if (this.models.length > RECENT_MODEL_LIMIT) this.models.shift();
        let response: Response;
        try { response = await this.options.dispatch({ endpoint: request.endpoint, headers: request.headers, body: request.body }, controller.signal, timing); }
        catch (error) { this.finishModel(timing, controller.signal.aborted ? "cancelled" : "failed"); throw error; }
        timing.headers_ms = this.elapsed() - timing.started_ms;
        timing.status = response.status;
        if (!this.live(id)) {
          void response.body?.cancel().catch(() => undefined);
          this.finishModel(timing, "cancelled");
          return null;
        }
        if (response.status >= 300 && response.status < 400) {
          void response.body?.cancel().catch(() => undefined);
          this.finishModel(timing, "failed");
          throw new Error("provider redirect rejected");
        }
        if (!response.body) { this.finishModel(timing, "failed"); throw new Error("provider response has no body"); }
        const body = ++this.nextBody;
        this.bodies.set(body, { reader: response.body.getReader(), timing });
        return { id: body, status: response.status, headers: [...response.headers] };
      }
      case "http_read": {
        const body = this.bodies.get(request.id);
        if (!body) throw new Error("provider response body is closed");
        const { reader, timing } = body;
        this.counters.body_reads++;
        this.onCancel(id, () => {
          this.bodies.delete(request.id);
          this.finishModel(timing, "cancelled");
          void reader.cancel().catch(() => undefined);
        });
        const readStarted = performance.now();
        let chunk: ReadableStreamReadResult<Uint8Array>;
        try { chunk = await reader.read(); }
        catch (error) { this.finishModel(timing, "failed"); throw error; }
        finally { timing.body_read_wait_ms += performance.now() - readStarted; timing.body_reads++; }
        if (chunk.done) {
          this.bodies.delete(request.id);
          reader.releaseLock();
          this.finishModel(timing, "eof");
          return null;
        }
        const at = this.elapsed() - timing.started_ms;
        if (chunk.value.byteLength) timing.first_byte_ms ??= at;
        if (timing.last_byte_ms !== undefined) timing.max_chunk_gap_ms = Math.max(timing.max_chunk_gap_ms, at - timing.last_byte_ms);
        timing.last_byte_ms = at;
        timing.bytes += chunk.value.byteLength;
        return chunk.value;
      }
      case "http_close": {
        const body = this.bodies.get(request.id);
        if (body) {
          this.bodies.delete(request.id);
          this.finishModel(body.timing, "closed");
          await body.reader.cancel().catch(() => undefined);
        }
        return null;
      }
      case "tool_call": {
        if (!this.options.tools) throw new Error("host tools are not configured");
        this.counters.tool_calls++;
        const controller = new AbortController();
        this.onCancel(id, () => controller.abort());
        return await this.options.tools.call(request.name, request.input, controller.signal) ?? null;
      }
      case "socket_open": {
        if (!this.options.socket) throw new Error("model sockets are not configured");
        this.counters.model_sockets++;
        const controller = new AbortController();
        this.onCancel(id, () => controller.abort());
        const timing: ModelTiming & { runtime_id: string } = { runtime_id: this.runtimeId, dispatch_id: id, transport: "websocket", started_ms: this.elapsed(), body_read_wait_ms: 0, body_reads: 0, bytes: 0, max_chunk_gap_ms: 0, sends: 0 };
        this.models.push(timing);
        if (this.models.length > RECENT_MODEL_LIMIT) this.models.shift();
        let response: Response;
        try { response = await this.options.socket.open({ headers: request.headers }, controller.signal, timing); }
        catch (error) { this.finishModel(timing, controller.signal.aborted ? "cancelled" : "failed"); throw error; }
        timing.headers_ms = this.elapsed() - timing.started_ms;
        timing.status = response.status;
        const socket = response.webSocket;
        if (response.status !== 101 || !socket) {
          this.finishModel(timing, "failed");
          return { status: response.status, headers: [...response.headers], body: await response.text() };
        }
        socket.accept();
        if (!this.live(id)) {
          socket.close(1000, "handshake cancelled");
          this.finishModel(timing, "cancelled");
          return null;
        }
        const handle = ++this.nextSocket;
        const idle = Number(response.headers.get("x-nanocode-stream-idle-ms"));
        const entry: Socket = { socket, idle: Number.isSafeInteger(idle) && idle > 0 ? idle : 0, frames: [], watchers: [], timing };
        this.sockets.set(handle, entry);
        socket.addEventListener("message", event => this.receive(entry, event.data));
        socket.addEventListener("close", event => this.closeSocket(handle, `with code ${event.code}: ${event.reason}`, "eof"));
        socket.addEventListener("error", () => this.closeSocket(handle, "after a transport error", "failed"));
        return { id: handle, status: response.status, headers: [...response.headers] };
      }
      case "socket_send": {
        const entry = this.sockets.get(request.id);
        if (entry?.closed !== undefined) return { closed: entry.closed };
        if (!entry) return { closed: "after the client released it" };
        await this.options.socket!.admit();
        if (entry.closed !== undefined) return { closed: entry.closed };
        try { entry.socket.send(this.decoder.decode(request.body)); }
        catch { this.closeSocket(request.id, "before request dispatch", "failed"); return { closed: entry.closed }; }
        entry.timing.sends = (entry.timing.sends ?? 0) + 1;
        this.counters.socket_sends++;
        return null;
      }
      case "socket_next": {
        const entry = this.sockets.get(request.id);
        if (!entry) return { closed: "after the client released it" };
        if (entry.frames.length) return this.frame(entry);
        if (entry.closed !== undefined) return { closed: entry.closed };
        if (entry.waiter) throw new Error("socket_next is already pending");
        const readStarted = performance.now();
        try {
          return await new Promise(resolve => {
            const timer = entry.idle ? setTimeout(() => this.closeSocket(request.id, "after the stream idle deadline elapsed", "failed"), entry.idle) : null;
            entry.waiter = { id, resolve: value => { clearTimeout(timer); resolve(value); } };
            this.onCancel(id, () => {
              clearTimeout(timer);
              if (entry.waiter?.id === id) entry.waiter = undefined;
              resolve(null);
            });
          });
        } finally { entry.timing.body_read_wait_ms += performance.now() - readStarted; }
      }
      case "socket_wait": {
        const entry = this.sockets.get(request.id);
        if (!entry || entry.closed !== undefined) return null;
        return new Promise(resolve => {
          entry.watchers.push(() => resolve(null));
          this.onCancel(id, () => resolve(null));
        });
      }
      case "socket_close": {
        this.closeSocket(request.id, "by the client", "closed");
        this.sockets.delete(request.id);
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
      case "store": {
        this.counters.store_calls++;
        return this.store.transaction(request.operation, request.statements, this.checkpointTimings);
      }
      default: throw new Error("unsupported host operation");
    }
  }

  private notify() {
    for (const watcher of [...this.watchers]) watcher();
  }

  private receive(entry: Socket, data: unknown) {
    if (entry.closed !== undefined) return;
    const at = this.elapsed() - entry.timing.started_ms;
    entry.timing.first_byte_ms ??= at;
    entry.timing.last_byte_ms = at;
    entry.timing.body_reads++;
    this.counters.socket_frames++;
    entry.frames.push(typeof data === "string" ? data : null);
    const waiter = entry.waiter;
    if (!waiter) return;
    entry.waiter = undefined;
    waiter.resolve(this.frame(entry));
  }

  private frame(entry: Socket): unknown {
    const frame = entry.frames.shift();
    if (frame === null || frame === undefined) return { binary: true };
    const bytes = this.encoder.encode(frame);
    entry.timing.bytes += bytes.byteLength;
    return bytes;
  }

  private closeSocket(handle: number, detail: string, outcome: ModelTiming["outcome"]) {
    const entry = this.sockets.get(handle);
    if (!entry || entry.closed !== undefined) return;
    entry.closed = detail;
    this.finishModel(entry.timing, outcome);
    try { entry.socket.close(1000, "closed"); } catch {}
    const waiter = entry.waiter;
    entry.waiter = undefined;
    waiter?.resolve({ closed: detail });
    for (const watcher of entry.watchers.splice(0)) watcher();
  }

  releaseConnections(): void {
    this.closeSockets("released while the session is idle", "closed");
  }

  private closeSockets(detail: string, outcome: ModelTiming["outcome"]) {
    for (const handle of [...this.sockets.keys()]) this.closeSocket(handle, detail, outcome);
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
    const started = performance.now();
    try {
      call();
      this.wasm.poll();
    } catch (error) {
      this.fail(error);
    } finally {
      this.timings.record("wasm.enter", performance.now() - started);
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
    for (const { reader, timing } of this.bodies.values()) { this.finishModel(timing, "cancelled"); void reader.cancel().catch(() => undefined); }
    this.bodies.clear();
    this.closeSockets("agent runtime disposed", "cancelled");
    this.sockets.clear();
    const commands = [...this.commands.values()];
    this.commands.clear();
    for (const pending of commands) pending.reject(reason);
    this.notify();
  }

  private read(pointer: number, length: number) {
    return this.decoder.decode(new Uint8Array(this.wasm.memory.buffer, pointer, length));
  }

  private write(value: unknown) {
    const started = performance.now();
    try { return this.writeBytes(this.encoder.encode(JSON.stringify(value))); }
    finally { this.timings.record("bridge.encode", performance.now() - started); }
  }

  private writeBytes(bytes: Uint8Array) {
    const started = performance.now();
    this.counters.bridge_reply_bytes += bytes.byteLength;
    const pointer = this.wasm.allocate(bytes.length);
    new Uint8Array(this.wasm.memory.buffer, pointer, bytes.length).set(bytes);
    this.timings.record("bridge.copy", performance.now() - started);
    return [pointer, bytes.length] as const;
  }

  private static encodeError(error: unknown) {
    return { detail: error instanceof Error ? error.message : String(error) };
  }

  private elapsed(): number { return performance.now() - this.started; }

  private finishModel(timing: ModelTiming, outcome: ModelTiming["outcome"]): void {
    if (timing.outcome) return;
    timing.outcome = outcome;
    timing.duration_ms = this.elapsed() - timing.started_ms;
  }
}

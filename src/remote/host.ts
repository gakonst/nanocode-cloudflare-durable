import { headerJson, type PrivateBinding, type SessionAuthority } from "../contracts";

export type RemotePolicy = { max_frame_bytes: number; timeout_ms: number };

const encoder = new TextEncoder();

function validate(policy: RemotePolicy) {
  for (const key of ["max_frame_bytes", "timeout_ms"] as const) {
    if (!Number.isSafeInteger(policy[key]) || policy[key] <= 0) throw new Error(`remote ${key} must be a positive integer`);
  }
}

export class RemoteExecutor {
  #socket: WebSocket;
  #policy: RemotePolicy;
  #catalog: string | undefined;
  #name: string | undefined;
  #catalogWaiter: { resolve(): void; reject(error: Error): void } | undefined;
  #frames: string[] = [];
  #waiter: { id: number; resolve(value: unknown): void } | undefined;
  #failure: string | undefined;
  #owner: object | undefined;
  #closed!: (reason: string) => void;
  readonly closed = new Promise<string>(resolve => { this.#closed = resolve; });

  private constructor(socket: WebSocket, policy: RemotePolicy, listen: boolean) {
    this.#socket = socket;
    this.#policy = policy;
    if (!listen) return;
    socket.addEventListener("message", event => this.receive(event.data));
    socket.addEventListener("close", () => this.disconnected());
    socket.addEventListener("error", () => this.disconnected("runner connection failed"));
  }

  static hibernatable(socket: WebSocket, policy: RemotePolicy): RemoteExecutor {
    validate(policy);
    return new RemoteExecutor(socket, policy, false);
  }

  static restore(socket: WebSocket, policy: RemotePolicy, catalog: string, name: string): RemoteExecutor {
    const executor = RemoteExecutor.hibernatable(socket, policy);
    executor.#catalog = catalog;
    executor.#name = name;
    return executor;
  }

  async ready(): Promise<void> {
    let timer: number | undefined;
    try {
      await Promise.race([this.#catalogReady(), new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("hand catalog deadline elapsed")), this.#policy.timeout_ms); })]);
    } catch (error) {
      this.#fail(error instanceof Error ? error.message : String(error), 1011);
      throw error;
    } finally {
      if (timer !== undefined) clearTimeout(timer);
    }
  }

  static async create(binding: PrivateBinding, authority: SessionAuthority, policy: RemotePolicy): Promise<RemoteExecutor> {
    validate(policy);
    const controller = new AbortController();
    let expire!: (error: Error) => void;
    const deadline = new Promise<never>((_, reject) => { expire = reject; });
    const timer = setTimeout(() => {
      controller.abort();
      expire(new Error("remote executor connection deadline elapsed"));
    }, policy.timeout_ms);
    let executor: RemoteExecutor | undefined;
    try {
      const response = await Promise.race([binding.fetch(new Request("https://remote.internal/", { headers: { upgrade: "websocket", "x-nanocode-authority": headerJson(authority) }, signal: controller.signal })), deadline]);
      const socket = response.webSocket;
      if (response.status !== 101 || !socket) {
        const detail = await response.json<{ error?: string }>().then(body => body.error, () => undefined);
        throw new Error(`remote executor connection rejected with status ${response.status}${detail ? `: ${detail}` : ""}`);
      }
      executor = new RemoteExecutor(socket, policy, true);
      socket.accept();
      await Promise.race([executor.#catalogReady(), deadline]);
      return executor;
    } catch (error) {
      if (executor) executor.#fail(error instanceof Error ? error.message : String(error), 1011);
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }

  get failure(): string | undefined {
    return this.#failure;
  }

  get name(): string | undefined {
    return this.#name;
  }

  get catalog(): string | undefined {
    return this.#catalog;
  }

  get socket(): WebSocket {
    return this.#socket;
  }

  disconnected(reason = "runner disconnected"): void {
    this.#fail(reason, reason === "runner disconnected" ? 1000 : 1011);
  }

  async execute(request: any, id: number): Promise<unknown> {
    switch (request.op) {
      case "remote_catalog":
        return { frame: this.#catalog, max_frame_bytes: this.#policy.max_frame_bytes, timeout_ms: this.#policy.timeout_ms };
      case "remote_send":
        if (this.#failure !== undefined) throw new Error(this.#failure);
        for (const frame of request.frames as string[]) this.#socket.send(frame);
        return null;
      case "remote_poll":
        if (this.#frames.length) return { frames: this.#frames.splice(0) };
        if (this.#failure !== undefined) return { closed: this.#failure };
        if (this.#waiter) throw new Error("remote_poll is already pending");
        return new Promise(resolve => { this.#waiter = { id, resolve }; });
      case "remote_close":
        this.#fail(String(request.reason), 1000);
        return null;
      default:
        throw new Error(`unsupported remote operation ${request.op}`);
    }
  }

  claim(owner: object): void {
    if (this.#owner === owner) return;
    this.#owner = owner;
    const waiter = this.#waiter;
    this.#waiter = undefined;
    waiter?.resolve({ closed: "connection moved to a newer runtime" });
  }

  release(): void {
    this.#owner = {};
  }

  owns(owner: object): boolean {
    return this.#owner === owner;
  }

  cancel(id: number): void {
    if (this.#waiter?.id !== id) return;
    this.#waiter.resolve(null);
    this.#waiter = undefined;
  }

  async dispose(): Promise<void> {
    this.#fail("remote executor disposed", 1000);
  }

  #catalogReady(): Promise<void> {
    if (this.#catalog !== undefined) return Promise.resolve();
    if (this.#failure !== undefined) return Promise.reject(new Error(this.#failure));
    return new Promise((resolve, reject) => { this.#catalogWaiter = { resolve, reject }; });
  }

  receive(data: unknown): void {
    if (this.#failure !== undefined) return;
    if (typeof data !== "string") return this.#fail("remote code mode requires JSON text frames", 1003);
    if (encoder.encode(data).byteLength > this.#policy.max_frame_bytes) return this.#fail("remote code mode frame exceeds caller max_frame_bytes policy", 1009);
    if (this.#catalog === undefined) {
      let catalog: any;
      try { catalog = JSON.parse(data); } catch {}
      if (catalog?.type !== "catalog") return this.#fail("runner did not publish a code mode catalog", 1002);
      this.#catalog = data;
      if (typeof catalog.name === "string") this.#name = catalog.name;
      this.#catalogWaiter?.resolve();
      this.#catalogWaiter = undefined;
      return;
    }
    this.#frames.push(data);
    const waiter = this.#waiter;
    if (!waiter) return;
    this.#waiter = undefined;
    waiter.resolve({ frames: this.#frames.splice(0) });
  }

  #fail(reason: string, code: number) {
    if (this.#failure !== undefined) return;
    this.#failure = reason;
    this.#closed(reason);
    try { this.#socket.close(code); } catch {}
    this.#catalogWaiter?.reject(new Error(reason));
    this.#catalogWaiter = undefined;
    const waiter = this.#waiter;
    this.#waiter = undefined;
    waiter?.resolve({ closed: reason });
  }
}

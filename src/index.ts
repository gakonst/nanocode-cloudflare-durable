import { DurableObject } from "cloudflare:workers";
import { AgentRuntime } from "./host";
import { CheckpointStore } from "./store";
import { EventHub } from "./events";
import { RemoteExecutor } from "./remote/host";
import { DEFAULT_EXECUTOR, EXECUTOR_ID, RECENT_MODEL_LIMIT, headerJson, json, sessionUuid, type AgentResult, type HostTool, type HostToolCall, type ModelDispatch, type ModelProvider, type PrivateBinding, type SessionDescriptor } from "./contracts";
import { Timings, timingDifference, type TimingSnapshot } from "./timing";

export type { HostTool, HostToolCall, ModelDispatch, ModelProvider, PrivateBinding, SessionAuthority, SessionDescriptor, SocketDispatch } from "./contracts";

export interface NanocodeEnv {
  SESSIONS: DurableObjectNamespace;
  MODEL_AUTH?: string;
  RECOVERY_DELAY_MS: string;
  EVENT_STREAM_KEEPALIVE_MS: string;
  EVENT_PAGE_SIZE: string;
  EVENT_COALESCE_MS: string;
  MODEL_SOCKET_LINGER_MS: string;
  RUNTIME_MEMORY_BUDGET_BYTES: string;
  RUNNER_ENABLED?: string;
  REMOTE_MAX_FRAME_BYTES: string;
  REMOTE_TIMEOUT_MS: string;
  SESSION_LOCATION_HINT?: DurableObjectLocationHint;
}

export type Principal = { account: string; read_only?: boolean };

export type NanocodeOptions<E extends NanocodeEnv> = {
  authenticate(request: Request, env: E): Promise<string | Principal | null> | string | Principal | null;
  egress(env: E): PrivateBinding;
  subscription?(account: string, env: E, provider: ModelProvider): Promise<boolean> | boolean;
  recordSession?(account: string, name: string, env: E): Promise<boolean> | boolean;
  tools?: { definitions: HostTool[]; call(call: HostToolCall, env: E, signal: AbortSignal): Promise<unknown> };
};

type Job = { request_id: string; prompt: string; status: string; result: string | null; scope: string; descriptor: string; cancel_requested: number; first_seq: number | null; last_seq: number | null };
type Running = { job: Job; runtime: AgentRuntime };
type TurnTiming = { started: number; admitted_at?: number; admission_ms?: number; pump_wait_ms?: number; runtime_open_ms?: number; runtime_reused?: boolean; run_ms?: number; recovered?: boolean; checkpoints: TimingSnapshot; runtime?: TimingSnapshot };

function policy(value: string, name: string): number {
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number <= 0) throw new Error(`${name} must be a positive integer`);
  return number;
}

function duration(value: string, name: string): number {
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < 0) throw new Error(`${name} must be a nonnegative integer`);
  return number;
}

const handName = (value: unknown): value is string => typeof value === "string" && EXECUTOR_ID.test(value);
const HAND_TAG = "hand";
const WATCH_TAG = "watch";
type Watch = { watch: string; cursor: number };

export function createNanocode<E extends NanocodeEnv>(options: NanocodeOptions<E>) {
  class AgentSession extends DurableObject<E> {
    private readonly store: CheckpointStore;
    private readonly runtimes = new Map<string, Promise<AgentRuntime>>();
    private readonly residents = new Map<string, AgentRuntime>();
    private readonly hands = new Map<string, Map<string, { connection: string; executor: RemoteExecutor; ready: boolean }>>();
    private readonly connections = new Map<string, RemoteExecutor>();
    private readonly handSockets = new Map<WebSocket, RemoteExecutor>();
    private linger?: ReturnType<typeof setTimeout>;
    private watchersPending = false;
    private running?: Running;
    private pumping = false;
    private recorded = false;
    private deleted = false;
    private readonly eventHub = new EventHub();
    private readonly modelDiagnostics: { scope: string; value: unknown }[] = [];
    private readonly timings = new Map<string, Timings>();
    private readonly turnTimings = new Map<string, TurnTiming>();
    private readonly eventTurns = new Map<string, string>();
    private readonly counts = { runtime_initializations: 0, model_dispatches: 0, events_written: 0, event_turn_queries: 0, event_turn_cache_hits: 0, preparation_writes: 0, preparation_writes_skipped: 0, effect_syncs: 0, alarms: 0, direct_starts: 0, recoveries: 0, runtime_retirements: 0, host_tool_calls: 0 };

    constructor(ctx: DurableObjectState, env: E) {
      super(ctx, env);
      this.store = new CheckpointStore(ctx.storage);
      ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS jobs (request_id TEXT PRIMARY KEY, prompt TEXT NOT NULL, status TEXT NOT NULL, result TEXT);
        CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY, scope TEXT NOT NULL, request_id TEXT, body TEXT NOT NULL, created_at INTEGER NOT NULL);
        CREATE INDEX IF NOT EXISTS unfinished_jobs ON jobs(status);
        DROP INDEX IF EXISTS scoped_events;
        DROP INDEX IF EXISTS scoped_request_events;
        DROP TABLE IF EXISTS grant_fences;
        CREATE TABLE IF NOT EXISTS event_turns (scope TEXT NOT NULL, turn_id TEXT NOT NULL, request_id TEXT NOT NULL, PRIMARY KEY (scope, turn_id));
        CREATE TABLE IF NOT EXISTS hand_sockets (connection TEXT PRIMARY KEY, scope TEXT NOT NULL, hand TEXT NOT NULL, catalog TEXT NOT NULL) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS watch_sockets (id TEXT PRIMARY KEY, scope TEXT NOT NULL, descriptor TEXT NOT NULL) WITHOUT ROWID;
      `);
      ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
      const columns = new Set(ctx.storage.sql.exec("PRAGMA table_info(jobs)").toArray().map(row => row.name));
      if (!columns.has("scope")) ctx.storage.sql.exec("ALTER TABLE jobs ADD COLUMN scope TEXT NOT NULL DEFAULT 'owner'");
      if (!columns.has("descriptor")) ctx.storage.sql.exec("ALTER TABLE jobs ADD COLUMN descriptor TEXT NOT NULL DEFAULT '{}'");
      if (!columns.has("cancel_requested")) ctx.storage.sql.exec("ALTER TABLE jobs ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0");
      if (!columns.has("first_seq")) ctx.storage.sql.exec("ALTER TABLE jobs ADD COLUMN first_seq INTEGER");
      if (!columns.has("last_seq")) ctx.storage.sql.exec("ALTER TABLE jobs ADD COLUMN last_seq INTEGER");
      this.restoreHands();
      this.restoreWatchers();
    }

    async fetch(request: Request): Promise<Response> {
      const admitted = performance.now();
      if (this.deleted) return json({ error: "session deleted" }, 410);
      try {
        const url = new URL(request.url);
        let descriptor: SessionDescriptor;
        try { descriptor = JSON.parse(request.headers.get("x-nanocode-descriptor") ?? "null"); } catch { return json({ error: "invalid session authority" }, 400); }
        if (!descriptor || typeof descriptor.account_id !== "string" || !descriptor.account_id || typeof descriptor.session_name !== "string" || !descriptor.session_name) return json({ error: "session authority is required" }, 403);
        const binding = this.store.row("SELECT value FROM metadata WHERE key = 'authority'")?.value;
        const identity = JSON.stringify({ account_id: descriptor.account_id, session_name: descriptor.session_name });
        if (binding && binding !== identity) return json({ error: "session authority mismatch" }, 403);
        const scope = "owner";
        const parts = url.pathname.split("/");
        if (parts[2] === "delete" && request.method === "POST") return await this.destroy();
        if (descriptor.read_only && (request.method !== "GET" || (parts[2] === "hands" && request.headers.get("upgrade")?.toLowerCase() === "websocket"))) return json({ error: "read-only access" }, 403);
        if (!this.recorded && options.recordSession) {
          if (!this.store.row("SELECT value FROM metadata WHERE key = 'recorded'")) {
            if (!await options.recordSession(descriptor.account_id, descriptor.session_name, this.env)) {
              await this.destroy();
              return json({ error: "account not found" }, 403);
            }
            this.ctx.storage.sql.exec("INSERT INTO metadata VALUES ('recorded', ?) ON CONFLICT(key) DO NOTHING", String(Date.now()));
          }
          this.recorded = true;
        }
        if (request.method === "GET" && parts[2] === "metrics") return json({ ...this.counts, object_id: this.ctx.id.toString(), database_bytes: this.ctx.storage.sql.databaseSize, model_diagnostics: this.modelDiagnostics.filter(entry => entry.scope === scope).map(entry => entry.value), timing: this.timings.get(scope)?.snapshot() ?? {}, checkpoints: { ...this.store.metrics, timing: this.residents.get(scope)?.metrics.timing.checkpoints ?? {} }, resident_runtimes: this.runtimes.size, runtime: this.residents.get(scope)?.metrics, persisted: this.store.persisted(scope) });
        if (request.method === "GET" && parts[2] === "events") return this.events(scope, url, request.headers.get("accept")?.includes("text/event-stream"), descriptor);
        if (parts[2] === "socket") return this.watch(scope, url, request, descriptor);
        if (parts[2] === "hands") {
          if (request.method !== "GET") return json({ error: "method not allowed" }, 405);
          if (parts.length === 3 && request.headers.get("upgrade")?.toLowerCase() !== "websocket") return json({ hands: [...(this.hands.get(scope) ?? [])].filter(([, entry]) => entry.ready).map(([hand]) => hand) });
          return this.attachHand(scope, parts.length === 4 ? decodeURIComponent(parts[3]) : undefined, request, descriptor);
        }
        if (parts[2] === "cancel" && parts.length === 4 && request.method === "POST") return await this.cancel(decodeURIComponent(parts[3]), scope);
        if (parts[2] !== "turns") return json({ error: "route not found" }, 404);
        if (request.method === "GET" && parts.length === 4) {
          const job = this.job(decodeURIComponent(parts[3]));
          return job && job.scope === scope ? json(this.publicJob(job)) : json({ error: "request not found" }, 404);
        }
        if (request.method !== "POST" || parts.length !== 3) return json({ error: "method not allowed" }, 405);
        let input: any;
        try { input = await request.json(); } catch { return json({ error: "invalid JSON" }, 400); }
        if (typeof input?.request_id !== "string" || !input.request_id.trim() || typeof input.prompt !== "string" || !input.prompt.trim()) return json({ error: "request_id and prompt must be nonempty strings" }, 400);
        if (input.request_id.startsWith("subagent-completion:")) return json({ error: "request_id prefix is reserved" }, 400);
        if (input.provider !== undefined && input.provider !== "claude" && input.provider !== "codex") return json({ error: "provider must be claude or codex" }, 400);
        if (input.executor !== undefined && (typeof input.executor !== "string" || !EXECUTOR_ID.test(input.executor))) return json({ error: "invalid executor" }, 400);
        if (input.hand !== undefined && !handName(input.hand)) return json({ error: "hand must be the name of an attached hand" }, 400);
        const streaming = request.headers.get("accept")?.includes("text/event-stream");
        if (streaming) {
          const after = Number(url.searchParams.get("after") ?? "0");
          const limit = Number(url.searchParams.get("limit") ?? this.env.EVENT_PAGE_SIZE);
          if (!Number.isSafeInteger(after) || after < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > policy(this.env.EVENT_PAGE_SIZE, "EVENT_PAGE_SIZE")) return json({ error: "invalid event cursor" }, 400);
        }
        const executorKey = `executor:${scope}`;
        const storedExecutor = this.store.row("SELECT value FROM metadata WHERE key = ?", executorKey)?.value as string | undefined;
        const initialized = !!this.store.row("SELECT value FROM metadata WHERE key = ?", scope === "owner" ? "session_id" : `session_id:${scope}`);
        const legacyExecutor = this.env.RUNNER_ENABLED === "true" ? DEFAULT_EXECUTOR : "";
        const boundExecutor = storedExecutor ?? (initialized ? legacyExecutor : undefined);
        if (boundExecutor !== undefined && input.executor !== undefined && input.executor !== boundExecutor) return json({ error: "session executor is immutable; create a new session" }, 409);
        const executor = input.executor ?? boundExecutor ?? legacyExecutor;
        descriptor.executor_id = executor || undefined;
        descriptor.hand = input.hand;
        const providerKey = `model_provider:${scope}`;
        const boundProvider = this.store.row("SELECT value FROM metadata WHERE key = ?", providerKey)?.value
          ?? (this.store.row("SELECT value FROM metadata WHERE key = ?", scope === "owner" ? "session_id" : `session_id:${scope}`) ? "claude" : undefined);
        if (boundProvider && input.provider !== undefined && input.provider !== boundProvider) return json({ error: "session provider is immutable; create a new session" }, 409);
        descriptor.provider = input.provider ?? boundProvider ?? "claude";
        if (descriptor.provider === "codex" && descriptor.session_id && descriptor.session_id[14] !== "7") return json({ error: "Codex requires a new thread with a UUIDv7 session" }, 409);
        if (descriptor.provider === "codex") descriptor.subscription = descriptor.codex_subscription ?? true;
        const existing = this.job(input.request_id);
        if (existing && (existing.prompt !== input.prompt || existing.scope !== scope || this.descriptor(existing).hand !== input.hand)) return json({ error: "request_id already belongs to different input or authority" }, 409);
        if (!existing) {
          policy(this.env.RECOVERY_DELAY_MS, "RECOVERY_DELAY_MS");
          this.ctx.storage.transactionSync(() => {
            const current = this.store.row("SELECT value FROM metadata WHERE key = 'authority'")?.value;
            if (current && current !== identity) throw new Error("session authority mismatch");
            if (!current) this.ctx.storage.sql.exec("INSERT INTO metadata VALUES ('authority', ?)", identity);
            this.ctx.storage.sql.exec("INSERT INTO metadata VALUES (?, ?) ON CONFLICT(key) DO NOTHING", providerKey, descriptor.provider!);
            this.ctx.storage.sql.exec("INSERT INTO metadata VALUES (?, ?) ON CONFLICT(key) DO NOTHING", executorKey, executor);
            this.sessionId(scope, descriptor.session_id);
            this.ctx.storage.sql.exec("INSERT INTO metadata VALUES (?, ?) ON CONFLICT(key) DO NOTHING", `background:${scope}`, JSON.stringify(descriptor));
            const queued = this.insertEvent(scope, input.request_id, { type: "queued", request_id: input.request_id });
            this.ctx.storage.sql.exec("INSERT INTO jobs (request_id, prompt, status, result, scope, descriptor, cancel_requested, first_seq) VALUES (?, ?, 'queued', NULL, ?, ?, 0, ?)", input.request_id, input.prompt, scope, JSON.stringify(descriptor), queued);
          });
          this.turnTimings.set(input.request_id, { started: admitted, checkpoints: {} });
        }
        const job = existing ?? this.job(input.request_id)!;
        if ((job.status === "queued" || job.status === "running") && !this.pumping) await this.ctx.storage.setAlarm(Date.now() + policy(this.env.RECOVERY_DELAY_MS, "RECOVERY_DELAY_MS"));
        if (!existing) {
          const timing = this.turnTimings.get(input.request_id);
          if (timing) {
            timing.admission_ms = performance.now() - admitted;
            timing.admitted_at = performance.now();
          }
        }
        if ((job.status === "queued" || job.status === "running") && !this.pumping) {
          this.counts.direct_starts++;
          this.ctx.waitUntil(this.pump(true));
        }
        if (streaming) {
          const cursor = Number(url.searchParams.get("after") ?? "0");
          const terminal = !["queued", "running"].includes(job.status);
          const last = terminal ? job.last_seq ?? this.store.row("SELECT seq FROM events WHERE scope = ? AND request_id = ? ORDER BY seq DESC LIMIT 1", scope, input.request_id)?.seq : undefined;
          if (terminal && (last === undefined || cursor >= Number(last))) return new Response(null, { headers: { "content-type": "text/event-stream", "cache-control": "no-store" } });
          return this.events(scope, url, true, descriptor, input.request_id);
        }
        return json(this.publicJob(job), ["queued", "running"].includes(job.status) ? 202 : 200);
      } catch (error) {
        if (error instanceof URIError) return json({ error: "invalid request encoding" }, 400);
        return json({ error: "session operation failed" }, 503);
      }
    }

    async alarm(): Promise<void> {
      if (this.deleted) return;
      this.counts.alarms++;
      await this.pump();
    }

    private async pump(watchdogArmed = false): Promise<void> {
      const delay = policy(this.env.RECOVERY_DELAY_MS, "RECOVERY_DELAY_MS");
      if (this.deleted) return;
      if (this.pumping) { await this.ctx.storage.setAlarm(Date.now() + delay); return; }
      this.pumping = true;
      if (this.linger !== undefined) clearTimeout(this.linger);
      this.linger = undefined;
      let scope: string | undefined;
      let runtime: AgentRuntime | undefined;
      let immediate = false;
      let waiting = false;
      let activeJob: Job | undefined;
      try {
        if (!watchdogArmed) await this.ctx.storage.setAlarm(Date.now() + delay);
        const job = this.ctx.storage.sql.exec<Job>("SELECT * FROM jobs WHERE status IN ('queued', 'running') ORDER BY rowid LIMIT 1").toArray()[0];
        const background = this.store.row("SELECT key, value FROM metadata WHERE key LIKE 'background:%' ORDER BY key LIMIT 1");
        if (!job && !background) return;
        scope = job?.scope ?? String(background!.key).slice("background:".length);
        const descriptor: SessionDescriptor = job ? this.descriptor(job) : JSON.parse(String(background!.value));
        let sessionId!: string;
        let preparationRequired = !watchdogArmed;
        this.ctx.storage.transactionSync(() => {
          const serialized = JSON.stringify(descriptor);
          if (preparationRequired || background?.key !== `background:${scope}` || background.value !== serialized) {
            this.ctx.storage.sql.exec("INSERT INTO metadata VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", `background:${scope}`, serialized);
            preparationRequired = true;
          }
          sessionId = this.sessionId(scope!, descriptor.session_id, () => { preparationRequired = true; });
          if (job) {
            activeJob = job;
            if (job.status === "running") this.counts.recoveries++;
          }
        });
        const prepared = performance.now();
        if (preparationRequired) this.counts.preparation_writes++;
        else this.counts.preparation_writes_skipped++;
        let timing: TurnTiming | undefined;
        if (job) {
          timing = this.turnTimings.get(job.request_id);
          if (!timing) {
            timing = { started: prepared, recovered: true, checkpoints: {} };
            this.turnTimings.set(job.request_id, timing);
          }
          timing.pump_wait_ms = prepared - (timing.admitted_at ?? prepared);
          timing.runtime_reused = this.runtimes.has(scope);
        }
        const runtimeStarted = performance.now();
        const initializations = this.counts.runtime_initializations;
        runtime = await this.runtime(scope, sessionId, descriptor);
        if (timing) {
          timing.runtime_reused = initializations === this.counts.runtime_initializations;
          timing.runtime_open_ms = performance.now() - runtimeStarted;
          timing.runtime = timing.runtime_reused ? runtime.metrics.timing.spans : undefined;
          timing.checkpoints = timing.runtime_reused ? runtime.metrics.timing.checkpoints : {};
        }
        const state = await runtime.pending();
        const first = state.pending[0];
        const pendingJob = first ? this.job(first) : undefined;
        activeJob = pendingJob?.scope === scope && ["queued", "running"].includes(pendingJob.status) ? pendingJob : !first ? job : undefined;
        if (activeJob) {
          this.running = { job: activeJob, runtime };
          this.ctx.storage.transactionSync(() => {
            this.ctx.storage.sql.exec("UPDATE jobs SET status = 'running', result = NULL WHERE request_id = ?", activeJob!.request_id);
            this.insertEvent(scope!, activeJob!.request_id, { type: "running", request_id: activeJob!.request_id });
          });
          const turnTiming = this.turnTimings.get(activeJob.request_id);
          const hand = this.descriptor(activeJob).hand;
          const latest = this.job(activeJob.request_id)!;
          const runStarted = performance.now();
          const result = latest.cancel_requested ? await runtime.cancel(activeJob.request_id) : await runtime.run({ request_id: activeJob.request_id, prompt: activeJob.prompt, hand });
          if (turnTiming) turnTiming.run_ms = performance.now() - runStarted;
          if (result.retry) throw new Error("runtime recovery required");
          waiting = !!result.waiting;
          if (!result.waiting) this.finish(activeJob, result);
        } else if (first) {
          const result = await runtime.resume(first, pendingJob ? this.descriptor(pendingJob).hand : undefined);
          if (result.retry) throw new Error("runtime recovery required");
          waiting = !!result.waiting;
        }
        this.running = undefined;
        const version = this.eventHub.version;
        if (await runtime.settle()) {
          this.ctx.storage.sql.exec("DELETE FROM metadata WHERE key = ?", `background:${scope}`);
        }
        else if (waiting || !this.store.row("SELECT request_id FROM jobs WHERE status IN ('queued', 'running') LIMIT 1")) {
          await this.eventHub.wait(delay, version);
        }
        immediate = !!this.store.row("SELECT request_id FROM jobs WHERE status IN ('queued', 'running') LIMIT 1");
      } catch {
        if (activeJob) this.turnTimings.delete(activeJob.request_id);
        if (activeJob) this.ctx.storage.sql.exec("UPDATE jobs SET result = ? WHERE request_id = ? AND status = 'running'", JSON.stringify({ error: "Agent execution interrupted; recovery scheduled" }), activeJob.request_id);
        if (scope) { this.runtimes.delete(scope); this.residents.delete(scope); this.releaseHands(scope); }
        if (runtime) await runtime.dispose().catch(() => {});
      } finally {
        this.running = undefined;
        if (this.deleted) { this.pumping = false; return; }
        const unfinished = this.store.row("SELECT request_id FROM jobs WHERE status IN ('queued', 'running') LIMIT 1");
        const background = this.store.row("SELECT key FROM metadata WHERE key LIKE 'background:%' LIMIT 1");
        if (!unfinished && !background) this.releaseIdle();
        this.pumping = false;
        if (unfinished || background) await this.ctx.storage.setAlarm(Date.now() + delay);
        else await this.ctx.storage.deleteAlarm();
        if (immediate && unfinished) this.ctx.waitUntil(this.pump(true));
      }
    }

    private descriptor(job: Job): SessionDescriptor {
      const value = JSON.parse(job.descriptor);
      if (value.account_id) return value;
      const identity = this.store.row("SELECT value FROM metadata WHERE key = 'authority'")?.value;
      if (!identity) throw new Error(`job ${job.request_id} has no session authority`);
      return { ...JSON.parse(String(identity)), session_id: "", subscription: this.env.MODEL_AUTH === "subscription" };
    }

    private sessionId(scope: string, suggested?: string, created?: () => void): string {
      const key = scope === "owner" ? "session_id" : `session_id:${scope}`;
      let session = this.store.row("SELECT value FROM metadata WHERE key = ?", key)?.value as string | undefined;
      if (!session) {
        session = suggested || sessionUuid();
        this.ctx.storage.sql.exec("INSERT INTO metadata VALUES (?, ?)", key, session);
        created?.();
      }
      return session;
    }

    private runtime(scope: string, sessionId: string, descriptor: SessionDescriptor): Promise<AgentRuntime> {
      let resident = this.runtimes.get(scope);
      if (resident) return resident.then(async runtime => {
        if (runtime.reusable && runtime.hands === this.handsMode(scope, descriptor) && runtime.subscription === descriptor.subscription) return runtime;
        if (this.runtimes.get(scope) === resident) { this.runtimes.delete(scope); this.residents.delete(scope); }
        this.releaseHands(scope);
        await runtime.dispose().catch(() => {});
        return this.runtime(scope, sessionId, descriptor);
      });
      if (!resident) {
        this.counts.runtime_initializations++;
        resident = (async () => {
          const authority = { account_id: descriptor.account_id, session_id: sessionId, executor_id: descriptor.executor_id };
          const enabled = this.executorEnabled(descriptor);
          let remote: RemoteExecutor | undefined;
          let remoteFailed = false;
          const owner = {};
          const connectStarted = performance.now();
          const connecting = enabled
            ? RemoteExecutor.create(options.egress(this.env), authority, { max_frame_bytes: policy(this.env.REMOTE_MAX_FRAME_BYTES, "REMOTE_MAX_FRAME_BYTES"), timeout_ms: policy(this.env.REMOTE_TIMEOUT_MS, "REMOTE_TIMEOUT_MS") }).then(value => remote = value, error => { remoteFailed = true; throw error; }).finally(() => this.scopeTimings(scope).record("remote.connect", performance.now() - connectStarted))
            : undefined;
          void connecting?.catch(() => {});
          try {
            const created = await AgentRuntime.create({ session_id: sessionId, provider: descriptor.provider ?? "claude", subscription: descriptor.subscription, endpoint: descriptor.provider === "codex" ? (descriptor.subscription ? "https://chatgpt.com/backend-api/codex" : "https://api.openai.com/v1") : `https://api.anthropic.com/v1/messages${descriptor.subscription ? "?beta=true" : ""}`, remote: enabled, executor: descriptor.executor_id, hands: this.handsMode(scope, descriptor), event_coalesce_ms: duration(this.env.EVENT_COALESCE_MS, "EVENT_COALESCE_MS"), tools: options.tools?.definitions }, this.store, {
          tools: options.tools && {
            call: async (name, input, signal) => {
              if (this.dispatchBlocked(scope)) throw new Error("request authority stopped");
              await this.durable(scope);
              this.counts.host_tool_calls++;
              return options.tools!.call({ authority: { account_id: authority.account_id, session_id: authority.session_id }, name, input }, this.env, signal);
            },
          },
          remote: {
            failed: () => remoteFailed || remote?.failure !== undefined,
            execute: async (request: any, id: number) => {
              const executor = request.connection != null ? this.connections.get(request.connection) : await connecting;
              if (!executor) throw new Error(request.connection != null ? "hand is not connected" : "remote executor is not configured");
              if (request.connection != null && request.op === "remote_catalog") executor.claim(owner);
              else if (request.connection != null && !executor.owns(owner)) {
                if (request.op === "remote_send") throw new Error("hand connection moved to a newer runtime");
                return request.op === "remote_poll" ? { closed: "connection moved to a newer runtime" } : null;
              }
              if (request.op === "remote_send" && request.durable !== false) await this.durable(scope);
              if (request.op === "remote_send" && this.dispatchBlocked(scope)) {
                const forbidden = request.frames.some((text: string) => {
                  const frame = JSON.parse(text);
                  return frame.type === "request" && ["Execute", "Wait", "Call"].includes(Object.keys(frame.operation)[0]);
                });
                if (forbidden) {
                  await executor.dispose();
                  throw new Error("request authority stopped");
                }
              }
              return executor.execute(request, id);
            },
            cancel: id => {
              remote?.cancel(id);
              for (const executor of this.connections.values()) executor.cancel(id);
            },
            dispose: async () => { await connecting?.then(value => value.dispose(), () => {}); },
          },
          dispatch: async (request, signal, timing) => {
            if (this.dispatchBlocked(scope)) throw new Error("request authority stopped");
            await this.durable(scope);
            this.counts.model_dispatches++;
            const dispatch: ModelDispatch = { endpoint: request.endpoint, headers: request.headers, authority, provider: descriptor.provider ?? "claude", subscription: descriptor.subscription };
            const response = await options.egress(this.env).fetch(new Request("https://egress.internal/model", { method: "POST", signal, headers: { "content-type": "application/octet-stream", "x-nanocode-model": headerJson(dispatch) }, body: request.body }));
            const diagnostic = response.headers.get("x-nanocode-model-diagnostic");
            if (diagnostic) {
              try {
                const value = JSON.parse(diagnostic);
                if (timing) timing.diagnostic = value;
                this.modelDiagnostics.push({ scope, value: { ...value, runtime_id: timing?.runtime_id, dispatch_id: timing?.dispatch_id } });
                if (this.modelDiagnostics.length > RECENT_MODEL_LIMIT) this.modelDiagnostics.shift();
              } catch {}
              const headers = new Headers(response.headers);
              headers.delete("x-nanocode-model-diagnostic");
              return new Response(response.body, { status: response.status, statusText: response.statusText, headers });
            }
            return response;
          },
          wake: () => this.eventHub.notify(),
          event: event => this.appendEvent(scope, null, event),
          events: events => this.appendEvents(scope, null, events),
          socket: {
            open: async (request, signal, timing) => {
              if (this.dispatchBlocked(scope)) throw new Error("request authority stopped");
              const response = await options.egress(this.env).fetch(new Request("https://egress.internal/socket", { signal, headers: { upgrade: "websocket", "x-nanocode-socket": headerJson({ authority, provider: descriptor.provider ?? "claude", headers: request.headers }) } }));
              this.socketDiagnostic(scope, response.headers.get("x-nanocode-model-diagnostic"), timing);
              return response;
            },
            admit: () => this.socketEffect(scope, descriptor),
          },
        });
            for (const [hand, entry] of this.hands.get(scope) ?? []) await created.attach(hand, entry.connection).catch(() => {});
            return created;
          } catch (error) { await connecting?.then(value => value.dispose(), () => {}); throw error; }
        })().then(runtime => {
          if (this.runtimes.get(scope) === resident) this.residents.set(scope, runtime);
          return runtime;
        });
        this.runtimes.set(scope, resident);
        void resident.catch(() => { if (this.runtimes.get(scope) === resident) { this.runtimes.delete(scope); this.residents.delete(scope); } });
      }
      return resident;
    }

    private attachHand(scope: string, requested: string | undefined, request: Request, descriptor: SessionDescriptor): Response {
      if (requested !== undefined && !handName(requested)) return json({ error: "invalid hand" }, 400);
      if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") return json({ error: "hands attach over WebSocket" }, 426);
      const [client, server] = Object.values(new WebSocketPair());
      this.ctx.acceptWebSocket(server, [HAND_TAG]);
      const executor = RemoteExecutor.hibernatable(server, this.handLimits());
      this.handSockets.set(server, executor);
      this.ctx.waitUntil(this.registerHand(scope, requested, executor, descriptor));
      return new Response(null, { status: 101, webSocket: client });
    }

    private handLimits() {
      return { max_frame_bytes: policy(this.env.REMOTE_MAX_FRAME_BYTES, "REMOTE_MAX_FRAME_BYTES"), timeout_ms: policy(this.env.REMOTE_TIMEOUT_MS, "REMOTE_TIMEOUT_MS") };
    }

    private async registerHand(scope: string, requested: string | undefined, executor: RemoteExecutor, descriptor: SessionDescriptor): Promise<void> {
      try { await executor.ready(); } catch { this.handSockets.delete(executor.socket); return; }
      const hand = requested ?? executor.name;
      if (!handName(hand)) {
        this.handSockets.delete(executor.socket);
        return executor.dispose();
      }
      const connection = `${hand}#${crypto.randomUUID()}`;
      this.ctx.storage.sql.exec("INSERT INTO hand_sockets VALUES (?, ?, ?, ?)", connection, scope, hand, executor.catalog!);
      executor.socket.serializeAttachment({ connection });
      const previous = this.hands.get(scope)?.get(hand);
      this.indexHand(scope, hand, connection, executor, false);
      const hands = this.hands.get(scope)!;
      if (previous) await previous.executor.dispose();
      const resident = this.runtimes.get(scope);
      const runtime = resident && await resident.catch(() => undefined);
      if (runtime && hands.get(hand)?.connection === connection) await runtime.attach(hand, connection).catch(() => {});
      const entry = hands.get(hand);
      if (entry?.connection !== connection) return;
      entry.ready = true;
    }

    private indexHand(scope: string, hand: string, connection: string, executor: RemoteExecutor, ready: boolean): void {
      let hands = this.hands.get(scope);
      if (!hands) this.hands.set(scope, hands = new Map());
      hands.set(hand, { connection, executor, ready });
      this.connections.set(connection, executor);
      void executor.closed.then(() => this.detachHand(scope, hand, connection));
    }

    private restoreHands(): void {
      const sockets = this.ctx.getWebSockets(HAND_TAG);
      const live: string[] = [];
      for (const socket of sockets) {
        const connection = (socket.deserializeAttachment() as { connection?: string } | null)?.connection;
        const row = connection ? this.store.row("SELECT scope, hand, catalog FROM hand_sockets WHERE connection = ?", connection) : undefined;
        if (!connection || !row) {
          try { socket.close(1011, "hand registration was lost"); } catch {}
          continue;
        }
        live.push(connection);
        const executor = RemoteExecutor.restore(socket, this.handLimits(), String(row.catalog), String(row.hand));
        this.handSockets.set(socket, executor);
        this.indexHand(String(row.scope), String(row.hand), connection, executor, true);
      }
      this.ctx.storage.sql.exec("DELETE FROM hand_sockets WHERE connection NOT IN (SELECT value FROM json_each(?))", JSON.stringify(live));
    }

    async webSocketMessage(socket: WebSocket, message: string | ArrayBuffer): Promise<void> {
      if (this.deleted) return;
      const executor = this.handSockets.get(socket);
      if (executor) return executor.receive(message);
      if (this.ctx.getTags(socket).includes(WATCH_TAG)) return this.watcherMessage(socket, message);
      try { socket.close(1011, "unknown connection"); } catch {}
    }

    async webSocketClose(socket: WebSocket): Promise<void> {
      if (this.deleted) return;
      this.handSockets.get(socket)?.disconnected();
      this.handSockets.delete(socket);
      this.forgetWatcher(socket);
      try { socket.close(1000, "closed"); } catch {}
    }

    async webSocketError(socket: WebSocket): Promise<void> {
      if (this.deleted) return;
      this.handSockets.get(socket)?.disconnected("runner connection failed");
      this.handSockets.delete(socket);
      this.forgetWatcher(socket);
    }

    private watch(scope: string, url: URL, request: Request, descriptor: SessionDescriptor): Response {
      if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") return json({ error: "session sockets attach over WebSocket" }, 426);
      const after = Number(url.searchParams.get("after") ?? "0");
      if (!Number.isSafeInteger(after) || after < 0) return json({ error: "after must be a nonnegative integer" }, 400);
      const [client, server] = Object.values(new WebSocketPair());
      const id = crypto.randomUUID();
      this.ctx.storage.sql.exec("INSERT INTO watch_sockets VALUES (?, ?, ?)", id, scope, JSON.stringify(descriptor));
      this.ctx.acceptWebSocket(server, [WATCH_TAG]);
      server.serializeAttachment({ watch: id, cursor: after } satisfies Watch);
      this.notifyWatchers();
      return new Response(null, { status: 101, webSocket: client });
    }

    private watcher(socket: WebSocket): { state: Watch; scope: string; descriptor: SessionDescriptor } | undefined {
      const state = socket.deserializeAttachment() as Watch | null;
      const row = state ? this.store.row("SELECT scope, descriptor FROM watch_sockets WHERE id = ?", state.watch) : undefined;
      if (!state || !row) return undefined;
      return { state, scope: String(row.scope), descriptor: JSON.parse(String(row.descriptor)) };
    }

    private forgetWatcher(socket: WebSocket): void {
      const state = socket.deserializeAttachment() as Watch | null;
      if (state?.watch) this.ctx.storage.sql.exec("DELETE FROM watch_sockets WHERE id = ?", state.watch);
    }

    private restoreWatchers(): void {
      const live = this.ctx.getWebSockets(WATCH_TAG).map(socket => (socket.deserializeAttachment() as Watch | null)?.watch).filter(Boolean);
      this.ctx.storage.sql.exec("DELETE FROM watch_sockets WHERE id NOT IN (SELECT value FROM json_each(?))", JSON.stringify(live));
    }

    private notifyWatchers(): void {
      if (this.watchersPending) return;
      this.watchersPending = true;
      queueMicrotask(() => {
        this.watchersPending = false;
        this.flushWatchers();
      });
    }

    private flushWatchers(): void {
      const page = policy(this.env.EVENT_PAGE_SIZE, "EVENT_PAGE_SIZE");
      const byScope = new Map<string, { socket: WebSocket; state: Watch }[]>();
      for (const socket of this.ctx.getWebSockets(WATCH_TAG)) {
        const watcher = this.watcher(socket);
        if (!watcher) { try { socket.close(1011, "watch registration was lost"); } catch {} continue; }
        let list = byScope.get(watcher.scope);
        if (!list) byScope.set(watcher.scope, list = []);
        list.push({ socket, state: watcher.state });
      }
      for (const [scope, list] of byScope) {
        let after = Math.min(...list.map(entry => entry.state.cursor));
        for (;;) {
          const rows = this.ctx.storage.sql.exec<{ seq: number; request_id: string | null; body: string; created_at: number }>("SELECT seq, request_id, body, created_at FROM events WHERE scope = ? AND seq > ? ORDER BY seq LIMIT ?", scope, after, page).toArray();
          for (const row of rows) {
            const message = `{"type":"event","seq":${row.seq},"request_id":${JSON.stringify(row.request_id)},"created_at":${row.created_at},"event":${row.body}}`;
            for (const entry of list) {
              if (row.seq <= entry.state.cursor) continue;
              try { entry.socket.send(message); entry.state.cursor = row.seq; } catch {}
            }
          }
          if (rows.length < page) break;
          after = rows.at(-1)!.seq;
        }
        for (const entry of list) entry.socket.serializeAttachment(entry.state);
      }
    }

    private async watcherMessage(socket: WebSocket, message: string | ArrayBuffer): Promise<void> {
      const watcher = this.watcher(socket);
      if (!watcher) { try { socket.close(1011, "watch registration was lost"); } catch {} return; }
      let input: any;
      try { input = JSON.parse(typeof message === "string" ? message : new TextDecoder().decode(message)); } catch { socket.send(JSON.stringify({ type: "error", error: "messages must be JSON" })); return; }
      const headers = { "content-type": "application/json", "x-nanocode-descriptor": headerJson(watcher.descriptor) };
      if (input?.type === "turn") {
        const { type: _, ...turn } = input;
        const response = await this.fetch(new Request("https://session.internal/internal/turns", { method: "POST", headers, body: JSON.stringify(turn) }));
        socket.send(JSON.stringify({ type: "turn", request_id: turn.request_id, status: response.status, body: await response.json() }));
      } else if (input?.type === "cancel" && typeof input.request_id === "string") {
        const response = await this.fetch(new Request(`https://session.internal/internal/cancel/${encodeURIComponent(input.request_id)}`, { method: "POST", headers }));
        socket.send(JSON.stringify({ type: "cancel", request_id: input.request_id, status: response.status, body: await response.json() }));
      } else if (input?.type === "warm") {
        const warmed = !watcher.descriptor.read_only && await this.warm(watcher.scope).catch(() => false);
        socket.send(JSON.stringify({ type: "warm", warmed }));
      } else {
        socket.send(JSON.stringify({ type: "error", error: "unknown message type" }));
      }
    }

    private async warm(scope: string): Promise<boolean> {
      const sessionId = this.store.row("SELECT value FROM metadata WHERE key = ?", scope === "owner" ? "session_id" : `session_id:${scope}`)?.value;
      const last = this.ctx.storage.sql.exec<Job>("SELECT * FROM jobs WHERE scope = ? ORDER BY rowid DESC LIMIT 1", scope).toArray()[0];
      if (!sessionId || !last) return false;
      const descriptor = this.descriptor(last);
      const runtime = await this.runtime(scope, String(sessionId), descriptor);
      await runtime.preconnect();
      if (!this.pumping && !this.store.row("SELECT request_id FROM jobs WHERE status IN ('queued', 'running') LIMIT 1") && !this.store.row("SELECT key FROM metadata WHERE key LIKE 'background:%' LIMIT 1")) this.releaseIdle();
      return true;
    }

    private executorEnabled(descriptor: SessionDescriptor): boolean {
      return !!descriptor.executor_id || this.env.RUNNER_ENABLED === "true";
    }

    private releaseIdle(): void {
      const budget = policy(this.env.RUNTIME_MEMORY_BUDGET_BYTES, "RUNTIME_MEMORY_BUDGET_BYTES");
      for (const [scope, runtime] of this.residents) {
        if (runtime.memoryBytes > budget) this.retire(scope, runtime);
      }
      if (this.linger !== undefined) clearTimeout(this.linger);
      this.linger = setTimeout(() => {
        this.linger = undefined;
        if (this.pumping || this.store.row("SELECT request_id FROM jobs WHERE status IN ('queued', 'running') LIMIT 1")) return;
        this.releaseConnections();
      }, duration(this.env.MODEL_SOCKET_LINGER_MS, "MODEL_SOCKET_LINGER_MS"));
    }

    private releaseConnections(): void {
      for (const [scope, runtime] of this.residents) {
        if (runtime.remote) this.retire(scope, runtime);
        else runtime.releaseConnections();
      }
    }

    private retire(scope: string, runtime: AgentRuntime): void {
      this.counts.runtime_retirements++;
      this.runtimes.delete(scope);
      this.residents.delete(scope);
      this.releaseHands(scope);
      void runtime.dispose().catch(() => {});
    }

    private handsMode(scope: string, descriptor: SessionDescriptor): boolean {
      return descriptor.hand !== undefined || !!this.hands.get(scope)?.size;
    }

    private releaseHands(scope: string): void {
      for (const entry of this.hands.get(scope)?.values() ?? []) entry.executor.release();
    }

    private async durable(scope: string): Promise<void> {
      const started = performance.now();
      this.counts.effect_syncs++;
      await this.ctx.storage.sync();
      this.scopeTimings(scope).record("effect.sync", performance.now() - started);
    }

    private detachHand(scope: string, hand: string, connection: string): void {
      if (this.deleted) return;
      const executor = this.connections.get(connection);
      if (executor) this.handSockets.delete(executor.socket);
      this.ctx.storage.sql.exec("DELETE FROM hand_sockets WHERE connection = ?", connection);
      this.connections.delete(connection);
      const hands = this.hands.get(scope);
      if (hands?.get(hand)?.connection === connection) hands.delete(hand);
      void this.residents.get(scope)?.detach(connection).catch(() => {});
    }

    private async destroy(): Promise<Response> {
      this.deleted = true;
      if (this.linger !== undefined) clearTimeout(this.linger);
      this.linger = undefined;
      for (const socket of this.ctx.getWebSockets()) { try { socket.close(1000, "session deleted"); } catch {} }
      const runtimes = [...this.runtimes.values()];
      this.runtimes.clear();
      this.residents.clear();
      this.eventHub.notify();
      await Promise.all(runtimes.map(resident => resident.then(runtime => runtime.dispose()).catch(() => {})));
      await this.ctx.storage.deleteAlarm();
      await this.ctx.storage.deleteAll();
      return json({ deleted: true });
    }

    private finish(job: Job, result: AgentResult): void {
      const status = result.cancelled ? "cancelled" : result.error ? "failed" : "completed";
      const timing = this.turnTimings.get(job.request_id);
      const runtime = this.running?.job.request_id === job.request_id ? this.running.runtime.metrics.timing : undefined;
      const { started, admitted_at, checkpoints, runtime: before, ...phases } = timing ?? { started: performance.now(), checkpoints: {} };
      const measured = { ...result, timing: { ...phases, observed_ms: performance.now() - started, runtime_id: runtime?.runtime_id, checkpoint_spans: timingDifference(runtime?.checkpoints ?? {}, checkpoints), runtime_spans: runtime ? timingDifference(runtime.spans, before ?? {}) : undefined } };
      this.ctx.storage.transactionSync(() => {
        const last = this.insertEvent(job.scope, job.request_id, { type: "result", request_id: job.request_id, status, ...measured });
        this.ctx.storage.sql.exec("UPDATE jobs SET status = ?, result = ?, last_seq = ? WHERE request_id = ?", status, JSON.stringify(measured), last, job.request_id);
      });
      this.turnTimings.delete(job.request_id);
    }

    private async cancel(id: string, scope: string): Promise<Response> {
      const job = this.job(id);
      if (!job || job.scope !== scope) return json({ error: "request not found" }, 404);
      if (!["queued", "running"].includes(job.status)) return json(this.publicJob(job));
      this.ctx.storage.sql.exec("UPDATE jobs SET cancel_requested = 1 WHERE request_id = ?", id);
      if (job.status === "queued") this.finish(job, { cancelled: true, error: "request cancelled" });
      else {
        const resident = this.runtimes.get(scope);
        if (resident) this.ctx.waitUntil(resident.then(runtime => runtime.cancel(id)).then(() => {}).catch(() => {}));
      }
      if (!this.pumping) await this.ctx.storage.setAlarm(Date.now());
      await this.ctx.storage.sync();
      return json(this.publicJob(this.job(id)!), job.status === "running" ? 202 : 200);
    }

    private async socketEffect(scope: string, descriptor: SessionDescriptor): Promise<void> {
      await this.durable(scope);
      if (this.dispatchBlocked(scope)) throw new Error("request authority stopped");
    }

    private socketDiagnostic(scope: string, header: string | null, timing: { runtime_id: string; dispatch_id: number; diagnostic?: unknown }): void {
      if (!header) return;
      try {
        const value = JSON.parse(header);
        timing.diagnostic = value;
        this.modelDiagnostics.push({ scope, value: { ...value, runtime_id: timing.runtime_id, dispatch_id: timing.dispatch_id } });
        if (this.modelDiagnostics.length > RECENT_MODEL_LIMIT) this.modelDiagnostics.shift();
      } catch {}
    }

    private dispatchBlocked(scope: string): boolean {
      return !!this.store.row("SELECT request_id FROM jobs WHERE scope = ? AND status = 'running' AND cancel_requested = 1 LIMIT 1", scope);
    }

    private events(scope: string, url: URL, streaming = false, descriptor?: SessionDescriptor, requestId?: string): Response {
      const after = Number(url.searchParams.get("after") ?? "0");
      const limit = Number(url.searchParams.get("limit") ?? this.env.EVENT_PAGE_SIZE);
      const maximum = policy(this.env.EVENT_PAGE_SIZE, "EVENT_PAGE_SIZE");
      if (!Number.isSafeInteger(after) || after < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > maximum) return json({ error: `after must be a nonnegative integer and limit must be between 1 and ${maximum}` }, 400);
      const first = requestId === undefined ? null : this.job(requestId)?.first_seq ?? null;
      const read = (cursor: number) => {
        if (first !== null) cursor = Math.max(cursor, first - 1);
        const query = requestId === undefined
          ? this.ctx.storage.sql.exec<{ seq: number; request_id: string | null; body: string; created_at: number }>("SELECT seq, request_id, body, created_at FROM events WHERE scope = ? AND seq > ? ORDER BY seq LIMIT ?", scope, cursor, limit)
          : this.ctx.storage.sql.exec<{ seq: number; request_id: string | null; body: string; created_at: number }>("SELECT seq, request_id, body, created_at FROM events WHERE scope = ? AND request_id = ? AND seq > ? ORDER BY seq LIMIT ?", scope, requestId, cursor, limit);
        const events = query.toArray().map(row => ({ seq: row.seq, request_id: row.request_id, created_at: row.created_at, event: JSON.parse(row.body) }));
        return { events, next_cursor: events.at(-1)?.seq ?? cursor, has_more: events.length === limit };
      };
      if (streaming) return this.eventHub.stream(after, policy(this.env.EVENT_STREAM_KEEPALIVE_MS, "EVENT_STREAM_KEEPALIVE_MS"), read, requestId === undefined ? undefined : event => (event.event as { type?: unknown } | undefined)?.type === "result");
      return json(read(after));
    }

    private insertEvent(scope: string, requestId: string | null, event: unknown, turns?: Map<string, string>): number {
      const value = event as any;
      const sdk = value?.source === "agent" ? value.event : value?.source === "subagent" ? value.update?.event : undefined;
      const turnId = sdk?.payload?.turn_id;
      const acceptedId = sdk?.payload?.request_id;
      if (value?.source === "agent" && typeof turnId === "string" && typeof acceptedId === "string" && this.job(acceptedId)?.scope === scope) {
        this.ctx.storage.sql.exec("INSERT INTO event_turns VALUES (?, ?, ?) ON CONFLICT(scope, turn_id) DO NOTHING", scope, turnId, acceptedId);
      }
      if (typeof turnId === "string") {
        requestId = turnId;
        if (value?.source === "agent") {
          const key = JSON.stringify([scope, turnId]);
          const cached = turns?.get(key) ?? this.eventTurns.get(key);
          if (cached !== undefined) {
            requestId = cached;
            this.counts.event_turn_cache_hits++;
          } else {
            this.counts.event_turn_queries++;
            const mapped = this.store.row("SELECT request_id FROM event_turns WHERE scope = ? AND turn_id = ?", scope, turnId)?.request_id;
            if (mapped !== undefined) {
              requestId = String(mapped);
              turns?.set(key, requestId);
            }
          }
        }
      }
      else if (typeof acceptedId === "string") requestId = acceptedId;
      const seq = this.ctx.storage.sql.exec<{ seq: number }>("INSERT INTO events (scope, request_id, body, created_at) VALUES (?, ?, ?, ?) RETURNING seq", scope, requestId, JSON.stringify(event), Date.now()).one().seq;
      this.counts.events_written++;
      this.eventHub.notify();
      this.notifyWatchers();
      return seq;
    }

    private async appendEvent(scope: string, requestId: string | null, event: unknown): Promise<void> {
      return this.appendEvents(scope, requestId, [event]);
    }

    private async appendEvents(scope: string, requestId: string | null, events: unknown[]): Promise<void> {
      const started = performance.now();
      const turns = new Map<string, string>();
      this.ctx.storage.transactionSync(() => { for (const event of events) this.insertEvent(scope, requestId, event, turns); });
      for (const [key, value] of turns) {
        this.eventTurns.set(key, value);
        while (this.eventTurns.size > 512) this.eventTurns.delete(this.eventTurns.keys().next().value!);
      }
      this.scopeTimings(scope).record("events.insert", performance.now() - started);
    }

    private scopeTimings(scope: string): Timings {
      let value = this.timings.get(scope);
      if (!value) { value = new Timings(); this.timings.set(scope, value); }
      return value;
    }

    private job(id: string): Job | undefined {
      return this.ctx.storage.sql.exec<Job>("SELECT * FROM jobs WHERE request_id = ?", id).toArray()[0];
    }

    private publicJob(job: Job) {
      return { request_id: job.request_id, status: job.status, ...(job.result ? JSON.parse(job.result) : {}) };
    }
  }

  async function fetch(request: Request, env: E): Promise<Response> {
    const started = performance.now();
    const principal = await options.authenticate(request, env);
    if (!principal) return json({ error: "unauthorized" }, 401);
    const account = typeof principal === "string" ? principal : principal.account;
    const url = new URL(request.url);
    if (url.pathname === "/health") return json({ ok: true, model: "claude-opus-5-5", models: { claude: "claude-opus-5-5", codex: "gpt-6.1-sol" }, auth: env.MODEL_AUTH ?? "api_key" });
    if (url.pathname === "/metrics" && request.method === "GET") return options.egress(env).fetch(new Request(url.searchParams.get("probe") === "codex" ? "https://egress.internal/probe/codex" : "https://egress.internal/metrics"));
    const match = /^\/sessions\/([^/]+)(?:\/(turns|events|metrics|socket|hands)(?:\/([^/]+)(?:\/(cancel))?)?)?$/.exec(url.pathname);
    if (!match) return json({ error: "route not found" }, 404);
    let name: string;
    try { name = decodeURIComponent(match[1]); } catch { return json({ error: "invalid session encoding" }, 400); }
    if (!name || name.includes("\0")) return json({ error: "invalid session name" }, 400);
    const mode = (provider: ModelProvider) => options.subscription ? options.subscription(account, env, provider) : env.MODEL_AUTH === "subscription";
    const [subscription, codex_subscription] = await Promise.all([mode("claude"), mode("codex")]);
    const descriptor: SessionDescriptor = { account_id: account, session_id: "", session_name: name, subscription, codex_subscription, ...(typeof principal === "object" && principal.read_only ? { read_only: true } : {}) };
    let path = `/internal/${match[2] ?? "metrics"}`;
    if (match[4]) path = `/internal/cancel/${match[3]}`;
    else if (match[3]) path += `/${match[3]}`;
    const headers = new Headers(request.headers);
    headers.delete("authorization");
    headers.set("x-nanocode-descriptor", headerJson(descriptor));
    const target = new URL(request.url);
    target.pathname = path;
    const dispatched = performance.now();
    const response = await env.SESSIONS.get(env.SESSIONS.idFromName(JSON.stringify([account, name])), { locationHint: env.SESSION_LOCATION_HINT }).fetch(new Request(target, { method: request.method, headers, body: request.body, redirect: "manual" }));
    if (response.webSocket) return response;
    const responseHeaders = new Headers(response.headers);
    responseHeaders.set("server-timing", `public_pre_session;dur=${dispatched - started}, session_response;dur=${performance.now() - dispatched}`);
    return new Response(response.body, { status: response.status, statusText: response.statusText, headers: responseHeaders });
  }

  async function deleteSession(account: string, name: string, env: E): Promise<boolean> {
    const descriptor: SessionDescriptor = { account_id: account, session_id: "", session_name: name, subscription: false };
    const response = await env.SESSIONS.get(env.SESSIONS.idFromName(JSON.stringify([account, name])), { locationHint: env.SESSION_LOCATION_HINT }).fetch(new Request("https://session.internal/internal/delete", { method: "POST", headers: { "x-nanocode-descriptor": headerJson(descriptor) } }));
    await response.body?.cancel();
    return response.ok || response.status === 410;
  }

  return { AgentSession, fetch, deleteSession };
}

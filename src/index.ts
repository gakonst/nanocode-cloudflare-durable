import { DurableObject } from "cloudflare:workers";
import { AgentRuntime, EventHub, HostFailure, RECENT_MODEL_LIMIT, RemoteExecutor, RemoteRefusal, Timings, sessionUuid, timingDifference, type HostTool, type LoggedEvent, type LogPage, type ModelProvider, type TimingSnapshot, type TurnView } from "nanocode-ts";
import wasm from "nanocode-ts/nanocode_wasm.wasm";
import { CheckpointStore, Metadata, SQLITE_VALUE_LIMIT_BYTES } from "./store";
import { ExecutorRejection, connectExecutor } from "./remote";
import { DEFAULT_EXECUTOR, EXECUTOR_ID, headerJson, json, THINKING, type HostToolCall, type ModelDispatch, type PrivateBinding, type SessionDescriptor, type Thinking } from "./contracts";

export type { HostTool, ModelProvider } from "nanocode-ts";
export type { HostToolCall, ModelDispatch, PrivateBinding, SessionAuthority, SessionDescriptor, SocketDispatch, Thinking } from "./contracts";
export { THINKING } from "./contracts";

const WORKERS_AI_MODELS = new Set(["glm-5.3"]);

export interface NanocodeEnv {
  SESSIONS: DurableObjectNamespace;
  MODEL_AUTH?: string;
  CLAUDE_MODEL?: string;
  WORKERS_AI_ACCOUNT_ID?: string;
  RECOVERY_DELAY_MS: string;
  EVENT_STREAM_KEEPALIVE_MS: string;
  EVENT_PAGE_SIZE: string;
  EVENT_TAIL_BYTES: string;
  EVENT_COALESCE_MS: string;
  MODEL_SOCKET_LINGER_MS: string;
  RUNTIME_MEMORY_BUDGET_BYTES: string;
  RUNNER_ENABLED?: string;
  REMOTE_MAX_FRAME_BYTES: string;
  REMOTE_TIMEOUT_MS: string;
  SESSION_LOCATION_HINT?: DurableObjectLocationHint;
}

export type Principal = { account: string; read_only?: boolean; grant?: unknown; expires_at?: number | null };

export type NanocodeOptions<E extends NanocodeEnv> = {
  authenticate(request: Request, env: E): Promise<string | Principal | null> | string | Principal | null;
  egress(env: E): PrivateBinding;
  subscription?(account: string, env: E, provider: ModelProvider): Promise<boolean> | boolean;
  recordSession?(account: string, name: string, env: E): Promise<boolean> | boolean;
  active?(account: string, grant: unknown, env: E): Promise<boolean> | boolean;
  tools?: { definitions: HostTool[]; call(call: HostToolCall, env: E, signal: AbortSignal): Promise<unknown> };
  chaos?: boolean;
};

type TurnTiming = { started: number; admitted_at?: number; admission_ms?: number; pump_wait_ms?: number; runtime_open_ms?: number; runtime_reused?: boolean; run_started?: number; run_ms?: number; recovered?: boolean; checkpoints: TimingSnapshot; runtime?: TimingSnapshot };
type LegacyJob = { request_id: string; prompt: string; status: string; result: string | null; scope?: string; descriptor?: string; cancel_requested?: number; first_seq?: number | null; last_seq?: number | null };

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
const TOO_LARGE = `request bodies are limited to ${SQLITE_VALUE_LIMIT_BYTES} bytes`;
const decoder = new TextDecoder();

async function bodyText(request: Request): Promise<string | undefined> {
  if (!request.body) return "";
  const reader = request.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > SQLITE_VALUE_LIMIT_BYTES) {
      await reader.cancel().catch(() => {});
      return undefined;
    }
    chunks.push(value);
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  return decoder.decode(bytes);
}
const HAND_TAG = "hand";
const HAND_REPLACED = "another hand attached under this name";
const INVALID_HAND = "hand names are 1 to 48 lowercase letters, digits, _ or -, starting with a letter or digit; pass --name";
const WATCH_TAG = "watch";
type Watch = { watch: string; cursor: number };
type Watcher = { state: Watch; scope: string; descriptor: SessionDescriptor };
type Executor = RemoteExecutor<WebSocket>;
const OWNER = "owner";
const UNFINISHED = new Set(["queued", "running"]);
const INTERRUPTED = "Agent execution interrupted; recovery scheduled";

function executorFailure(error: unknown): HostFailure {
  const message = error instanceof Error ? error.message : String(error);
  if (!(error instanceof ExecutorRejection)) return new HostFailure(message, "unreachable", false);
  if (error.status === 401 || error.status === 403) return new HostFailure(message, "unauthorized", true);
  if (error.status === 501 || (error.status >= 400 && error.status < 500 && error.status !== 408 && error.status !== 429)) return new HostFailure(message, "rejected", true);
  return new HostFailure(message, "unavailable", false);
}

export function createNanocode<E extends NanocodeEnv>(options: NanocodeOptions<E>) {
  class AgentSession extends DurableObject<E> {
    private readonly store: CheckpointStore;
    private readonly metadata: Metadata;
    private readonly instances = new Map<string, Promise<AgentRuntime>>();
    private readonly runtimes = new Map<string, Promise<AgentRuntime>>();
    private readonly residents = new Map<string, AgentRuntime>();
    private readonly efforts = new WeakMap<AgentRuntime, Thinking>();
    private readonly blocked = new Set<string>();
    private readonly driving = new Map<string, string>();
    private binding?: Promise<void>;
    private readonly watchers = new Map<WebSocket, Watcher>();
    private unfinished?: boolean;
    private admissions = 0;
    private flushing = false;
    private reflush = false;
    private readonly hands = new Map<string, Map<string, { connection: string; executor: Executor; ready: boolean }>>();
    private readonly connections = new Map<string, Executor>();
    private readonly handSockets = new Map<WebSocket, Executor>();
    private readonly handAuthority = new Map<string, SessionDescriptor>();
    private readonly rechecks = new Set<WebSocket>();
    private readonly handFrames = new Map<WebSocket, Promise<void>>();
    private linger?: ReturnType<typeof setTimeout>;
    private pumping = false;
    private recorded = false;
    private deleted = false;
    private readonly eventHub = new EventHub(() => policy(this.env.EVENT_PAGE_SIZE, "EVENT_PAGE_SIZE"), () => policy(this.env.EVENT_TAIL_BYTES, "EVENT_TAIL_BYTES"));
    private readonly modelDiagnostics: { scope: string; value: unknown }[] = [];
    private readonly timings = new Map<string, Timings>();
    private readonly turnTimings = new Map<string, TurnTiming>();
    private crash?: number;
    private readonly counts = { runtime_initializations: 0, model_dispatches: 0, events_written: 0, preparation_writes: 0, preparation_writes_skipped: 0, effect_syncs: 0, alarms: 0, direct_starts: 0, recoveries: 0, runtime_retirements: 0, host_tool_calls: 0 };

    constructor(ctx: DurableObjectState, env: E) {
      super(ctx, env);
      this.store = new CheckpointStore(ctx.storage);
      this.metadata = new Metadata(ctx.storage);
      ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
        DROP INDEX IF EXISTS scoped_events;
        DROP INDEX IF EXISTS scoped_request_events;
        DROP TABLE IF EXISTS grant_fences;
        CREATE TABLE IF NOT EXISTS hand_sockets (connection TEXT PRIMARY KEY, scope TEXT NOT NULL, hand TEXT NOT NULL, catalog TEXT NOT NULL, descriptor TEXT) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS watch_sockets (id TEXT PRIMARY KEY, scope TEXT NOT NULL, descriptor TEXT NOT NULL) WITHOUT ROWID;
      `);
      if (!this.store.row("SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'metadata'")?.sql?.toString().includes("WITHOUT ROWID")) ctx.storage.transactionSync(() => ctx.storage.sql.exec("CREATE TABLE metadata_rows (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID; INSERT INTO metadata_rows SELECT key, value FROM metadata; DROP TABLE metadata; ALTER TABLE metadata_rows RENAME TO metadata"));
      if (!ctx.storage.sql.exec("PRAGMA table_info(hand_sockets)").toArray().some(column => column.name === "descriptor")) ctx.storage.sql.exec("ALTER TABLE hand_sockets ADD COLUMN descriptor TEXT");
      ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
      this.restoreHands();
      this.restoreWatchers();
      if (this.store.row("SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'jobs'")) void ctx.blockConcurrencyWhile(() => this.migrate());
    }

    private async migrate(): Promise<void> {
      const columns = new Set(this.ctx.storage.sql.exec("PRAGMA table_info(jobs)").toArray().map(row => row.name));
      const select = ["request_id", "prompt", "status", "result", ...["scope", "descriptor", "cancel_requested", "first_seq", "last_seq"].filter(column => columns.has(column))].join(", ");
      const jobs = this.ctx.storage.sql.exec<LegacyJob>(`SELECT ${select} FROM jobs ORDER BY rowid`).toArray();
      const ranges = new Map<string, { first: number | null; last: number | null }>();
      if (this.store.row("SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'events'")) {
        for (const row of this.ctx.storage.sql.exec<{ request_id: string; first: number | null; last: number | null }>("SELECT request_id, MIN(seq) AS first, MAX(CASE WHEN body LIKE '{\"type\":\"result\"%' THEN seq END) AS last FROM events WHERE request_id IS NOT NULL GROUP BY request_id")) ranges.set(row.request_id, row);
      }
      const scopes = new Map<string, unknown[]>();
      for (const job of jobs) {
        const scope = job.scope ?? OWNER;
        let descriptor: SessionDescriptor | Record<string, never>;
        try { descriptor = this.descriptorOf(JSON.parse(job.descriptor ?? "{}")); } catch { descriptor = {}; }
        const result = job.result ? JSON.parse(job.result) : undefined;
        const unfinished = UNFINISHED.has(job.status);
        const range = ranges.get(job.request_id);
        let list = scopes.get(scope);
        if (!list) scopes.set(scope, list = []);
        list.push({ request_id: job.request_id, status: job.status, prompt: job.prompt, hand: descriptor.hand, metadata: descriptor, cancel_requested: !!job.cancel_requested, note: unfinished && typeof result?.error === "string" ? result.error : undefined, first_seq: job.first_seq ?? range?.first ?? undefined, last_seq: unfinished ? undefined : job.last_seq ?? range?.last ?? undefined, result: unfinished ? undefined : result });
      }
      for (const [scope, turns] of scopes) await (await this.instance(scope)).import(scope, turns);
      this.ctx.storage.sql.exec("DROP TABLE IF EXISTS event_turns; DROP TABLE jobs");
    }

    async fetch(request: Request): Promise<Response> {
      const admitted = performance.now();
      if (this.deleted) return json({ error: "session deleted" }, 410);
      try {
        const url = new URL(request.url);
        let descriptor: SessionDescriptor;
        try { descriptor = JSON.parse(request.headers.get("x-nanocode-descriptor") ?? "null"); } catch { return json({ error: "invalid session authority" }, 400); }
        if (!descriptor || typeof descriptor.account_id !== "string" || !descriptor.account_id || typeof descriptor.session_name !== "string" || !descriptor.session_name) return json({ error: "session authority is required" }, 403);
        const binding = this.metadata.get("authority");
        const identity = JSON.stringify({ account_id: descriptor.account_id, session_name: descriptor.session_name });
        if (binding && binding !== identity) return json({ error: "session authority mismatch" }, 403);
        const scope = OWNER;
        const parts = url.pathname.split("/");
        if (parts[2] === "delete" && request.method === "POST") return await this.destroy();
        if (descriptor.read_only && (request.method !== "GET" || (parts[2] === "hands" && request.headers.get("upgrade")?.toLowerCase() === "websocket"))) return json({ error: "read-only access" }, 403);
        if (!this.recorded && options.recordSession) {
          if (this.metadata.get("recorded") === undefined) {
            if (!await options.recordSession(descriptor.account_id, descriptor.session_name, this.env)) {
              await this.destroy();
              return json({ error: "account not found" }, 403);
            }
            this.metadata.insert("recorded", String(Date.now()));
          }
          this.recorded = true;
        }
        if (request.method === "GET" && parts[2] === "metrics") return json({ ...this.counts, object_id: this.ctx.id.toString(), alarm_at: await this.ctx.storage.getAlarm(), database_bytes: this.ctx.storage.sql.databaseSize, model_diagnostics: this.modelDiagnostics.filter(entry => entry.scope === scope).map(entry => entry.value), timing: this.timings.get(scope)?.snapshot() ?? {}, checkpoints: { ...this.store.metrics, timing: this.residents.get(scope)?.metrics.timing.checkpoints ?? {} }, log: this.store.log, resident_runtimes: this.runtimes.size, runtime: this.residents.get(scope)?.metrics, persisted: this.store.persisted(scope) });
        if (request.method === "GET" && parts[2] === "events") {
          const cursor = this.cursor(url, request);
          if (!cursor) return json({ error: `after must be a nonnegative integer and limit must be between 1 and ${policy(this.env.EVENT_PAGE_SIZE, "EVENT_PAGE_SIZE")}` }, 400);
          return await this.events(scope, cursor, request.headers.get("accept")?.includes("text/event-stream"), undefined, descriptor);
        }
        if (parts[2] === "socket") return this.watch(scope, url, request, descriptor);
        if (parts[2] === "hands") {
          if (request.method !== "GET") return json({ error: "method not allowed" }, 405);
          if (parts.length === 3 && request.headers.get("upgrade")?.toLowerCase() !== "websocket") return json({ hands: [...(this.hands.get(scope) ?? [])].filter(([, entry]) => entry.ready).map(([hand]) => hand) });
          return this.attachHand(scope, parts.length === 4 ? decodeURIComponent(parts[3]) : undefined, request, descriptor);
        }
        if (parts[2] === "cancel" && parts.length === 4 && request.method === "POST") return await this.cancel(decodeURIComponent(parts[3]), scope);
        if (parts[2] === "warm") {
          if (request.method !== "POST") return json({ error: "method not allowed" }, 405);
          const input = await request.json().catch(() => null) as { provider?: unknown; model?: unknown } | null;
          const provider = this.metadata.get(`model_provider:${scope}`) ?? input?.provider ?? "claude";
          if (provider !== "claude" && provider !== "codex") return json({ error: "provider must be claude or codex" }, 400);
          const model = this.metadata.get(`model:${scope}`) ?? (typeof input?.model === "string" && input.model ? input.model : provider === "codex" ? "gpt-6.1-sol" : this.env.CLAUDE_MODEL ?? "claude-opus-5-5");
          if (provider === "codex" && WORKERS_AI_MODELS.has(model)) {
            await this.instance(scope);
            return new Response(null, { status: 204 });
          }
          const [, warmed] = await Promise.all([this.instance(scope), options.egress(this.env).fetch(new Request("https://egress.internal/warm", { method: "POST", headers: { "x-nanocode-model": headerJson({ provider, model }) } }))]);
          await warmed.body?.cancel();
          return new Response(null, { status: warmed.ok ? 204 : warmed.status });
        }
        if (parts[2] === "crash" && options.chaos && request.method === "POST") {
          const after = Number(((await request.json().catch(() => null)) as { after_ms?: unknown } | null)?.after_ms ?? 0);
          if (!Number.isSafeInteger(after) || after < 0) return json({ error: "after_ms must be a nonnegative integer" }, 400);
          this.crash = after;
          return json({ armed: true, after_ms: after });
        }
        if (parts[2] !== "turns") return json({ error: "route not found" }, 404);
        if (request.method === "GET" && parts.length === 4) {
          const turn = await (await this.instance(scope)).turn(scope, decodeURIComponent(parts[3]));
          return turn ? json(turn.report) : json({ error: "request not found" }, 404);
        }
        if (request.method !== "POST" || parts.length !== 3) return json({ error: "method not allowed" }, 405);
        const body = await bodyText(request);
        if (body === undefined) return json({ error: TOO_LARGE }, 413);
        let input: any;
        try { input = JSON.parse(body); } catch { return json({ error: "invalid JSON" }, 400); }
        if (typeof input?.request_id !== "string" || !input.request_id.trim() || typeof input.prompt !== "string" || !input.prompt.trim()) return json({ error: "request_id and prompt must be nonempty strings" }, 400);
        if (input.provider !== undefined && input.provider !== "claude" && input.provider !== "codex") return json({ error: "provider must be claude or codex" }, 400);
        if (input.model !== undefined && (typeof input.model !== "string" || !input.model)) return json({ error: "model must be a nonempty model ID" }, 400);
        if (input.thinking !== undefined && !THINKING.includes(input.thinking)) return json({ error: `thinking must be one of ${THINKING.join(", ")}` }, 400);
        if (input.executor !== undefined && (typeof input.executor !== "string" || !EXECUTOR_ID.test(input.executor))) return json({ error: "invalid executor" }, 400);
        if (input.hand !== undefined && !handName(input.hand)) return json({ error: "hand must be the name of an attached hand" }, 400);
        if (input.steer !== undefined && typeof input.steer !== "boolean") return json({ error: "steer must be a boolean" }, 400);
        if (input.steer) {
          const resident = this.residents.get(scope);
          if (resident && this.driving.has(scope) && await resident.steer(input.request_id, input.prompt)) return json({ request_id: input.request_id, status: "steered" });
          return json({ error: "no running turn takes steering; submit the message as a turn" }, 409);
        }
        const streaming = request.headers.get("accept")?.includes("text/event-stream");
        const cursor = streaming ? this.cursor(url, request) : undefined;
        if (streaming && !cursor) return json({ error: "invalid event cursor" }, 400);
        const executorKey = `executor:${scope}`;
        const providerKey = `model_provider:${scope}`;
        const modelKey = `model:${scope}`;
        const sessionKey = this.sessionKey(scope);
        const unbound = () => this.metadata.get(executorKey) === undefined && this.metadata.get(providerKey) === undefined && this.metadata.get(sessionKey) === undefined;
        while (this.binding && unbound()) await this.binding;
        let bound: (() => void) | undefined;
        if (unbound()) this.binding = new Promise<void>(resolve => { bound = resolve; });
        let submitted: Awaited<ReturnType<AgentRuntime["submit"]>>;
        try {
          const storedExecutor = this.metadata.get(executorKey);
          const initialized = this.metadata.get(sessionKey) !== undefined;
          const legacyExecutor = this.legacyExecutor();
          const boundExecutor = storedExecutor ?? (initialized ? legacyExecutor : undefined);
          if (boundExecutor !== undefined && input.executor !== undefined && input.executor !== boundExecutor) return json({ error: "session executor is immutable; create a new session" }, 409);
          const executor = input.executor ?? boundExecutor ?? legacyExecutor;
          descriptor.executor_id = executor || undefined;
          descriptor.hand = input.hand;
          const boundProvider = this.metadata.get(providerKey) ?? (initialized ? "claude" : undefined);
          descriptor.provider = input.provider ?? boundProvider ?? "claude";
          const rejected = await (await this.instance(scope)).admit(input.request_id, boundProvider === undefined ? null : String(boundProvider), descriptor.provider!);
          if (rejected === "reserved_request_id") return json({ error: "request_id prefix is reserved" }, 400);
          if (rejected === "provider_changed") return json({ error: "session provider is immutable; create a new session" }, 409);
          const boundModel = this.metadata.get(modelKey) ?? (initialized ? null : undefined);
          if (boundModel !== undefined && input.model !== undefined && input.model !== boundModel) return json({ error: "session model is immutable; create a new session" }, 409);
          descriptor.model = input.model ?? (boundModel === null ? undefined : boundModel);
          descriptor.thinking = input.thinking;
          if (descriptor.provider === "codex") descriptor.subscription = descriptor.codex_subscription ?? true;
          policy(this.env.RECOVERY_DELAY_MS, "RECOVERY_DELAY_MS");
          const current = this.metadata.get("authority");
          if (current && current !== identity) throw new Error("session authority mismatch");
          submitted = await (await this.instance(scope)).submit(scope, { request_id: input.request_id, prompt: input.prompt, hand: input.hand, metadata: descriptor });
          if (submitted.submitted === "too_large") return json({ error: "request exceeds the session's storage limit", bytes: submitted.bytes, limit: submitted.limit }, 413);
          if (submitted.submitted === "conflict") return json({ error: "request_id already belongs to different input or authority" }, 409);
          if (submitted.submitted === "accepted") {
            this.metadata.transaction(() => {
              this.metadata.insert("authority", identity);
              this.metadata.insert(providerKey, descriptor.provider!);
              if (descriptor.model !== undefined) this.metadata.insert(modelKey, descriptor.model);
              this.metadata.insert(executorKey, executor);
              this.sessionId(scope, descriptor.session_id);
            });
            this.unfinished = true;
            this.admissions++;
            this.turnTimings.set(input.request_id, { started: admitted, checkpoints: {} });
          }
        } finally {
          if (bound) { this.binding = undefined; bound(); }
        }
        const turn = submitted.turn;
        const unfinished = UNFINISHED.has(turn.report.status);
        if (unfinished && !this.pumping) await this.ctx.storage.setAlarm(Date.now() + policy(this.env.RECOVERY_DELAY_MS, "RECOVERY_DELAY_MS"));
        if (submitted.submitted === "accepted") {
          const timing = this.turnTimings.get(input.request_id);
          if (timing) {
            timing.admission_ms = performance.now() - admitted;
            timing.admitted_at = performance.now();
          }
        }
        if (unfinished && !this.pumping) {
          this.counts.direct_starts++;
          this.ctx.waitUntil(this.pump(true));
        }
        if (cursor) {
          if (!unfinished && (turn.last_seq === null || cursor.after >= turn.last_seq)) return new Response(null, { headers: { "content-type": "text/event-stream", "cache-control": "no-store" } });
          return await this.events(scope, cursor, true, input.request_id, descriptor);
        }
        return json(turn.report, unfinished ? 202 : 200);
      } catch (error) {
        if (error instanceof URIError) return json({ error: "invalid request encoding" }, 400);
        if (error instanceof Error && error.message.includes("SQLITE_TOOBIG")) return json({ error: "request exceeds the session's storage limit" }, 413);
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
      let active: string | undefined;
      let retryAt: number | undefined;
      try {
        if (!watchdogArmed) await this.ctx.storage.setAlarm(Date.now() + delay);
        const reader = await this.instance(OWNER);
        const admissions = this.admissions;
        const pending = await reader.pending(OWNER);
        const next = pending[0];
        this.unfinished = next ? true : admissions === this.admissions ? false : undefined;
        const background = this.metadata.first("background:");
        if (!next && !background) { immediate = admissions !== this.admissions; return; }
        scope = next ? OWNER : background![0].slice("background:".length);
        const descriptor: SessionDescriptor = next ? this.descriptorOf(next.metadata) : JSON.parse(background![1]);
        const serialized = JSON.stringify(descriptor);
        const marked = background?.[0] === `background:${scope}`;
        const accepted = !next || next.report.status === "running";
        let sessionId!: string;
        let preparationRequired = false;
        const mark = () => {
          this.metadata.set(`background:${scope}`, serialized);
          preparationRequired = true;
        };
        this.metadata.transaction(() => {
          if ((marked || accepted) && (!watchdogArmed || !marked || background![1] !== serialized)) mark();
          sessionId = this.sessionId(scope!, descriptor.session_id, () => { preparationRequired = true; });
        });
        if (next) {
          active = next.report.request_id;
          if (next.report.status === "running") this.counts.recoveries++;
        }
        const prepared = performance.now();
        let timing: TurnTiming | undefined;
        if (active) {
          timing = this.turnTimings.get(active);
          if (!timing) {
            timing = { started: prepared, recovered: true, checkpoints: {} };
            this.turnTimings.set(active, timing);
          }
          timing.pump_wait_ms = prepared - (timing.admitted_at ?? prepared);
          timing.runtime_reused = this.runtimes.has(scope);
        }
        const hand = next && !next.cancel_requested ? descriptor.hand : undefined;
        if (hand !== undefined && !this.hands.get(scope)?.get(hand)?.ready) {
          retryAt = await this.unavailable(scope, next, new HostFailure(`hand \`${hand}\` is not attached`, "detached", false), delay);
          immediate = retryAt === undefined || !!this.hands.get(scope)?.get(hand)?.ready;
          return;
        }
        const runtimeStarted = performance.now();
        const initializations = this.counts.runtime_initializations;
        try {
          runtime = await this.runtime(scope, sessionId, descriptor);
        } catch (error) {
          if (active) this.blocked.delete(active);
          retryAt = await this.unavailable(scope, next, error instanceof HostFailure ? error : new HostFailure(error instanceof Error ? error.message : String(error), "runtime", false), delay);
          immediate = retryAt === undefined;
          return;
        }
        const thinking = next && !next.cancel_requested ? descriptor.thinking : undefined;
        if (thinking && this.efforts.get(runtime) !== thinking) {
          try {
            await runtime.setThinking(thinking);
            this.efforts.set(runtime, thinking);
          } catch (error) {
            if (active) this.blocked.delete(active);
            retryAt = await this.unavailable(scope, next, new HostFailure(error instanceof Error ? error.message : String(error), "configuration", true), delay);
            immediate = retryAt === undefined;
            return;
          }
        }
        if (!marked && !accepted) mark();
        if (preparationRequired) this.counts.preparation_writes++;
        else this.counts.preparation_writes_skipped++;
        if (timing) {
          timing.runtime_reused = initializations === this.counts.runtime_initializations;
          timing.runtime_open_ms = performance.now() - runtimeStarted;
          timing.runtime = timing.runtime_reused ? runtime.metrics.timing.spans : undefined;
          timing.checkpoints = timing.runtime_reused ? runtime.metrics.timing.checkpoints : {};
          timing.run_started = performance.now();
        }
        for (const id of this.blocked) if (id !== active) this.blocked.delete(id);
        if (active && next?.cancel_requested && next.report.status === "running") this.blocked.add(active);
        if (active) this.driving.set(scope, active);
        const driven = await runtime.drive(active);
        this.driving.delete(scope);
        if (active) this.blocked.delete(active);
        if (driven.retry) throw new Error("runtime recovery required");
        if (driven.finished) {
          this.turnTimings.delete(driven.finished.report.request_id);
          this.unfinished = pending.length === 1 && next!.report.request_id === driven.finished.report.request_id && admissions === this.admissions ? false : undefined;
        }
        waiting = !!driven.waiting;
        const version = this.eventHub.version;
        if (await runtime.settle()) {
          this.metadata.delete(`background:${scope}`);
        }
        else if (waiting || this.queueKnownEmpty()) {
          await this.eventHub.wait(delay, version);
        }
        immediate = !this.queueKnownEmpty();
      } catch {
        if (scope) this.driving.delete(scope);
        if (active) this.blocked.delete(active);
        if (scope && runtime) {
          if (this.residents.get(scope) === runtime) { this.runtimes.delete(scope); this.residents.delete(scope); this.releaseHands(scope); }
          await runtime.dispose().catch(() => {});
        }
        if (active) {
          const fields = { cancelled: true, error: "request cancelled", ...this.observed(active) };
          this.turnTimings.delete(active);
          this.unfinished = true;
          const reader = await this.instance(scope ?? OWNER).catch(() => undefined);
          const turn = await reader?.turn(scope ?? OWNER, active).catch(() => null);
          if (turn?.cancel_requested && UNFINISHED.has(turn.report.status)) {
            immediate = await reader!.finish(scope ?? OWNER, active, "cancelled", fields).then(() => true, () => false);
            if (immediate) this.unfinished = undefined;
          } else await reader?.interrupted(scope ?? OWNER, active, INTERRUPTED).catch(() => {});
        }
      } finally {
        if (this.deleted) { this.pumping = false; return; }
        const unfinished = !this.queueKnownEmpty();
        const background = this.metadata.first("background:");
        if (!unfinished && !background) this.releaseIdle();
        this.pumping = false;
        if (unfinished || background) await this.ctx.storage.setAlarm(retryAt ?? Date.now() + delay);
        else await this.ctx.storage.deleteAlarm();
        if (immediate && unfinished) this.ctx.waitUntil(this.pump(true));
      }
    }

    private queueKnownEmpty(): boolean {
      return this.unfinished === false;
    }

    private async unavailable(scope: string, next: TurnView | undefined, failure: HostFailure, delay: number): Promise<number | undefined> {
      if (!next) {
        if (!failure.permanent) return Date.now() + delay;
        this.metadata.delete(`background:${scope}`);
        return undefined;
      }
      const id = next.report.request_id;
      const reader = await this.instance(scope);
      let turn: TurnView | null = next;
      if (!next.cancel_requested && !failure.permanent) {
        const retryAt = Date.now() + delay;
        turn = await reader.retry(scope, id, failure.message, failure.kind, retryAt);
        if (turn && UNFINISHED.has(turn.report.status) && !turn.cancel_requested) return retryAt;
      }
      if (turn && UNFINISHED.has(turn.report.status)) {
        const fields = turn.cancel_requested ? { cancelled: true, error: "request cancelled" } : { error: failure.message, kind: failure.kind };
        await reader.finish(scope, id, turn.cancel_requested ? "cancelled" : "failed", { ...fields, ...this.observed(id) });
      }
      this.turnTimings.delete(id);
      this.unfinished = undefined;
      return undefined;
    }

    private descriptorOf(value: unknown): SessionDescriptor {
      const descriptor = value as SessionDescriptor | null;
      if (descriptor?.account_id) return descriptor;
      const identity = this.metadata.get("authority");
      if (!identity) throw new Error("turn has no session authority");
      return { ...JSON.parse(identity), session_id: "", subscription: this.env.MODEL_AUTH === "subscription" };
    }

    private sessionKey(scope: string): string {
      return scope === OWNER ? "session_id" : `session_id:${scope}`;
    }

    private legacyExecutor(): string {
      return this.env.RUNNER_ENABLED === "true" ? DEFAULT_EXECUTOR : "";
    }

    private sessionId(scope: string, suggested?: string, created?: () => void): string {
      const key = this.sessionKey(scope);
      let session = this.metadata.get(key);
      if (!session) {
        session = suggested || sessionUuid();
        this.metadata.insert(key, session);
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
          let remote: Executor | undefined;
          let remoteFailed = false;
          let remoteError: unknown;
          const owner = {};
          const connectStarted = performance.now();
          const connecting = enabled
            ? connectExecutor(options.egress(this.env), authority, this.handLimits()).then(value => remote = value, error => { remoteFailed = true; remoteError = error; throw error; }).finally(() => this.scopeTimings(scope).record("remote.connect", performance.now() - connectStarted))
            : undefined;
          void connecting?.catch(() => {});
          try {
            const created = await this.instance(scope, true);
            await created.open({ scope, session_id: sessionId, provider: descriptor.provider ?? "claude", model: descriptor.model ?? (descriptor.provider === "codex" ? undefined : this.env.CLAUDE_MODEL), subscription: descriptor.subscription, endpoint: descriptor.provider === "codex" ? (descriptor.subscription ? "https://chatgpt.com/backend-api/codex" : "https://api.openai.com/v1") : `https://api.anthropic.com/v1/messages${descriptor.subscription ? "?beta=true" : ""}`, ...(descriptor.provider === "codex" && WORKERS_AI_MODELS.has(descriptor.model ?? "") ? { gateway: "workers_ai" as const, cloudflare_account_id: this.env.WORKERS_AI_ACCOUNT_ID } : {}), remote: enabled, executor: descriptor.executor_id, hands: this.handsMode(scope, descriptor), event_coalesce_ms: duration(this.env.EVENT_COALESCE_MS, "EVENT_COALESCE_MS"), tools: options.tools?.definitions }, {
          tools: options.tools && {
            call: async (name, input, signal) => {
              if (this.dispatchBlocked(scope)) throw new Error("request authority stopped");
              await this.durable(scope);
              this.counts.host_tool_calls++;
              const crash = this.crash;
              this.crash = undefined;
              if (crash !== undefined) setTimeout(() => this.ctx.abort(`chaos: the session object was reset during ${name}`), crash);
              return options.tools!.call({ authority: { account_id: authority.account_id, session_id: authority.session_id }, name, input, grant: this.turnGrant(scope, descriptor) }, this.env, signal);
            },
          },
          remote: {
            failed: () => remoteFailed || remote?.failure !== undefined,
            execute: async (request: any, id: number) => {
              const executor = request.connection != null ? this.connections.get(request.connection) : await connecting;
              if (!executor) throw new RemoteRefusal(request.connection != null ? "hand is not connected" : "remote executor is not configured");
              if (request.connection != null && request.op === "remote_catalog") executor.claim(owner);
              else if (request.connection != null && !executor.owns(owner)) {
                if (request.op === "remote_send") throw new RemoteRefusal("hand connection moved to a newer runtime");
                return request.op === "remote_poll" ? { closed: "connection moved to a newer runtime" } : null;
              }
              if (request.op === "remote_send" && request.durable !== false) {
                try { await this.durable(scope); } catch (error) { throw new RemoteRefusal(error instanceof Error ? error.message : String(error)); }
              }
              if (request.op === "remote_send" && request.dispatches && this.dispatchBlocked(scope)) {
                setTimeout(() => void executor.dispose(), 0);
                throw new RemoteRefusal("request authority stopped");
              }
              if (request.op === "remote_send" && request.connection != null && !await this.handAllowed(request.connection)) {
                setTimeout(() => void executor.dispose(), 0);
                throw new RemoteRefusal("the hand's access token expired or was revoked");
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
              this.diagnostic(scope, diagnostic, timing);
              const headers = new Headers(response.headers);
              headers.delete("x-nanocode-model-diagnostic");
              return new Response(response.body, { status: response.status, statusText: response.statusText, headers });
            }
            return response;
          },
          finishing: id => this.finishing(id, created),
          socket: {
            open: async (request, signal, timing) => {
              if (this.dispatchBlocked(scope)) throw new Error("request authority stopped");
              const response = await options.egress(this.env).fetch(new Request("https://egress.internal/socket", { signal, headers: { upgrade: "websocket", "x-nanocode-socket": headerJson({ authority, provider: descriptor.provider ?? "claude", headers: request.headers }) } }));
              this.diagnostic(scope, response.headers.get("x-nanocode-model-diagnostic"), timing);
              const socket = response.status === 101 ? response.webSocket ?? undefined : undefined;
              socket?.accept();
              return { status: response.status, headers: response.headers, socket, idle_ms: Number(response.headers.get("x-nanocode-stream-idle-ms")), text: () => response.text() };
            },
            admit: () => this.socketEffect(scope),
          },
        });
            for (const [hand, entry] of this.hands.get(scope) ?? []) await created.attach(hand, entry.connection).catch(() => {});
            return created;
          } catch (error) {
            const failed = remoteFailed;
            await connecting?.then(value => value.dispose(), () => {});
            throw failed ? executorFailure(remoteError) : error;
          }
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
      const executor = RemoteExecutor.accept(server, this.handLimits());
      this.handSockets.set(server, executor);
      this.ctx.waitUntil(this.registerHand(scope, requested, executor, descriptor));
      return new Response(null, { status: 101, webSocket: client });
    }

    private handLimits() {
      return { max_frame_bytes: policy(this.env.REMOTE_MAX_FRAME_BYTES, "REMOTE_MAX_FRAME_BYTES"), timeout_ms: policy(this.env.REMOTE_TIMEOUT_MS, "REMOTE_TIMEOUT_MS") };
    }

    private async registerHand(scope: string, requested: string | undefined, executor: Executor, descriptor: SessionDescriptor): Promise<void> {
      try { await executor.ready(); } catch { this.handSockets.delete(executor.socket); return; }
      let advertised: { name: string | null; runtime_id: string | null };
      try { advertised = await (await this.instance(scope)).identity(executor.catalog!, this.handLimits().max_frame_bytes); }
      catch (error) {
        this.handSockets.delete(executor.socket);
        return executor.reject(error instanceof Error ? error.message : String(error));
      }
      const hand = requested ?? advertised.name;
      if (!handName(hand)) {
        this.handSockets.delete(executor.socket);
        return executor.refuse(INVALID_HAND);
      }
      const connection = `${hand}#${crypto.randomUUID()}`;
      this.ctx.storage.sql.exec("INSERT INTO hand_sockets (connection, scope, hand, catalog, descriptor) VALUES (?, ?, ?, ?, ?)", connection, scope, hand, executor.catalog!, JSON.stringify(descriptor));
      this.handAuthority.set(connection, descriptor);
      executor.socket.serializeAttachment({ connection });
      const current = this.hands.get(scope)?.get(hand)?.executor;
      const incumbent = current && advertised.runtime_id !== null ? (await (await this.instance(scope)).identity(current.catalog!, this.handLimits().max_frame_bytes).catch(() => undefined))?.runtime_id : undefined;
      const previous = this.hands.get(scope)?.get(hand);
      this.indexHand(scope, hand, connection, executor, false);
      const hands = this.hands.get(scope)!;
      if (previous && previous.executor === current && incumbent && incumbent !== advertised.runtime_id) previous.executor.refuse(HAND_REPLACED);
      else if (previous) await previous.executor.dispose();
      const resident = this.runtimes.get(scope);
      const runtime = resident && await resident.catch(() => undefined);
      if (runtime && hands.get(hand)?.connection === connection) await runtime.attach(hand, connection).catch(() => {});
      const entry = hands.get(hand);
      if (entry?.connection !== connection) return;
      entry.ready = true;
      if (this.pumping || this.unfinished === false) return;
      const next = (await (await this.instance(scope)).pending(scope))[0];
      if ((next?.metadata as SessionDescriptor | null | undefined)?.hand === hand && !this.pumping) this.ctx.waitUntil(this.pump());
    }

    private indexHand(scope: string, hand: string, connection: string, executor: Executor, ready: boolean): void {
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
        const row = connection ? this.store.row("SELECT scope, hand, catalog, descriptor FROM hand_sockets WHERE connection = ?", connection) : undefined;
        if (!connection || !row) {
          try { socket.close(1011, "hand registration was lost"); } catch {}
          continue;
        }
        live.push(connection);
        if (row.descriptor) this.handAuthority.set(connection, JSON.parse(String(row.descriptor)));
        const executor = RemoteExecutor.restore(socket, this.handLimits(), String(row.catalog));
        this.handSockets.set(socket, executor);
        this.indexHand(String(row.scope), String(row.hand), connection, executor, true);
      }
      this.ctx.storage.sql.exec("DELETE FROM hand_sockets WHERE connection NOT IN (SELECT value FROM json_each(?))", JSON.stringify(live));
    }

    async webSocketMessage(socket: WebSocket, message: string | ArrayBuffer): Promise<void> {
      if (this.deleted) return;
      const executor = this.handSockets.get(socket);
      if (executor) return this.handFrame(socket, executor, message);
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
      const state: Watch = { watch: id, cursor: after };
      server.serializeAttachment(state);
      this.watchers.set(server, { state, scope, descriptor });
      this.notifyWatchers();
      return new Response(null, { status: 101, webSocket: client });
    }

    private watcher(socket: WebSocket): Watcher | undefined {
      const known = this.watchers.get(socket);
      if (known) return known;
      const state = socket.deserializeAttachment() as Watch | null;
      const row = state ? this.store.row("SELECT scope, descriptor FROM watch_sockets WHERE id = ?", state.watch) : undefined;
      if (!state || !row) return undefined;
      const watcher = { state, scope: String(row.scope), descriptor: JSON.parse(String(row.descriptor)) };
      this.watchers.set(socket, watcher);
      return watcher;
    }

    private forgetWatcher(socket: WebSocket): void {
      this.watchers.delete(socket);
      const state = socket.deserializeAttachment() as Watch | null;
      if (state?.watch) this.ctx.storage.sql.exec("DELETE FROM watch_sockets WHERE id = ?", state.watch);
    }

    private restoreWatchers(): void {
      const live = this.ctx.getWebSockets(WATCH_TAG).map(socket => (socket.deserializeAttachment() as Watch | null)?.watch).filter(Boolean);
      this.ctx.storage.sql.exec("DELETE FROM watch_sockets WHERE id NOT IN (SELECT value FROM json_each(?))", JSON.stringify(live));
    }

    private watching(): { socket: WebSocket; watcher: Watcher }[] {
      const list = [];
      for (const socket of this.ctx.getWebSockets(WATCH_TAG)) {
        const watcher = this.watcher(socket);
        if (!watcher) { try { socket.close(1011, "watch registration was lost"); } catch {} continue; }
        if (!this.current(watcher.descriptor)) { this.refuse(socket); continue; }
        list.push({ socket, watcher });
      }
      return list;
    }

    private deliver(socket: WebSocket, state: Watch, event: LoggedEvent): void {
      if (event.seq <= state.cursor) return;
      try { socket.send(JSON.stringify({ type: "event", seq: event.seq, request_id: event.request_id, created_at: event.created_at, event: event.event })); state.cursor = event.seq; } catch {}
    }

    private published(scope: string, events: LoggedEvent[], bytes: number[]): void {
      if (!events.length || this.deleted) return;
      this.counts.events_written += events.length;
      this.eventHub.publish(events, bytes);
      let behind = false;
      for (const { socket, watcher } of this.watching()) {
        if (watcher.scope !== scope) continue;
        this.recheck(socket, watcher.descriptor);
        if (watcher.state.cursor < events[0].seq - 1) { behind = true; continue; }
        const cursor = watcher.state.cursor;
        for (const event of events) this.deliver(socket, watcher.state, event);
        if (watcher.state.cursor !== cursor) socket.serializeAttachment(watcher.state);
      }
      if (behind) this.notifyWatchers();
    }

    private notifyWatchers(): void {
      if (this.flushing) { this.reflush = true; return; }
      this.flushing = true;
      this.ctx.waitUntil((async () => {
        try {
          do {
            this.reflush = false;
            await this.flushWatchers();
          } while (this.reflush && !this.deleted);
        } catch {} finally { this.flushing = false; }
      })());
    }

    private async flushWatchers(): Promise<void> {
      const page = policy(this.env.EVENT_PAGE_SIZE, "EVENT_PAGE_SIZE");
      const byScope = new Map<string, { socket: WebSocket; watcher: Watcher }[]>();
      for (const entry of this.watching()) {
        let list = byScope.get(entry.watcher.scope);
        if (!list) byScope.set(entry.watcher.scope, list = []);
        list.push(entry);
      }
      for (const [scope, list] of byScope) {
        let after = Math.min(...list.map(entry => entry.watcher.state.cursor));
        const before = list.map(entry => entry.watcher.state.cursor);
        for (;;) {
          const rows = await (await this.instance(scope)).page(scope, after, page);
          for (const event of rows.events) for (const { socket, watcher } of list) this.deliver(socket, watcher.state, event);
          if (!rows.has_more) break;
          after = rows.next_cursor;
        }
        list.forEach(({ socket, watcher }, index) => { if (watcher.state.cursor !== before[index]) socket.serializeAttachment(watcher.state); });
      }
    }

    private async watcherMessage(socket: WebSocket, message: string | ArrayBuffer): Promise<void> {
      const watcher = this.watcher(socket);
      if (!watcher) { try { socket.close(1011, "watch registration was lost"); } catch {} return; }
      if (!await this.allowed(watcher.descriptor)) return this.refuse(socket);
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
        const warmed = !watcher.descriptor.read_only && await this.warm(watcher.scope, watcher.descriptor).catch(() => false);
        socket.send(JSON.stringify({ type: "warm", warmed }));
      } else {
        socket.send(JSON.stringify({ type: "error", error: "unknown message type" }));
      }
    }

    private current(descriptor: SessionDescriptor): boolean {
      return descriptor.expires_at === undefined || descriptor.expires_at > Date.now();
    }

    private async allowed(descriptor: SessionDescriptor): Promise<boolean> {
      if (!this.current(descriptor)) return false;
      if (!options.active) return true;
      try { return await options.active(descriptor.account_id, descriptor.grant, this.env); } catch { return false; }
    }

    private async handAllowed(connection: string): Promise<boolean> {
      const descriptor = this.handAuthority.get(connection);
      return !descriptor || await this.allowed(descriptor);
    }

    private handFrame(socket: WebSocket, executor: Executor, message: string | ArrayBuffer): Promise<void> {
      const connection = (socket.deserializeAttachment() as { connection?: string } | null)?.connection;
      const deliver = async () => {
        if (connection !== undefined && !await this.handAllowed(connection)) return executor.dispose();
        executor.receive(message);
      };
      const next = (this.handFrames.get(socket) ?? Promise.resolve()).then(deliver, deliver);
      const settled = () => { if (this.handFrames.get(socket) === next) this.handFrames.delete(socket); };
      this.handFrames.set(socket, next);
      void next.then(settled, settled);
      return next;
    }

    private recheck(socket: WebSocket, descriptor: SessionDescriptor): void {
      if (!options.active || this.rechecks.has(socket)) return;
      this.rechecks.add(socket);
      this.ctx.waitUntil(this.allowed(descriptor).then(allowed => {
        this.rechecks.delete(socket);
        if (!allowed) this.refuse(socket);
      }));
    }

    private refuse(socket: WebSocket): void {
      this.forgetWatcher(socket);
      try { socket.send(JSON.stringify({ type: "error", error: "access token expired or revoked" })); } catch {}
      try { socket.close(1008, "access token expired or revoked"); } catch {}
    }

    private turnGrant(scope: string, fallback: SessionDescriptor): unknown {
      const background = this.metadata.get(`background:${scope}`);
      return background === undefined ? fallback.grant : (JSON.parse(background) as SessionDescriptor).grant;
    }

    private async warm(scope: string, base: SessionDescriptor): Promise<boolean> {
      const sessionId = this.metadata.get(this.sessionKey(scope));
      if (!sessionId) return false;
      const provider = (this.metadata.get(`model_provider:${scope}`) ?? "claude") as ModelProvider;
      const executor = this.metadata.get(`executor:${scope}`) ?? this.legacyExecutor();
      const descriptor: SessionDescriptor = { ...base, executor_id: executor || undefined, hand: undefined, provider, subscription: provider === "codex" ? base.codex_subscription ?? true : base.subscription };
      const resident = this.runtimes.get(scope);
      const busy = this.pumping || !this.queueKnownEmpty() || !!this.metadata.first("background:");
      const current = resident && busy ? await resident.catch(() => undefined) : undefined;
      const runtime = current?.reusable ? current : await this.runtime(scope, sessionId, descriptor);
      await runtime.preconnect();
      if (this.unfinished === undefined) {
        const admissions = this.admissions;
        const pending = await runtime.pending(scope);
        if (this.unfinished === undefined && admissions === this.admissions) this.unfinished = pending.length > 0;
      }
      if (!this.pumping && !this.unfinished && !this.metadata.first("background:")) this.releaseIdle(true);
      return true;
    }
    private executorEnabled(descriptor: SessionDescriptor): boolean {
      return !!descriptor.executor_id || this.env.RUNNER_ENABLED === "true";
    }

    private releaseIdle(warmed = false): void {
      const budget = policy(this.env.RUNTIME_MEMORY_BUDGET_BYTES, "RUNTIME_MEMORY_BUDGET_BYTES");
      for (const [scope, runtime] of this.residents) {
        if (runtime.memoryBytes > budget) this.retire(scope, runtime);
      }
      if (this.linger !== undefined) clearTimeout(this.linger);
      this.linger = undefined;
      if (!warmed && ![...this.residents.values()].some(runtime => runtime.remote || runtime.connected)) return;
      this.linger = setTimeout(() => {
        this.linger = undefined;
        if (this.pumping || !this.queueKnownEmpty()) return;
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
      const instance = this.instances.get(scope);
      void instance?.then(current => { if (current === runtime && this.instances.get(scope) === instance) this.instances.delete(scope); }, () => {});
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
      this.handAuthority.delete(connection);
      const hands = this.hands.get(scope);
      if (hands?.get(hand)?.connection === connection) hands.delete(hand);
      void this.residents.get(scope)?.detach(connection).catch(() => {});
    }

    private async destroy(): Promise<Response> {
      this.deleted = true;
      if (this.linger !== undefined) clearTimeout(this.linger);
      this.linger = undefined;
      for (const socket of this.ctx.getWebSockets()) { try { socket.close(1000, "session deleted"); } catch {} }
      const runtimes = [...this.runtimes.values(), ...this.instances.values()];
      this.runtimes.clear();
      this.residents.clear();
      this.instances.clear();
      this.eventHub.notify();
      await Promise.all(runtimes.map(resident => resident.then(runtime => runtime.dispose()).catch(() => {})));
      await this.ctx.storage.deleteAlarm();
      await this.ctx.storage.deleteAll();
      this.metadata.reset();
      return json({ deleted: true });
    }

    private finishing(id: string, current: AgentRuntime): Record<string, unknown> {
      const timing = this.turnTimings.get(id);
      const runtime = current.metrics.timing;
      const { started, admitted_at, checkpoints, runtime: before, run_started, ...phases } = timing ?? { started: performance.now(), checkpoints: {} };
      if (run_started !== undefined) phases.run_ms = performance.now() - run_started;
      return { timing: { ...phases, observed_ms: performance.now() - started, runtime_id: runtime.runtime_id, checkpoint_spans: timingDifference(runtime.checkpoints, checkpoints), runtime_spans: timingDifference(runtime.spans, before ?? {}) } };
    }

    private observed(id: string): { timing: Record<string, unknown> } {
      const { started, admitted_at, checkpoints, runtime: before, run_started, ...phases } = this.turnTimings.get(id) ?? { started: performance.now(), checkpoints: {} };
      return { timing: { ...phases, observed_ms: performance.now() - started, checkpoint_spans: {} } };
    }

    private async cancel(id: string, scope: string): Promise<Response> {
      const fields = { cancelled: true, error: "request cancelled", ...this.observed(id) };
      const { turn, dequeued } = await (await this.instance(scope)).cancel(scope, id, fields);
      if (!turn) return json({ error: "request not found" }, 404);
      const running = turn.report.status === "running";
      if (!running && !dequeued) return json(turn.report);
      if (running) this.blocked.add(id);
      else this.turnTimings.delete(id);
      if (!this.pumping) await this.ctx.storage.setAlarm(Date.now());
      await this.ctx.storage.sync();
      return json(turn.report, running ? 202 : 200);
    }

    private async socketEffect(scope: string): Promise<void> {
      await this.durable(scope);
      if (this.dispatchBlocked(scope)) throw new Error("request authority stopped");
    }

    private diagnostic(scope: string, header: string | null, timing: { runtime_id: string; dispatch_id: number; diagnostic?: unknown }): void {
      if (!header) return;
      try {
        const value = JSON.parse(header);
        timing.diagnostic = value;
        this.modelDiagnostics.push({ scope, value: { ...value, runtime_id: timing.runtime_id, dispatch_id: timing.dispatch_id } });
        if (this.modelDiagnostics.length > RECENT_MODEL_LIMIT) this.modelDiagnostics.shift();
      } catch {}
    }

    private dispatchBlocked(scope: string): boolean {
      const id = this.driving.get(scope);
      return id !== undefined && this.blocked.has(id);
    }

    private instance(scope: string, unopened = false): Promise<AgentRuntime> {
      if (this.deleted) return Promise.reject(new Error("session deleted"));
      let current = this.instances.get(scope);
      if (!current) {
        current = AgentRuntime.instantiate(wasm, this.store, { published: (events, bytes) => this.published(scope, events, bytes), wake: () => this.eventHub.notify() });
        const created = current;
        this.instances.set(scope, created);
        void created.catch(() => { if (this.instances.get(scope) === created) this.instances.delete(scope); });
      }
      const instance = current;
      return instance.then(runtime => {
        if (!runtime.closed && !(unopened && runtime.opened)) return runtime;
        if (this.instances.get(scope) === instance) this.instances.delete(scope);
        if (!runtime.closed) void runtime.dispose().catch(() => {});
        return this.instance(scope, unopened);
      });
    }

    private cursor(url: URL, request: Request): { after: number; limit: number } | undefined {
      const after = Math.max(Number(url.searchParams.get("after") ?? "0"), Number(request.headers.get("last-event-id") ?? "0"));
      const limit = Number(url.searchParams.get("limit") ?? this.env.EVENT_PAGE_SIZE);
      return Number.isSafeInteger(after) && after >= 0 && Number.isSafeInteger(limit) && limit >= 1 && limit <= policy(this.env.EVENT_PAGE_SIZE, "EVENT_PAGE_SIZE") ? { after, limit } : undefined;
    }

    private async events(scope: string, { after, limit }: { after: number; limit: number }, streaming = false, requestId?: string, descriptor?: SessionDescriptor): Promise<Response> {
      const read = async (cursor: number): Promise<LogPage> => {
        if (descriptor && !await this.allowed(descriptor)) throw new Error("access token expired or revoked");
        return this.eventHub.recent(cursor, requestId) ?? await (await this.instance(scope)).page(scope, cursor, limit, requestId);
      };
      if (streaming) return this.eventHub.stream(after, policy(this.env.EVENT_STREAM_KEEPALIVE_MS, "EVENT_STREAM_KEEPALIVE_MS"), read, requestId === undefined ? undefined : event => (event.event as { type?: unknown } | undefined)?.type === "result");
      return json(await (await this.instance(scope)).page(scope, after, limit));
    }

    private scopeTimings(scope: string): Timings {
      let value = this.timings.get(scope);
      if (!value) { value = new Timings(); this.timings.set(scope, value); }
      return value;
    }
  }

  async function fetch(request: Request, env: E): Promise<Response> {
    const started = performance.now();
    const principal = await options.authenticate(request, env);
    if (!principal) return json({ error: "unauthorized" }, 401);
    const account = typeof principal === "string" ? principal : principal.account;
    const url = new URL(request.url);
    const claude = env.CLAUDE_MODEL ?? "claude-opus-5-5";
    if (url.pathname === "/health") return json({ ok: true, model: claude, models: { claude, codex: "gpt-6.1-sol" }, auth: env.MODEL_AUTH ?? "api_key" });
    const head = request.method === "HEAD";
    if (url.pathname === "/metrics" && (request.method === "GET" || head)) return options.egress(env).fetch(new Request(url.searchParams.get("probe") === "codex" ? "https://egress.internal/probe/codex" : "https://egress.internal/metrics"));
    const match = /^\/sessions\/([^/]+)(?:\/(turns|events|metrics|socket|hands|crash|warm)(?:\/([^/]+)(?:\/(cancel))?)?)?$/.exec(url.pathname);
    if (!match || (match[3] !== undefined && match[2] !== "turns" && match[2] !== "hands") || (match[4] !== undefined && match[2] !== "turns")) return json({ error: "route not found" }, 404);
    let name: string;
    try { name = decodeURIComponent(match[1]); } catch { return json({ error: "invalid session encoding" }, 400); }
    if (!name || name.includes("\0")) return json({ error: "invalid session name" }, 400);
    if (Number(request.headers.get("content-length") ?? 0) > SQLITE_VALUE_LIMIT_BYTES) return json({ error: TOO_LARGE }, 413);
    const mode = (provider: ModelProvider) => options.subscription ? options.subscription(account, env, provider) : env.MODEL_AUTH === "subscription";
    const [subscription, codex_subscription] = await Promise.all([mode("claude"), mode("codex")]);
    const caller = typeof principal === "object" ? principal : undefined;
    const descriptor: SessionDescriptor = { account_id: account, session_id: "", session_name: name, subscription, codex_subscription, ...(caller?.read_only ? { read_only: true } : {}), ...(caller?.grant !== undefined ? { grant: caller.grant } : {}), ...(typeof caller?.expires_at === "number" ? { expires_at: caller.expires_at } : {}) };
    let path = `/internal/${match[2] ?? "metrics"}`;
    if (match[4]) path = `/internal/cancel/${match[3]}`;
    else if (match[3]) path += `/${match[3]}`;
    const headers = new Headers(request.headers);
    headers.delete("authorization");
    if (head) headers.delete("upgrade");
    headers.set("x-nanocode-descriptor", headerJson(descriptor));
    const target = new URL(request.url);
    target.pathname = path;
    const dispatched = performance.now();
    const response = await env.SESSIONS.get(env.SESSIONS.idFromName(JSON.stringify([account, name])), { locationHint: env.SESSION_LOCATION_HINT }).fetch(new Request(target, { method: head ? "GET" : request.method, headers, body: head ? null : request.body, redirect: "manual" }));
    if (response.webSocket) return response;
    const responseHeaders = new Headers(response.headers);
    responseHeaders.set("server-timing", `public_pre_session;dur=${dispatched - started}, session_response;dur=${performance.now() - dispatched}`);
    if (head) await response.body?.cancel();
    return new Response(head ? null : response.body, { status: response.status, statusText: response.statusText, headers: responseHeaders });
  }

  async function deleteSession(account: string, name: string, env: E): Promise<boolean> {
    const descriptor: SessionDescriptor = { account_id: account, session_id: "", session_name: name, subscription: false };
    const response = await env.SESSIONS.get(env.SESSIONS.idFromName(JSON.stringify([account, name])), { locationHint: env.SESSION_LOCATION_HINT }).fetch(new Request("https://session.internal/internal/delete", { method: "POST", headers: { "x-nanocode-descriptor": headerJson(descriptor) } }));
    await response.body?.cancel();
    return response.ok || response.status === 410;
  }

  return { AgentSession, fetch, deleteSession };
}

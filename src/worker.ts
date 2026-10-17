import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";
import { AgentRuntime } from "./host";
import { CheckpointStore } from "./store";
import { handleConnect } from "./connect/index";
export { SubscriptionAuth } from "./auth";
import { json, type AgentResult, type PrivateBinding, type SessionDescriptor } from "./contracts";

export interface Env {
  SESSIONS: DurableObjectNamespace<AgentSession>;
  EGRESS: PrivateBinding;
  AUTHORITY: PrivateBinding;
  SERVICE_EGRESS: PrivateBinding;
  WORKER_TOKEN: string;
  ACCOUNT_ID: string;
  MODEL_AUTH: string;
  RECOVERY_DELAY_MS: string;
  CONNECT_STREAM_REVALIDATE_MS: string;
  EVENT_PAGE_SIZE: string;
  PUBLIC_ORIGIN?: string;
}

type Job = { request_id: string; prompt: string; status: string; result: string | null; scope: string; descriptor: string; cancel_requested: number };
type Running = { job: Job; runtime: AgentRuntime };

function policy(value: string, name: string): number {
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number <= 0) throw new Error(`${name} must be a positive integer`);
  return number;
}

async function ownerAuthorized(request: Request, env: Env): Promise<boolean> {
  if (!env.WORKER_TOKEN) return false;
  const encoder = new TextEncoder();
  const [actual, expected] = await Promise.all([
    crypto.subtle.digest("SHA-256", encoder.encode(request.headers.get("authorization") ?? "")),
    crypto.subtle.digest("SHA-256", encoder.encode(`Bearer ${env.WORKER_TOKEN}`)),
  ]);
  return crypto.subtle.timingSafeEqual(actual, expected);
}

export default {
  async fetch(request, env): Promise<Response> {
    const authorized = await ownerAuthorized(request, env);
    const connect = await handleConnect(request, env, authorized);
    if (connect) return connect;
    if (!env.WORKER_TOKEN) return json({ error: "WORKER_TOKEN is not configured" }, 503);
    if (!authorized) return json({ error: "unauthorized" }, 401);
    const url = new URL(request.url);
    if (url.pathname === "/health") return json({ ok: true, model: "claude-opus-5-5", auth: env.MODEL_AUTH, architecture: "session-local", credential_boundary: "private-egress" });
    const match = /^\/sessions\/([^/]+)(?:\/(turns|events|metrics|socket)(?:\/([^/]+)(?:\/(cancel))?)?)?$/.exec(url.pathname);
    if (!match) return json({ error: "route not found" }, 404);
    let name: string;
    try { name = decodeURIComponent(match[1]); } catch { return json({ error: "invalid session encoding" }, 400); }
    if (!name || name.includes("\0")) return json({ error: "invalid session name" }, 400);
    const sessionName = `owner:${env.ACCOUNT_ID}:${name}`;
    const descriptor: SessionDescriptor = { account_id: env.ACCOUNT_ID, session_id: "", session_name: sessionName, subscription: env.MODEL_AUTH === "subscription" };
    let path = `/internal/${match[2] ?? "metrics"}`;
    if (match[4]) path = `/internal/cancel/${match[3]}`;
    else if (match[3]) path += `/${match[3]}`;
    const headers = new Headers(request.headers);
    headers.delete("authorization");
    headers.set("x-nanocode-descriptor", JSON.stringify(descriptor));
    const target = new URL(request.url);
    target.pathname = path;
    return env.SESSIONS.get(env.SESSIONS.idFromName(sessionName)).fetch(new Request(target, { method: request.method, headers, body: request.body, redirect: "manual" }));
  },
} satisfies ExportedHandler<Env>;

export class SessionControl extends WorkerEntrypoint<Env> {
  async fetch(request: Request): Promise<Response> {
    if (request.method !== "POST") return json({ error: "method not allowed" }, 405);
    let input: any;
    try { input = await request.json(); } catch { return json({ error: "invalid JSON" }, 400); }
    if (typeof input?.account_id !== "string" || typeof input.grant_id !== "string" || typeof input.session_name !== "string" || !Number.isSafeInteger(input.revision) || input.revision < 1) return json({ error: "invalid fence" }, 400);
    return this.env.SESSIONS.get(this.env.SESSIONS.idFromName(input.session_name)).fetch(new Request("https://session.internal/internal/fence", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(input) }));
  }
}

export class AgentSession extends DurableObject<Env> {
  private readonly store: CheckpointStore;
  private readonly runtimes = new Map<string, Promise<AgentRuntime>>();
  private running?: Running;
  private pumping = false;
  private eventSync?: Promise<void>;
  private readonly counts = { runtime_initializations: 0, model_dispatches: 0, events_written: 0, alarms: 0, recoveries: 0 };

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.store = new CheckpointStore(ctx.storage);
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS jobs (request_id TEXT PRIMARY KEY, prompt TEXT NOT NULL, status TEXT NOT NULL, result TEXT);
      CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY AUTOINCREMENT, scope TEXT NOT NULL, request_id TEXT, body TEXT NOT NULL, created_at INTEGER NOT NULL);
      CREATE INDEX IF NOT EXISTS unfinished_jobs ON jobs(status);
      CREATE INDEX IF NOT EXISTS scoped_events ON events(scope, seq);
      CREATE TABLE IF NOT EXISTS grant_fences (grant_id TEXT PRIMARY KEY, revision INTEGER NOT NULL);
    `);
    const columns = new Set(ctx.storage.sql.exec("PRAGMA table_info(jobs)").toArray().map(row => row.name));
    if (!columns.has("scope")) ctx.storage.sql.exec("ALTER TABLE jobs ADD COLUMN scope TEXT NOT NULL DEFAULT 'owner'");
    if (!columns.has("descriptor")) ctx.storage.sql.exec("ALTER TABLE jobs ADD COLUMN descriptor TEXT NOT NULL DEFAULT '{}'");
    if (!columns.has("cancel_requested")) ctx.storage.sql.exec("ALTER TABLE jobs ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0");
  }

  async fetch(request: Request): Promise<Response> {
    try {
      const url = new URL(request.url);
      if (url.pathname === "/internal/fence") return await this.fence(request);
      let descriptor: SessionDescriptor;
      try { descriptor = JSON.parse(request.headers.get("x-nanocode-descriptor") ?? "null"); } catch { return json({ error: "invalid session authority" }, 400); }
      if (!descriptor || typeof descriptor.account_id !== "string" || !descriptor.account_id || typeof descriptor.session_name !== "string" || !descriptor.session_name) return json({ error: "session authority is required" }, 403);
      const binding = this.store.row("SELECT value FROM metadata WHERE key = 'authority'")?.value;
      const identity = JSON.stringify({ account_id: descriptor.account_id, session_name: descriptor.session_name });
      if (binding && binding !== identity) return json({ error: "session authority mismatch" }, 403);
      if (this.isFenced(descriptor)) return json({ error: "grant revoked" }, 403);
      const scope = descriptor.grant_id ?? "owner";
      const parts = url.pathname.split("/");
      if (request.method === "GET" && parts[2] === "metrics") return json({ ...this.counts, resident_runtimes: this.runtimes.size, runtime: this.running?.runtime.metrics, persisted: this.store.row("SELECT (SELECT COUNT(*) FROM states) AS states, (SELECT COUNT(*) FROM records) AS records, (SELECT COUNT(*) FROM jobs WHERE scope = ? AND status IN ('queued', 'running')) AS unfinished", scope) });
      if (request.method === "GET" && parts[2] === "events") return this.events(scope, url);
      if (parts[2] === "socket") return json({ error: "use the authenticated event stream" }, 405);
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
      const existing = this.job(input.request_id);
      if (existing && (existing.prompt !== input.prompt || existing.scope !== scope)) return json({ error: "request_id already belongs to different input or authority" }, 409);
      if (!existing) {
        policy(this.env.RECOVERY_DELAY_MS, "RECOVERY_DELAY_MS");
        this.ctx.storage.transactionSync(() => {
          if (!binding) this.ctx.storage.sql.exec("INSERT INTO metadata VALUES ('authority', ?)", identity);
          this.sessionId(scope, descriptor.session_id);
          this.ctx.storage.sql.exec("INSERT INTO jobs (request_id, prompt, status, result, scope, descriptor, cancel_requested) VALUES (?, ?, 'queued', NULL, ?, ?, 0)", input.request_id, input.prompt, scope, JSON.stringify(descriptor));
          this.insertEvent(scope, input.request_id, { type: "queued", request_id: input.request_id });
        });
      }
      const job = existing ?? this.job(input.request_id)!;
      if ((job.status === "queued" || job.status === "running") && !this.pumping) await this.ctx.storage.setAlarm(Date.now());
      await this.ctx.storage.sync();
      return json(this.publicJob(job), ["queued", "running"].includes(job.status) ? 202 : 200);
    } catch (error) {
      if (error instanceof URIError) return json({ error: "invalid request encoding" }, 400);
      return json({ error: "session operation failed" }, 503);
    }
  }

  async alarm(): Promise<void> {
    this.counts.alarms++;
    const delay = policy(this.env.RECOVERY_DELAY_MS, "RECOVERY_DELAY_MS");
    if (this.pumping) { await this.ctx.storage.setAlarm(Date.now() + delay); return; }
    const job = this.ctx.storage.sql.exec<Job>("SELECT * FROM jobs WHERE status IN ('queued', 'running') ORDER BY rowid LIMIT 1").toArray()[0];
    if (!job) { await this.ctx.storage.deleteAlarm(); return; }
    this.pumping = true;
    let completed = false;
    let runtime: AgentRuntime | undefined;
    try {
      const descriptor = this.descriptor(job);
      if (job.cancel_requested || this.isFenced(descriptor)) {
        this.finish(job, { cancelled: true, error: "request cancelled" });
        completed = true;
        return;
      }
      if (job.status === "running") this.counts.recoveries++;
      const sessionId = this.sessionId(job.scope, descriptor.session_id);
      this.ctx.storage.sql.exec("UPDATE jobs SET status = 'running', result = NULL WHERE request_id = ?", job.request_id);
      await this.ctx.storage.setAlarm(Date.now() + delay);
      await this.ctx.storage.sync();
      runtime = await this.runtime(job.scope, sessionId, descriptor);
      this.running = { job, runtime };
      const latest = this.job(job.request_id)!;
      if (latest.cancel_requested || this.isFenced(descriptor)) {
        this.finish(job, { cancelled: true, error: "request cancelled" });
        completed = true;
        return;
      }
      await this.appendEvent(job.scope, job.request_id, { type: "running", request_id: job.request_id });
      const result = await runtime.run({ request_id: job.request_id, prompt: job.prompt });
      if (result.retry) throw new Error("runtime recovery required");
      this.finish(job, result);
      completed = true;
    } catch {
      this.ctx.storage.sql.exec("UPDATE jobs SET result = ? WHERE request_id = ? AND status = 'running'", JSON.stringify({ error: "Agent execution interrupted; recovery scheduled" }), job.request_id);
      this.runtimes.delete(job.scope);
      if (runtime) await runtime.dispose().catch(() => {});
    } finally {
      this.running = undefined;
      this.pumping = false;
      if (this.store.row("SELECT request_id FROM jobs WHERE status IN ('queued', 'running') LIMIT 1")) await this.ctx.storage.setAlarm(Date.now() + (completed ? 0 : delay));
      else await this.ctx.storage.deleteAlarm();
      await this.ctx.storage.sync();
    }
  }

  private descriptor(job: Job): SessionDescriptor {
    const value = JSON.parse(job.descriptor);
    if (value.account_id) return value;
    const identity = this.store.row("SELECT value FROM metadata WHERE key = 'authority'")?.value;
    return { ...(identity ? JSON.parse(String(identity)) : { account_id: this.env.ACCOUNT_ID, session_name: this.ctx.id.toString() }), session_id: "", subscription: this.env.MODEL_AUTH === "subscription" };
  }

  private sessionId(scope: string, suggested?: string): string {
    const key = scope === "owner" ? "session_id" : `session_id:${scope}`;
    let session = this.store.row("SELECT value FROM metadata WHERE key = ?", key)?.value as string | undefined;
    if (!session) {
      session = scope === "owner" && suggested ? suggested : crypto.randomUUID();
      this.ctx.storage.sql.exec("INSERT INTO metadata VALUES (?, ?)", key, session);
    }
    return session;
  }

  private runtime(scope: string, sessionId: string, descriptor: SessionDescriptor): Promise<AgentRuntime> {
    let resident = this.runtimes.get(scope);
    if (!resident) {
      this.counts.runtime_initializations++;
      resident = AgentRuntime.create({ session_id: sessionId, subscription: descriptor.subscription, endpoint: `https://api.anthropic.com/v1/messages${descriptor.subscription ? "?beta=true" : ""}` }, this.store, {
        dispatch: async request => {
          if (this.isFenced(descriptor)) throw new Error("grant revoked");
          this.counts.model_dispatches++;
          return this.env.EGRESS.fetch(new Request("https://egress.internal/model", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ ...request, authority: { ...descriptor, session_id: sessionId }, subscription: descriptor.subscription }) }));
        },
        event: event => this.appendEvent(scope, this.running?.job.scope === scope ? this.running.job.request_id : null, event),
      });
      this.runtimes.set(scope, resident);
      void resident.catch(() => { if (this.runtimes.get(scope) === resident) this.runtimes.delete(scope); });
    }
    return resident;
  }

  private finish(job: Job, result: AgentResult): void {
    const status = result.cancelled ? "cancelled" : result.error ? "failed" : "completed";
    this.ctx.storage.transactionSync(() => {
      this.ctx.storage.sql.exec("UPDATE jobs SET status = ?, result = ? WHERE request_id = ?", status, JSON.stringify(result), job.request_id);
      this.insertEvent(job.scope, job.request_id, { type: "result", request_id: job.request_id, status, ...result });
    });
  }

  private async cancel(id: string, scope: string): Promise<Response> {
    const job = this.job(id);
    if (!job || job.scope !== scope) return json({ error: "request not found" }, 404);
    if (!["queued", "running"].includes(job.status)) return json(this.publicJob(job));
    this.ctx.storage.sql.exec("UPDATE jobs SET cancel_requested = 1 WHERE request_id = ?", id);
    if (job.status === "queued") this.finish(job, { cancelled: true, error: "request cancelled" });
    else if (this.running?.job.request_id === id) this.ctx.waitUntil(this.running.runtime.cancel());
    if (!this.pumping) await this.ctx.storage.setAlarm(Date.now());
    await this.ctx.storage.sync();
    return json(this.publicJob(this.job(id)!), job.status === "running" ? 202 : 200);
  }

  private async fence(request: Request): Promise<Response> {
    const input = await request.json<any>();
    const binding = this.store.row("SELECT value FROM metadata WHERE key = 'authority'")?.value;
    if (binding) {
      const identity = JSON.parse(String(binding));
      if (identity.account_id !== input.account_id || identity.session_name !== input.session_name) return json({ error: "fence authority mismatch" }, 403);
    }
    if (typeof input.grant_id !== "string" || !Number.isSafeInteger(input.revision) || input.revision < 1) return json({ error: "invalid fence" }, 400);
    this.ctx.storage.transactionSync(() => {
      this.ctx.storage.sql.exec("INSERT INTO grant_fences VALUES (?, ?) ON CONFLICT(grant_id) DO UPDATE SET revision = MAX(revision, excluded.revision)", input.grant_id, input.revision);
      this.ctx.storage.sql.exec("UPDATE jobs SET cancel_requested = 1 WHERE scope = ? AND status IN ('queued', 'running')", input.grant_id);
      const queued = this.ctx.storage.sql.exec<Job>("SELECT * FROM jobs WHERE scope = ? AND status = 'queued'", input.grant_id).toArray();
      for (const job of queued) this.finish(job, { cancelled: true, error: "grant revoked" });
      this.insertEvent(input.grant_id, null, { type: "grant_revoked", revision: input.revision });
    });
    if (this.running?.job.scope === input.grant_id) this.ctx.waitUntil(this.running.runtime.cancel());
    if (!this.pumping && this.store.row("SELECT request_id FROM jobs WHERE status IN ('queued', 'running') LIMIT 1")) await this.ctx.storage.setAlarm(Date.now());
    await this.ctx.storage.sync();
    return json({ ok: true, revision: input.revision });
  }

  private isFenced(descriptor: SessionDescriptor): boolean {
    return !!descriptor.grant_id && !!this.store.row("SELECT revision FROM grant_fences WHERE grant_id = ?", descriptor.grant_id);
  }

  private events(scope: string, url: URL): Response {
    const after = Number(url.searchParams.get("after") ?? "0");
    const limit = Number(url.searchParams.get("limit") ?? this.env.EVENT_PAGE_SIZE);
    const maximum = policy(this.env.EVENT_PAGE_SIZE, "EVENT_PAGE_SIZE");
    if (!Number.isSafeInteger(after) || after < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > maximum) return json({ error: `after must be a nonnegative integer and limit must be between 1 and ${maximum}` }, 400);
    const events = this.ctx.storage.sql.exec<{ seq: number; request_id: string | null; body: string; created_at: number }>("SELECT seq, request_id, body, created_at FROM events WHERE scope = ? AND seq > ? ORDER BY seq LIMIT ?", scope, after, limit).toArray().map(row => ({ seq: row.seq, request_id: row.request_id, created_at: row.created_at, event: JSON.parse(row.body) }));
    return json({ events, next_cursor: events.at(-1)?.seq ?? after, has_more: events.length === limit });
  }

  private insertEvent(scope: string, requestId: string | null, event: unknown): void {
    this.ctx.storage.sql.exec("INSERT INTO events (scope, request_id, body, created_at) VALUES (?, ?, ?, ?)", scope, requestId, JSON.stringify(event), Date.now());
    this.counts.events_written++;
  }

  private async appendEvent(scope: string, requestId: string | null, event: unknown): Promise<void> {
    this.insertEvent(scope, requestId, event);
    if (!this.eventSync) this.eventSync = Promise.resolve().then(() => this.ctx.storage.sync()).finally(() => { this.eventSync = undefined; });
    await this.eventSync;
  }

  private job(id: string): Job | undefined {
    return this.ctx.storage.sql.exec<Job>("SELECT * FROM jobs WHERE request_id = ?", id).toArray()[0];
  }

  private publicJob(job: Job) {
    return { request_id: job.request_id, status: job.status, ...(job.result ? JSON.parse(job.result) : {}) };
  }
}

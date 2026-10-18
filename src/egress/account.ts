import { DurableObject } from "cloudflare:workers";
import { DEFAULT_EXECUTOR, LEGACY_RUNNER, runnerConnection, sessionUuid, type AuthorityCommand, type AuthorityResult, type ModelProvider, type SessionAuthority, type SessionDescriptor } from "../contracts";
import { ACCOUNT_PATTERN, base64url, CODEX, connections, exactOrigin, executorConnection, executorId, executors, executorSelected, Failure, jwtClaims, mode, modelAccount, modelProvider, open, placeCredential, random, readLimited, seal, SECRET_PATTERN, setting, sha256, sha256hex, SUBSCRIPTION, vaultConfigured, type Connection, type EgressEnv } from "./common";

export type Access = { ok: true; generation: number; expires_at: number | null; headers: [string, string][] } | { ok: false; status: number; error: string };
export type ServiceAccess = { ok: true; grant_id: string; generation: number; expires_at: number | null; refreshable: boolean; headers: [string, string][]; secrets: string[] } | { ok: false; status: number; error: string };
export type ExecutorAccess = { ok: true; executor_id: string } | { ok: false; status: number; error: string };
export type ServiceRequest = { account_id: string; token: string; origin: string; app_id?: string; connection_id: string; kind: "connector" | "vault"; rejected_generation?: number };

type Grant = { grant_id: string; app_id: string; origin: string; principal: string; scopes: string; connections: string; catalog_digest: string | null; client_id: string | null; revision: number; status: string; created_at: number; expires_at: number | null };
type Client = { client_id: string; app_id: string; origins: string; redirect_uris: string; created_at: number };
type Code = { digest: string; client_id: string; origin: string; redirect_uri: string; principal: string; scopes: string; connections: string; challenge: string; expires_at: number; grant_id: string | null };
type Thread = { thread_id: string; app_id: string; origin: string; principal: string; session_name: string; session_id: string; created_at: number };
type Fence = { grant_id: string; session_name: string; revision: number; attempts: number; next_at: number };
type CredentialRow = { name: string; seed: string; envelope: string; generation: number; state: string; forced: number; expires_at: number | null; updated_at: number };
type StoredToken = { access: string; refresh: string; expires_at: number | null; scopes: string[]; refresh_expires_at: number | null; account_id?: string; fedramp?: boolean };
type Pending = { access: string; refresh: string; access_digest: string; refresh_digest: string };
type TokenPolicy = { rejected(status: number, body: string): string | undefined; accept(body: Record<string, unknown>, now: number): StoredToken | string | null };
type Identity = { account_id?: string; fedramp?: boolean };

const SCHEMA = `
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS clients (client_id TEXT PRIMARY KEY, app_id TEXT NOT NULL UNIQUE, origins TEXT NOT NULL, redirect_uris TEXT NOT NULL, created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS grants (grant_id TEXT PRIMARY KEY, app_id TEXT NOT NULL, origin TEXT NOT NULL, principal TEXT NOT NULL, scopes TEXT NOT NULL, connections TEXT NOT NULL, catalog_digest TEXT, client_id TEXT, revision INTEGER NOT NULL, status TEXT NOT NULL, created_at INTEGER NOT NULL, expires_at INTEGER);
CREATE TABLE IF NOT EXISTS tokens (digest TEXT PRIMARY KEY, kind TEXT NOT NULL, grant_id TEXT NOT NULL, expires_at INTEGER NOT NULL, used_at INTEGER);
CREATE INDEX IF NOT EXISTS tokens_by_grant ON tokens(grant_id);
CREATE INDEX IF NOT EXISTS tokens_by_expiry ON tokens(expires_at);
CREATE TABLE IF NOT EXISTS codes (digest TEXT PRIMARY KEY, client_id TEXT NOT NULL, origin TEXT NOT NULL, redirect_uri TEXT NOT NULL, principal TEXT NOT NULL, scopes TEXT NOT NULL, connections TEXT NOT NULL, challenge TEXT NOT NULL, expires_at INTEGER NOT NULL, grant_id TEXT);
CREATE TABLE IF NOT EXISTS threads (thread_id TEXT PRIMARY KEY, app_id TEXT NOT NULL, origin TEXT NOT NULL, principal TEXT NOT NULL, session_name TEXT NOT NULL UNIQUE, session_id TEXT NOT NULL, created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS grant_sessions (grant_id TEXT NOT NULL, session_name TEXT NOT NULL, session_id TEXT NOT NULL, PRIMARY KEY (grant_id, session_name));
CREATE TABLE IF NOT EXISTS fences (grant_id TEXT NOT NULL, session_name TEXT NOT NULL, revision INTEGER NOT NULL, attempts INTEGER NOT NULL, next_at INTEGER NOT NULL, PRIMARY KEY (grant_id, session_name));
CREATE TABLE IF NOT EXISTS credentials (name TEXT PRIMARY KEY, seed TEXT NOT NULL, envelope TEXT NOT NULL, generation INTEGER NOT NULL, state TEXT NOT NULL, forced INTEGER NOT NULL, expires_at INTEGER, updated_at INTEGER NOT NULL);
`;

const APP_ID = /^[A-Za-z0-9_.:-]{1,128}$/;
const SCOPE = /^[\x21\x23-\x5b\x5d-\x7e]{1,128}$/;
const DIGEST = /^[A-Za-z0-9_.:+/=-]{1,256}$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const CLIENT_ID = /^ncl_[A-Za-z0-9_-]{22}$/;
const CHALLENGE = /^[A-Za-z0-9_-]{43}$/;
const VERIFIER = /^[A-Za-z0-9._~-]{43,128}$/;
const TOKEN = /^(nc1|nr1|na1)\.([A-Za-z0-9_-]{1,64})\.([A-Za-z0-9_-]{43})$/;
const FENCE_BACKOFF_CAP = 3_600_000;
const CODEX_ACCOUNT = /^[A-Za-z0-9_.:-]{1,128}$/;
const CODEX_CREDENTIAL = "model:codex";
const CODEX_LOGIN_REQUIRED = new Set(["refresh_token_expired", "refresh_token_reused", "refresh_token_invalidated", "invalid_grant"]);

function failure(error: unknown): { ok: false; status: number; error: string } {
  if (error instanceof Failure) return { ok: false, status: error.status, error: error.code };
  return { ok: false, status: 500, error: "internal_error" };
}

function text(value: unknown, pattern: RegExp): string {
  if (typeof value !== "string" || !pattern.test(value)) throw new Failure(400, "invalid_request");
  return value;
}

function optional(value: unknown, pattern: RegExp): string | undefined {
  return value === undefined || value === null ? undefined : text(value, pattern);
}

function principal(value: unknown): string {
  if (typeof value !== "string" || !value || value.length > 256 || /[\u0000-\u001f\u007f]/.test(value)) throw new Failure(400, "invalid_request");
  return value;
}

function list(value: unknown, check: (item: unknown) => string, min: number, max: number): string[] {
  if (!Array.isArray(value) || value.length < min || value.length > max) throw new Failure(400, "invalid_request");
  const items = value.map(check);
  if (new Set(items).size !== items.length) throw new Failure(400, "invalid_request");
  return items;
}

function redirectUri(value: unknown): string {
  if (typeof value !== "string" || value.length > 2048) throw new Failure(400, "invalid_request");
  let url: URL;
  try { url = new URL(value); } catch { throw new Failure(400, "invalid_request"); }
  const loopback = url.hostname === "localhost" || url.hostname === "127.0.0.1" || url.hostname === "[::1]";
  if (url.hash || url.username || url.password || !(url.protocol === "https:" || (url.protocol === "http:" && loopback)) || url.toString() !== value) throw new Failure(400, "invalid_request");
  return value;
}

function token(prefix: "nc1" | "nr1" | "na1", account: string, value: unknown, status = 401, code = "invalid_token"): string {
  const match = typeof value === "string" ? TOKEN.exec(value) : null;
  if (!match || match[1] !== prefix || match[2] !== account) throw new Failure(status, code);
  return value as string;
}

function safeGrant(grant: Grant) {
  return {
    grant_id: grant.grant_id,
    revision: grant.revision,
    status: grant.status,
    app_id: grant.app_id,
    origin: grant.origin,
    principal: grant.principal,
    scopes: JSON.parse(grant.scopes) as string[],
    connections: JSON.parse(grant.connections) as string[],
    catalog_digest: grant.catalog_digest,
    client_id: grant.client_id,
    created_at: grant.created_at,
    expires_at: grant.expires_at,
  };
}

function oauthError(body: string): { error?: unknown; error_description?: unknown } {
  try {
    const value = JSON.parse(body);
    return value && typeof value === "object" ? value : {};
  } catch {
    return {};
  }
}

function subscriptionSeed(source: string): StoredToken {
  let document: any;
  try { document = JSON.parse(source); } catch { throw new Failure(500, "configuration_error"); }
  const entry = document?.anthropic ?? document?.claudeAiOauth ?? document;
  const access = entry?.access ?? entry?.accessToken;
  const refresh = entry?.refresh ?? entry?.refreshToken;
  const expires = entry?.expires ?? entry?.expiresAt;
  const scopes = Array.isArray(entry?.scopes) ? entry.scopes.filter((item: unknown) => typeof item === "string") : typeof entry?.scope === "string" ? entry.scope.split(/\s+/).filter(Boolean) : [];
  if (typeof access !== "string" || !SECRET_PATTERN.test(access) || typeof refresh !== "string" || !SECRET_PATTERN.test(refresh) || !Number.isSafeInteger(expires)) throw new Failure(500, "configuration_error");
  return { access, refresh, expires_at: expires, scopes, refresh_expires_at: null };
}

function codexIdentity(jwt: string): Identity | null {
  const claims = jwtClaims(jwt);
  if (!claims) return null;
  const auth = claims[CODEX.claims];
  if (auth === undefined) return {};
  if (!auth || typeof auth !== "object" || Array.isArray(auth)) return null;
  const account = auth.chatgpt_account_id ?? undefined;
  const fedramp = auth.chatgpt_account_is_fedramp ?? undefined;
  if ((account !== undefined && (typeof account !== "string" || !CODEX_ACCOUNT.test(account))) || (fedramp !== undefined && typeof fedramp !== "boolean")) return null;
  return { account_id: account, fedramp };
}

function codexExpiry(access: string): number | null {
  const exp = jwtClaims(access)?.exp;
  return Number.isSafeInteger(exp) && exp > 0 ? exp * 1000 : null;
}

function codexSeed(source: string): StoredToken {
  const fail = (): never => { throw new Failure(500, "configuration_error"); };
  let document: any;
  try { document = JSON.parse(source); } catch { return fail(); }
  if (!document || typeof document !== "object" || Array.isArray(document)) fail();
  if (document.auth_mode !== undefined && document.auth_mode !== null && document.auth_mode !== "chatgpt") fail();
  const entry = document.tokens ?? document;
  if (!entry || typeof entry !== "object") fail();
  const access = entry.access_token ?? entry.access;
  const refresh = entry.refresh_token ?? entry.refresh;
  const id = entry.id_token ?? undefined;
  const explicit = entry.account_id ?? undefined;
  if (typeof access !== "string" || !SECRET_PATTERN.test(access) || typeof refresh !== "string" || !SECRET_PATTERN.test(refresh)) fail();
  if ((id !== undefined && typeof id !== "string") || (explicit !== undefined && (typeof explicit !== "string" || !CODEX_ACCOUNT.test(explicit)))) fail();
  const accessIdentity = codexIdentity(access) ?? fail();
  const idIdentity = id === undefined ? {} : codexIdentity(id) ?? fail();
  const accounts = new Set([explicit, idIdentity.account_id, accessIdentity.account_id].filter((value): value is string => value !== undefined));
  if (accounts.size !== 1) fail();
  const stated = entry.expires ?? undefined;
  if (stated !== undefined && !Number.isSafeInteger(stated)) fail();
  const expires = codexExpiry(access) ?? stated ?? fail();
  return { access, refresh, expires_at: expires, scopes: [], refresh_expires_at: null, account_id: [...accounts][0], fedramp: idIdentity.fedramp ?? accessIdentity.fedramp ?? false };
}

function codexPolicy(current: StoredToken, minimumLifetime: number): TokenPolicy {
  return {
    rejected(status, body) {
      const value = oauthError(body) as Record<string, any>;
      const code = typeof value.error === "string" ? value.error : typeof value.error?.code === "string" ? value.error.code : typeof value.code === "string" ? value.code : undefined;
      return status === 401 || (code !== undefined && [400, 401, 403].includes(status) && CODEX_LOGIN_REQUIRED.has(code)) ? "login_required" : undefined;
    },
    accept(body, now) {
      const access = body.access_token;
      const refresh = body.refresh_token ?? undefined;
      const id = body.id_token ?? undefined;
      if (typeof access !== "string" || !SECRET_PATTERN.test(access)) return null;
      if (refresh !== undefined && (typeof refresh !== "string" || !SECRET_PATTERN.test(refresh))) return null;
      if (id !== undefined && typeof id !== "string") return null;
      if (body.token_type !== undefined && (typeof body.token_type !== "string" || body.token_type.toLowerCase() !== "bearer")) return null;
      const accessIdentity = codexIdentity(access);
      const idIdentity = id === undefined ? {} : codexIdentity(id);
      if (!accessIdentity || !idIdentity) return null;
      if ([accessIdentity.account_id, idIdentity.account_id].some(account => account !== undefined && account !== current.account_id)) return "account_mismatch";
      const expiresIn = body.expires_in;
      const expires = codexExpiry(access) ?? (Number.isSafeInteger(expiresIn) && (expiresIn as number) > 0 ? now + (expiresIn as number) * 1000 : null);
      if (expires === null || expires - now <= minimumLifetime) return null;
      return {
        access,
        refresh: refresh ?? current.refresh,
        expires_at: expires,
        scopes: current.scopes,
        refresh_expires_at: current.refresh_expires_at,
        account_id: current.account_id,
        fedramp: idIdentity.fedramp ?? accessIdentity.fedramp ?? current.fedramp ?? false,
      };
    },
  };
}

export class AccountAuthority extends DurableObject<EgressEnv> {
  private account?: string;
  private gates = new Map<string, Promise<unknown>>();
  private counters = { refreshes: 0, refresh_failures: 0, fence_deliveries: 0, fence_failures: 0 };

  constructor(ctx: DurableObjectState, env: EgressEnv) {
    super(ctx, env);
    ctx.storage.sql.exec(SCHEMA);
  }

  async command(command: AuthorityCommand): Promise<AuthorityResult> {
    try {
      const account = this.bind(command?.account_id);
      switch (command.op) {
        case "health": return await this.health(account);
        case "register_client": return this.registerClient(command);
        case "create_grant": return await this.createGrant(account, command);
        case "authorize_code": return await this.authorizeCode(account, command);
        case "exchange_code": return await this.exchangeCode(account, command);
        case "refresh": return await this.refresh(account, command);
        case "authorize": return await this.authorize(account, command);
        case "revoke": return await this.revoke(account, command);
        case "inspect": return this.inspect(command);
        case "list_connections": return await this.listConnections(account);
        default: return { ok: false, status: 400, error: "unsupported_op" };
      }
    } catch (error) {
      return failure(error);
    }
  }

  async modelAccess(input: { account_id: string; authority: SessionAuthority; rejected_generation?: number; provider?: ModelProvider }): Promise<Access> {
    try {
      const account = this.bind(input?.account_id);
      const selected = modelProvider(input.provider);
      if (account !== modelAccount(this.env) || input.authority?.account_id !== account) throw new Failure(403, "forbidden");
      if (input.authority.grant_id !== undefined) this.sessionGrant(input.authority);
      const rejected = input.rejected_generation;
      if (rejected !== undefined && !Number.isSafeInteger(rejected)) throw new Failure(400, "invalid_request");
      if (selected === "codex") {
        if (!this.env.CODEX_OAUTH_JSON) throw new Failure(500, "configuration_error");
        if (!vaultConfigured(this.env)) throw new Failure(503, "vault_key_required");
        return await this.gate(CODEX_CREDENTIAL, () => this.codex(rejected));
      }
      const current = mode(this.env);
      if (current === "unconfigured") throw new Failure(500, "configuration_error");
      if (current === "api_key") {
        if (rejected !== undefined) throw new Failure(401, "credential_rejected");
        return { ok: true, generation: 0, expires_at: null, headers: [["x-api-key", this.env.ANTHROPIC_API_KEY!]] };
      }
      return await this.gate("model", () => this.subscription(rejected));
    } catch (error) {
      return failure(error);
    }
  }

  async serviceAccess(input: ServiceRequest): Promise<ServiceAccess> {
    try {
      const account = this.bind(input?.account_id);
      const bearer = token("nc1", account, input.token);
      const origin = exactOrigin(input.origin);
      const app = optional(input.app_id, APP_ID);
      const id = text(input.connection_id, /^[A-Za-z0-9_-]{1,64}$/);
      if (input.kind !== "connector" && input.kind !== "vault") throw new Failure(400, "invalid_request");
      const rejected = input.rejected_generation;
      if (rejected !== undefined && !Number.isSafeInteger(rejected)) throw new Failure(400, "invalid_request");
      const digest = await sha256hex(bearer);
      const grant = this.ctx.storage.transactionSync(() => {
        const grant = this.liveGrant(digest, Date.now());
        if (grant.origin !== origin) throw new Failure(403, "origin_mismatch");
        if (app !== undefined && app !== grant.app_id) throw new Failure(403, "forbidden");
        if (!(JSON.parse(grant.connections) as string[]).includes(id)) throw new Failure(403, "forbidden");
        return grant;
      });
      return await this.resolveConnection(account, id, input.kind, grant.grant_id, rejected);
    } catch (error) {
      return failure(error);
    }
  }

  async sessionCatalog(input: { account_id: string; authority: SessionAuthority }): Promise<AuthorityResult> {
    try {
      const account = this.bind(input.account_id);
      if (account !== modelAccount(this.env) || input.authority?.account_id !== account) throw new Failure(403, "forbidden");
      const grant = input.authority.grant_id ? this.sessionGrant(input.authority) : undefined;
      const selected = grant ? JSON.parse(grant.connections) as string[] : undefined;
      const result = await this.listConnections(account);
      if (selected && Array.isArray(result.connections)) result.connections = result.connections.filter((connection: any) => selected.includes(connection.connection_id));
      return result;
    } catch (error) { return failure(error); }
  }

  async sessionServiceAccess(input: { account_id: string; authority: SessionAuthority; connection_id: string; kind: "connector" | "vault"; rejected_generation?: number }): Promise<ServiceAccess> {
    try {
      const account = this.bind(input.account_id);
      if (account !== modelAccount(this.env) || input.authority?.account_id !== account) throw new Failure(403, "forbidden");
      const grant = input.authority.grant_id ? this.sessionGrant(input.authority) : undefined;
      const id = text(input.connection_id, /^[A-Za-z0-9_-]{1,64}$/);
      if (grant && !(JSON.parse(grant.connections) as string[]).includes(id)) throw new Failure(403, "forbidden");
      if (input.kind !== "connector" && input.kind !== "vault") throw new Failure(400, "invalid_request");
      if (input.rejected_generation !== undefined && !Number.isSafeInteger(input.rejected_generation)) throw new Failure(400, "invalid_request");
      return await this.resolveConnection(account, id, input.kind, grant?.grant_id ?? "", input.rejected_generation);
    } catch (error) { return failure(error); }
  }

  async executorAccess(input: { account_id: string; authority: SessionAuthority }): Promise<ExecutorAccess> {
    try {
      const account = this.bind(input?.account_id);
      if (account !== modelAccount(this.env) || input.authority?.account_id !== account || typeof input.authority.session_id !== "string") throw new Failure(403, "forbidden");
      const id = executorId(input.authority.executor_id);
      if (!executors(this.env).has(id)) throw new Failure(503, "executor_unavailable");
      if (input.authority.grant_id !== undefined) {
        const grant = this.sessionGrant(input.authority);
        if (!executorSelected(JSON.parse(grant.connections) as string[], id)) throw new Failure(403, "forbidden");
      }
      return { ok: true, executor_id: id };
    } catch (error) { return failure(error); }
  }

  private async resolveConnection(account: string, id: string, kind: "connector" | "vault", grantId: string, rejected?: number): Promise<ServiceAccess> {
    const connection = connections(this.env).get(id);
    if (!connection || !connection.accounts.includes(account)) throw new Failure(404, "not_found");
    if (kind === "connector" && connection.kind === "vault") throw new Failure(403, "forbidden");
    const credential = connection.credential;
    if (credential.type === "static") {
      if (rejected !== undefined) throw new Failure(401, "credential_rejected");
      return { ok: true, grant_id: grantId, generation: 0, expires_at: null, refreshable: false, headers: [placeCredential(connection.auth, credential.value)], secrets: [credential.value] };
    }
    if (!vaultConfigured(this.env)) throw new Failure(503, "vault_key_required");
    const resolved = await this.gate(`connection:${id}`, () => this.connectionAccess(connection, rejected));
    return { ok: true, grant_id: grantId, generation: resolved.generation, expires_at: resolved.expires_at, refreshable: true, headers: [placeCredential(connection.auth, resolved.access)], secrets: [resolved.access] };
  }

  async alarm(): Promise<void> {
    await this.deliver();
  }

  private rows<T extends Record<string, SqlStorageValue>>(query: string, ...bindings: unknown[]): T[] {
    return this.ctx.storage.sql.exec<T>(query, ...bindings.map(value => value === undefined ? null : value)).toArray();
  }

  private row<T extends Record<string, SqlStorageValue>>(query: string, ...bindings: unknown[]): T | undefined {
    return this.rows<T>(query, ...bindings)[0];
  }

  private run(query: string, ...bindings: unknown[]): void {
    this.ctx.storage.sql.exec(query, ...bindings.map(value => value === undefined ? null : value));
  }

  private bind(value: unknown): string {
    if (typeof value !== "string" || !ACCOUNT_PATTERN.test(value)) throw new Failure(400, "invalid_request");
    if (this.account === undefined) {
      const bound = this.row<{ value: string }>("SELECT value FROM meta WHERE key = 'account_id'")?.value;
      if (bound === undefined) this.run("INSERT INTO meta VALUES ('account_id', ?)", value);
      this.account = bound ?? value;
    }
    if (this.account !== value) throw new Failure(409, "account_mismatch");
    return value;
  }

  private gate<T>(name: string, run: () => Promise<T>): Promise<T> {
    const next = (this.gates.get(name) ?? Promise.resolve()).then(run);
    const settled = next.then(() => {}, () => {});
    this.gates.set(name, settled);
    void settled.then(() => { if (this.gates.get(name) === settled) this.gates.delete(name); });
    return next;
  }

  private connectionIds(account: string, value: unknown): string[] {
    const available = connections(this.env);
    return list(value, item => {
      const executor = typeof item === "string" ? executorConnection(item) : undefined;
      if (executor !== undefined) {
        if (account !== modelAccount(this.env) || !executors(this.env).has(executor)) throw new Failure(400, "invalid_request");
        return item as string;
      }
      const id = text(item, /^[A-Za-z0-9_-]{1,64}$/);
      if (!available.get(id)?.accounts.includes(account)) throw new Failure(400, "invalid_request");
      return id;
    }, 0, 64);
  }

  private async pending(account: string): Promise<Pending> {
    const access = `nc1.${account}.${random()}`;
    const refresh = `nr1.${account}.${random()}`;
    const [access_digest, refresh_digest] = await Promise.all([sha256hex(access), sha256hex(refresh)]);
    return { access, refresh, access_digest, refresh_digest };
  }

  private issue(grant: Grant, pending: Pending, now: number, accessTtl: number, refreshTtl: number): AuthorityResult {
    const limit = grant.expires_at ?? Number.MAX_SAFE_INTEGER;
    const accessExpires = Math.min(now + accessTtl, limit);
    const refreshExpires = Math.min(now + refreshTtl, limit);
    this.run("INSERT INTO tokens VALUES (?, 'access', ?, ?, NULL)", pending.access_digest, grant.grant_id, accessExpires);
    this.run("INSERT INTO tokens VALUES (?, 'refresh', ?, ?, NULL)", pending.refresh_digest, grant.grant_id, refreshExpires);
    return {
      ok: true,
      grant: safeGrant(grant),
      access_token: pending.access,
      refresh_token: pending.refresh,
      token_type: "Bearer",
      expires_in: Math.max(1, Math.floor((accessExpires - now) / 1000)),
      refresh_expires_in: Math.max(1, Math.floor((refreshExpires - now) / 1000)),
      scope: (JSON.parse(grant.scopes) as string[]).join(" "),
    };
  }

  private insertGrant(grant: Grant): Grant {
    this.run("INSERT INTO grants VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", grant.grant_id, grant.app_id, grant.origin, grant.principal, grant.scopes, grant.connections, grant.catalog_digest, grant.client_id, grant.revision, grant.status, grant.created_at, grant.expires_at);
    return grant;
  }

  private sweep(now: number): void {
    this.run("DELETE FROM tokens WHERE expires_at <= ?", now);
    this.run("DELETE FROM codes WHERE expires_at <= ?", now);
  }

  private liveGrant(digest: string, now: number): Grant {
    const row = this.row<{ grant_id: string; expires_at: number }>("SELECT grant_id, expires_at FROM tokens WHERE digest = ? AND kind = 'access'", digest);
    if (!row || row.expires_at <= now) throw new Failure(401, "invalid_token");
    const grant = this.row<Grant>("SELECT * FROM grants WHERE grant_id = ?", row.grant_id);
    if (!grant || grant.status !== "active" || (grant.expires_at !== null && grant.expires_at <= now)) throw new Failure(401, "invalid_token");
    return grant;
  }

  private sessionGrant(authority: SessionAuthority): Grant {
    const grant = this.row<Grant>("SELECT * FROM grants WHERE grant_id = ?", authority.grant_id);
    const now = Date.now();
    if (!grant || grant.status !== "active" || (grant.expires_at !== null && grant.expires_at <= now) || grant.revision !== authority.grant_revision) throw new Failure(403, "grant_revoked");
    if (!this.row("SELECT 1 AS present FROM grant_sessions WHERE grant_id = ? AND session_id = ?", grant.grant_id, authority.session_id)) throw new Failure(403, "forbidden");
    return grant;
  }

  private revokeGrant(grantId: string, now: number): number | null {
    const grant = this.row<Grant>("SELECT * FROM grants WHERE grant_id = ?", grantId);
    if (!grant) return null;
    if (grant.status !== "active") return grant.revision;
    const revision = grant.revision + 1;
    this.run("UPDATE grants SET status = 'revoked', revision = ? WHERE grant_id = ?", revision, grantId);
    this.run("DELETE FROM tokens WHERE grant_id = ?", grantId);
    this.run("INSERT OR REPLACE INTO fences (grant_id, session_name, revision, attempts, next_at) SELECT grant_id, session_name, ?, 0, ? FROM grant_sessions WHERE grant_id = ?", revision, now, grantId);
    return revision;
  }

  private async afterRevoke(grantId: string | undefined): Promise<void> {
    if (grantId) await this.deliver(grantId);
  }

  private async deliver(grantId?: string): Promise<void> {
    const now = Date.now();
    const due = grantId ? this.rows<Fence>("SELECT * FROM fences WHERE grant_id = ?", grantId) : this.rows<Fence>("SELECT * FROM fences WHERE next_at <= ?", now);
    const binding = this.env.SESSION_FENCES;
    const base = setting(this.env, "RECOVERY_DELAY_MS");
    await Promise.all(due.map(async fence => {
      let acknowledged = false;
      if (binding && this.account) {
        try {
          const response = await binding.fetch(new Request("https://session-fences.internal/", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ account_id: this.account, grant_id: fence.grant_id, revision: fence.revision, session_name: fence.session_name }),
            signal: AbortSignal.timeout(10_000),
          }));
          acknowledged = response.ok;
          await response.body?.cancel();
        } catch {
          acknowledged = false;
        }
      }
      if (acknowledged) {
        this.counters.fence_deliveries++;
        this.run("DELETE FROM fences WHERE grant_id = ? AND session_name = ? AND revision = ?", fence.grant_id, fence.session_name, fence.revision);
      } else {
        this.counters.fence_failures++;
        const delay = Math.min(base * 2 ** Math.min(fence.attempts, 20), FENCE_BACKOFF_CAP);
        this.run("UPDATE fences SET attempts = attempts + 1, next_at = ? WHERE grant_id = ? AND session_name = ? AND revision = ?", Date.now() + delay, fence.grant_id, fence.session_name, fence.revision);
      }
    }));
    const next = this.row<{ next_at: number | null }>("SELECT MIN(next_at) AS next_at FROM fences")?.next_at ?? null;
    if (next === null) await this.ctx.storage.deleteAlarm();
    else await this.ctx.storage.setAlarm(Math.max(next, Date.now()));
  }

  private async health(account: string): Promise<AuthorityResult> {
    const current = mode(this.env);
    const model = account === modelAccount(this.env);
    let credential: { state: string; generation: number; expires_at: number | null } | null = null;
    if (model && current === "subscription") {
      const row = this.row<CredentialRow>("SELECT * FROM credentials WHERE name = 'model'");
      const seed = await sha256hex(`model:${this.env.OAUTH_JSON}`);
      credential = !row || row.seed !== seed ? { state: "pending_seed", generation: row?.generation ?? 0, expires_at: null } : { state: row.state, generation: row.generation, expires_at: row.expires_at };
    }
    let codex: { state: string; generation: number; expires_at: number | null } | null = null;
    if (model && this.env.CODEX_OAUTH_JSON) {
      const row = this.row<CredentialRow>("SELECT * FROM credentials WHERE name = ?", CODEX_CREDENTIAL);
      const seed = await sha256hex(`${CODEX_CREDENTIAL}:${this.env.CODEX_OAUTH_JSON}`);
      codex = !vaultConfigured(this.env) ? { state: "vault_key_required", generation: row?.generation ?? 0, expires_at: null } : !row || row.seed !== seed ? { state: "pending_seed", generation: row?.generation ?? 0, expires_at: null } : { state: row.state, generation: row.generation, expires_at: row.expires_at };
    }
    const count = (query: string, ...bindings: unknown[]) => this.row<{ count: number }>(query, ...bindings)?.count ?? 0;
    return {
      ok: true,
      account_id: account,
      mode: current,
      subscription: model && current === "subscription",
      model_account: model,
      endpoint_override: !!this.env.ANTHROPIC_ENDPOINT,
      vault_encryption: vaultConfigured(this.env),
      credential,
      codex,
      grants_active: count("SELECT COUNT(*) AS count FROM grants WHERE status = 'active'"),
      threads: count("SELECT COUNT(*) AS count FROM threads"),
      pending_fences: count("SELECT COUNT(*) AS count FROM fences"),
      authority: { ...this.counters },
    };
  }

  private registerClient(command: AuthorityCommand): AuthorityResult {
    const app_id = text(command.app_id, APP_ID);
    const origins = list(command.origins, exactOrigin, 1, 16);
    const redirect_uris = list(command.redirect_uris, redirectUri, 1, 16);
    return this.ctx.storage.transactionSync(() => {
      const existing = this.row<Client>("SELECT * FROM clients WHERE app_id = ?", app_id);
      const client_id = existing?.client_id ?? `ncl_${random(16)}`;
      const created_at = existing?.created_at ?? Date.now();
      this.run("INSERT OR REPLACE INTO clients VALUES (?, ?, ?, ?, ?)", client_id, app_id, JSON.stringify(origins), JSON.stringify(redirect_uris), created_at);
      return { ok: true, client: { client_id, app_id, origins, redirect_uris, created_at } };
    });
  }

  private async createGrant(account: string, command: AuthorityCommand): Promise<AuthorityResult> {
    const app_id = text(command.app_id, APP_ID);
    const origin = exactOrigin(command.origin);
    const owner = principal(command.principal);
    const scopes = list(command.scopes, item => text(item, SCOPE), 1, 64);
    const selected = this.connectionIds(account, command.connections);
    const catalog = optional(command.catalog_digest, DIGEST) ?? null;
    const ttl = command.ttl_ms;
    if (ttl !== undefined && ttl !== null && (!Number.isSafeInteger(ttl) || (ttl as number) <= 0)) throw new Failure(400, "invalid_request");
    const accessTtl = setting(this.env, "CONNECT_TOKEN_TTL_MS");
    const refreshTtl = setting(this.env, "CONNECT_REFRESH_TTL_MS");
    const pending = await this.pending(account);
    return this.ctx.storage.transactionSync(() => {
      const now = Date.now();
      this.sweep(now);
      const client = this.row<Client>("SELECT * FROM clients WHERE app_id = ?", app_id);
      if (client && !(JSON.parse(client.origins) as string[]).includes(origin)) throw new Failure(403, "origin_mismatch");
      const grant = this.insertGrant({
        grant_id: crypto.randomUUID(), app_id, origin, principal: owner, scopes: JSON.stringify(scopes), connections: JSON.stringify(selected),
        catalog_digest: catalog, client_id: client?.client_id ?? null, revision: 1, status: "active", created_at: now,
        expires_at: typeof ttl === "number" ? now + ttl : null,
      });
      return this.issue(grant, pending, now, accessTtl, refreshTtl);
    });
  }

  private async authorizeCode(account: string, command: AuthorityCommand): Promise<AuthorityResult> {
    const client_id = text(command.client_id, CLIENT_ID);
    const origin = exactOrigin(command.origin);
    const redirect_uri = redirectUri(command.redirect_uri);
    const owner = principal(command.principal);
    const scopes = list(command.scopes, item => text(item, SCOPE), 1, 64);
    const selected = this.connectionIds(account, command.connections);
    const challenge = text(command.code_challenge, CHALLENGE);
    if (command.code_challenge_method !== "S256") throw new Failure(400, "invalid_request");
    const ttl = setting(this.env, "CONNECT_CODE_TTL_MS");
    const code = `na1.${account}.${random()}`;
    const digest = await sha256hex(code);
    return this.ctx.storage.transactionSync(() => {
      const now = Date.now();
      const client = this.row<Client>("SELECT * FROM clients WHERE client_id = ?", client_id);
      if (!client) throw new Failure(400, "invalid_client");
      if (!(JSON.parse(client.origins) as string[]).includes(origin)) throw new Failure(403, "origin_mismatch");
      if (!(JSON.parse(client.redirect_uris) as string[]).includes(redirect_uri)) throw new Failure(400, "invalid_request");
      this.run("INSERT INTO codes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)", digest, client_id, origin, redirect_uri, owner, JSON.stringify(scopes), JSON.stringify(selected), challenge, now + ttl);
      return { ok: true, code, expires_in: Math.max(1, Math.floor(ttl / 1000)), client_id, redirect_uri };
    });
  }

  private async exchangeCode(account: string, command: AuthorityCommand): Promise<AuthorityResult> {
    const code = token("na1", account, command.code, 400, "invalid_grant");
    const verifier = text(command.code_verifier, VERIFIER);
    const client_id = text(command.client_id, CLIENT_ID);
    const redirect_uri = redirectUri(command.redirect_uri);
    const origin = exactOrigin(command.origin);
    const accessTtl = setting(this.env, "CONNECT_TOKEN_TTL_MS");
    const refreshTtl = setting(this.env, "CONNECT_REFRESH_TTL_MS");
    const [digest, challenge, pending] = await Promise.all([sha256hex(code), sha256(verifier), this.pending(account)]);
    const encoded = new TextEncoder().encode(base64url(challenge));
    const outcome = this.ctx.storage.transactionSync((): { result: AuthorityResult; revoked?: string } => {
      const now = Date.now();
      const row = this.row<Code>("SELECT * FROM codes WHERE digest = ?", digest);
      if (!row) return { result: { ok: false, status: 400, error: "invalid_grant" } };
      if (row.grant_id) {
        this.revokeGrant(row.grant_id, now);
        return { result: { ok: false, status: 400, error: "invalid_grant" }, revoked: row.grant_id };
      }
      const expected = new TextEncoder().encode(row.challenge);
      const matches = expected.byteLength === encoded.byteLength && crypto.subtle.timingSafeEqual(expected, encoded);
      if (row.expires_at <= now || row.client_id !== client_id || row.redirect_uri !== redirect_uri || row.origin !== origin || !matches) {
        this.run("DELETE FROM codes WHERE digest = ?", digest);
        return { result: { ok: false, status: 400, error: "invalid_grant" } };
      }
      const client = this.row<Client>("SELECT * FROM clients WHERE client_id = ?", client_id);
      if (!client) return { result: { ok: false, status: 400, error: "invalid_client" } };
      this.sweep(now);
      const grant = this.insertGrant({
        grant_id: crypto.randomUUID(), app_id: client.app_id, origin, principal: row.principal, scopes: row.scopes, connections: row.connections,
        catalog_digest: null, client_id, revision: 1, status: "active", created_at: now, expires_at: null,
      });
      this.run("UPDATE codes SET grant_id = ? WHERE digest = ?", grant.grant_id, digest);
      return { result: this.issue(grant, pending, now, accessTtl, refreshTtl) };
    });
    await this.afterRevoke(outcome.revoked);
    return outcome.result;
  }

  private async refresh(account: string, command: AuthorityCommand): Promise<AuthorityResult> {
    const refresh = token("nr1", account, command.refresh_token, 400, "invalid_grant");
    const client_id = optional(command.client_id, /^[\x21-\x7e]{1,128}$/);
    const origin = command.origin === undefined || command.origin === null ? undefined : exactOrigin(command.origin);
    const accessTtl = setting(this.env, "CONNECT_TOKEN_TTL_MS");
    const refreshTtl = setting(this.env, "CONNECT_REFRESH_TTL_MS");
    const [digest, pending] = await Promise.all([sha256hex(refresh), this.pending(account)]);
    const outcome = this.ctx.storage.transactionSync((): { result: AuthorityResult; revoked?: string } => {
      const now = Date.now();
      const invalid = { ok: false, status: 400, error: "invalid_grant" };
      const row = this.row<{ grant_id: string; expires_at: number; used_at: number | null }>("SELECT grant_id, expires_at, used_at FROM tokens WHERE digest = ? AND kind = 'refresh'", digest);
      if (!row) return { result: invalid };
      if (row.used_at !== null) {
        this.revokeGrant(row.grant_id, now);
        return { result: invalid, revoked: row.grant_id };
      }
      const grant = this.row<Grant>("SELECT * FROM grants WHERE grant_id = ?", row.grant_id);
      if (row.expires_at <= now || !grant || grant.status !== "active" || (grant.expires_at !== null && grant.expires_at <= now)) return { result: invalid };
      if (client_id !== undefined && client_id !== grant.client_id) return { result: { ok: false, status: 400, error: "invalid_client" } };
      if (origin !== undefined && origin !== grant.origin) return { result: { ok: false, status: 403, error: "origin_mismatch" } };
      this.run("UPDATE tokens SET used_at = ? WHERE digest = ?", now, digest);
      this.sweep(now);
      return { result: this.issue(grant, pending, now, accessTtl, refreshTtl) };
    });
    await this.afterRevoke(outcome.revoked);
    return outcome.result;
  }

  private async authorize(account: string, command: AuthorityCommand): Promise<AuthorityResult> {
    const bearer = token("nc1", account, command.token);
    const origin = exactOrigin(command.origin);
    const app = optional(command.app_id, APP_ID);
    const owner = command.principal === undefined || command.principal === null ? undefined : principal(command.principal);
    const scope = optional(command.required_scope, SCOPE);
    const catalog = optional(command.catalog_digest, DIGEST);
    const requested = optional(command.thread_id, /^[0-9A-Fa-f-]{36}$/)?.toLowerCase();
    if (requested !== undefined && !UUID.test(requested)) throw new Failure(400, "invalid_request");
    if (command.create_thread !== undefined && typeof command.create_thread !== "boolean") throw new Failure(400, "invalid_request");
    const create = command.create_thread === true;
    const subscription = mode(this.env) === "subscription" && account === modelAccount(this.env);
    const digest = await sha256hex(bearer);
    return this.ctx.storage.transactionSync(() => {
      const now = Date.now();
      const grant = this.liveGrant(digest, now);
      if (grant.origin !== origin) throw new Failure(403, "origin_mismatch");
      if (app !== undefined && app !== grant.app_id) throw new Failure(403, "forbidden");
      if (owner !== undefined && owner !== grant.principal) throw new Failure(403, "forbidden");
      if (scope !== undefined && !(JSON.parse(grant.scopes) as string[]).includes(scope)) throw new Failure(403, "insufficient_scope");
      if (catalog !== undefined && grant.catalog_digest !== null && catalog !== grant.catalog_digest) throw new Failure(403, "catalog_mismatch");
      if (requested === undefined && !create) return { ok: true, grant: safeGrant(grant) };
      let thread: Thread | undefined;
      let created = false;
      if (requested !== undefined) {
        thread = this.row<Thread>("SELECT * FROM threads WHERE thread_id = ? AND app_id = ? AND origin = ? AND principal = ?", requested, grant.app_id, grant.origin, grant.principal);
        if (!thread) throw new Failure(404, "not_found");
      } else {
        const thread_id = crypto.randomUUID();
        thread = { thread_id, app_id: grant.app_id, origin: grant.origin, principal: grant.principal, session_name: `connect/${account}/${thread_id}`, session_id: sessionUuid(), created_at: now };
        this.run("INSERT INTO threads VALUES (?, ?, ?, ?, ?, ?, ?)", thread.thread_id, thread.app_id, thread.origin, thread.principal, thread.session_name, thread.session_id, thread.created_at);
        created = true;
      }
      this.run("INSERT OR IGNORE INTO grant_sessions VALUES (?, ?, ?)", grant.grant_id, thread.session_name, sessionUuid());
      const membership = this.row<{ session_id: string }>("SELECT session_id FROM grant_sessions WHERE grant_id = ? AND session_name = ?", grant.grant_id, thread.session_name)!;
      const descriptor: SessionDescriptor = {
        account_id: account,
        session_name: thread.session_name,
        session_id: membership.session_id,
        grant_id: grant.grant_id,
        grant_revision: grant.revision,
        app_id: grant.app_id,
        origin: grant.origin,
        principal: grant.principal,
        connections: JSON.parse(grant.connections) as string[],
        thread_id: thread.thread_id,
        subscription,
      };
      if (grant.catalog_digest !== null) descriptor.catalog_digest = grant.catalog_digest;
      if (grant.expires_at !== null) descriptor.grant_expires_at = grant.expires_at;
      return { ok: true, grant: safeGrant(grant), descriptor, thread_id: thread.thread_id, created_thread: created };
    });
  }

  private async revoke(account: string, command: AuthorityCommand): Promise<AuthorityResult> {
    const binding = !!this.env.SESSION_FENCES;
    let grantId: string | undefined;
    if (command.grant_id !== undefined) {
      grantId = text(command.grant_id, UUID);
    } else if (typeof command.token === "string") {
      const match = TOKEN.exec(command.token);
      if (match && (match[1] === "nc1" || match[1] === "nr1") && match[2] === account) {
        grantId = this.row<{ grant_id: string }>("SELECT grant_id FROM tokens WHERE digest = ?", await sha256hex(command.token))?.grant_id;
      }
      if (!grantId) return { ok: true, status: 200, grant_id: null, revision: null, pending_fences: 0, fence_binding: binding };
    } else {
      throw new Failure(400, "invalid_request");
    }
    const id = grantId;
    const revision = this.ctx.storage.transactionSync(() => this.revokeGrant(id, Date.now()));
    if (revision === null) throw new Failure(404, "not_found");
    await this.afterRevoke(id);
    const pending = this.row<{ count: number }>("SELECT COUNT(*) AS count FROM fences WHERE grant_id = ?", id)?.count ?? 0;
    return { ok: true, status: pending ? 202 : 200, grant_id: id, revision, pending_fences: pending, fence_binding: binding };
  }

  private inspect(command: AuthorityCommand): AuthorityResult {
    const id = text(command.grant_id, UUID);
    const grant = this.row<Grant>("SELECT * FROM grants WHERE grant_id = ?", id);
    if (!grant) throw new Failure(404, "not_found");
    const sessions = this.row<{ count: number }>("SELECT COUNT(*) AS count FROM grant_sessions WHERE grant_id = ?", id)?.count ?? 0;
    const pending = this.row<{ count: number }>("SELECT COUNT(*) AS count FROM fences WHERE grant_id = ?", id)?.count ?? 0;
    return { ok: true, grant: safeGrant(grant), sessions, pending_fences: pending };
  }

  private async listConnections(account: string): Promise<AuthorityResult> {
    const result = [];
    for (const connection of connections(this.env).values()) {
      if (!connection.accounts.includes(account)) continue;
      let credential = "static";
      if (connection.credential.type === "oauth") {
        const name = `connection:${connection.id}`;
        const row = this.row<CredentialRow>("SELECT * FROM credentials WHERE name = ?", name);
        const seed = await sha256hex(`${name}:${JSON.stringify(connection.credential)}`);
        credential = !vaultConfigured(this.env) ? "vault_key_required" : !row || row.seed !== seed ? "pending_seed" : row.state;
      }
      result.push({ connection_id: connection.id, kind: connection.kind, methods: connection.methods, credential });
    }
    if (account === modelAccount(this.env)) {
      const methods = ["code_mode", "Bash", "Read", "Write", "Edit"];
      const available = executors(this.env);
      if (available.has(DEFAULT_EXECUTOR)) result.push({ connection_id: LEGACY_RUNNER, kind: "executor", executor_id: DEFAULT_EXECUTOR, methods, credential: "private" });
      for (const id of available.keys()) result.push({ connection_id: runnerConnection(id), kind: "executor", executor_id: id, methods, credential: "private" });
    }
    return { ok: true, connections: result };
  }

  private aad(name: string): string {
    return `${this.account}|${name}`;
  }

  private async load(name: string, source: string, parse: () => StoredToken, required: boolean): Promise<{ row: CredentialRow; token: StoredToken }> {
    const seed = await sha256hex(`${name}:${source}`);
    const existing = this.row<CredentialRow>("SELECT * FROM credentials WHERE name = ?", name);
    if (!existing || existing.seed !== seed) {
      const token = parse();
      const envelope = await seal(this.env, this.aad(name), token, required);
      const row: CredentialRow = { name, seed, envelope, generation: (existing?.generation ?? 0) + 1, state: "ready", forced: 0, expires_at: token.expires_at, updated_at: Date.now() };
      this.run("INSERT OR REPLACE INTO credentials VALUES (?, ?, ?, ?, ?, ?, ?, ?)", row.name, row.seed, row.envelope, row.generation, row.state, row.forced, row.expires_at, row.updated_at);
      await this.ctx.storage.sync();
      return { row, token };
    }
    const { value, plaintext } = await open<StoredToken>(this.env, this.aad(name), existing.envelope);
    if (plaintext && required) throw new Failure(503, "vault_key_required");
    if (plaintext && vaultConfigured(this.env)) {
      const envelope = await seal(this.env, this.aad(name), value, true);
      this.run("UPDATE credentials SET envelope = ? WHERE name = ? AND generation = ?", envelope, name, existing.generation);
      await this.ctx.storage.sync();
      existing.envelope = envelope;
    }
    return { row: existing, token: value };
  }

  private async subscription(rejected?: number): Promise<Access> {
    const margin = setting(this.env, "REFRESH_MARGIN_MS");
    let { row, token } = await this.load("model", this.env.OAUTH_JSON!, () => subscriptionSeed(this.env.OAUTH_JSON!), false);
    if (row.state !== "ready") throw new Failure(401, row.state === "exchanging" ? "refresh_uncertain" : row.state);
    const force = rejected !== undefined && rejected === row.generation;
    if (force && row.forced) throw new Failure(401, "credential_rejected");
    if (force || token.expires_at === null || token.expires_at - Date.now() <= margin) {
      const scopes = SUBSCRIPTION.scopes.filter(scope => scope !== "org:create_api_key");
      for (const scope of token.scopes) if ((scope === "user:projects:read" || scope === "user:projects:write") && !scopes.includes(scope)) scopes.push(scope);
      const current = token;
      ({ row, token } = await this.exchange("model", row, token, force, false, margin, scope => fetch(SUBSCRIPTION.token_url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ grant_type: "refresh_token", refresh_token: current.refresh, client_id: SUBSCRIPTION.client_id, scope }),
        redirect: "manual",
        signal: AbortSignal.timeout(30_000),
      }), scopes.join(" "), current.scopes.length ? current.scopes.join(" ") : undefined));
    }
    return { ok: true, generation: row.generation, expires_at: token.expires_at, headers: [["authorization", `Bearer ${token.access}`]] };
  }

  private async codex(rejected?: number): Promise<Access> {
    const margin = setting(this.env, "REFRESH_MARGIN_MS");
    const source = this.env.CODEX_OAUTH_JSON!;
    let { row, token } = await this.load(CODEX_CREDENTIAL, source, () => codexSeed(source), true);
    if (row.state !== "ready") throw new Failure(401, row.state === "exchanging" ? "refresh_uncertain" : row.state);
    if (typeof token.account_id !== "string" || !CODEX_ACCOUNT.test(token.account_id)) throw new Failure(500, "configuration_error");
    const force = rejected !== undefined && rejected === row.generation;
    if (force && row.forced) throw new Failure(401, "credential_rejected");
    if (force || token.expires_at === null || token.expires_at - Date.now() <= margin) {
      const current = token;
      ({ row, token } = await this.exchange(CODEX_CREDENTIAL, row, token, force, true, margin, () => fetch(CODEX.token_url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ client_id: CODEX.client_id, grant_type: "refresh_token", refresh_token: current.refresh }),
        redirect: "manual",
        signal: AbortSignal.timeout(30_000),
      }), undefined, undefined, codexPolicy(current, margin)));
    }
    const headers: [string, string][] = [["authorization", `Bearer ${token.access}`], ["chatgpt-account-id", token.account_id!]];
    if (token.fedramp) headers.push(["x-openai-fedramp", "true"]);
    return { ok: true, generation: row.generation, expires_at: token.expires_at, headers };
  }

  private async connectionAccess(connection: Connection, rejected?: number): Promise<{ generation: number; access: string; expires_at: number | null }> {
    const credential = connection.credential;
    if (credential.type !== "oauth") throw new Failure(500, "internal_error");
    const name = `connection:${connection.id}`;
    const margin = setting(this.env, "REFRESH_MARGIN_MS");
    let { row, token } = await this.load(name, JSON.stringify(credential), () => ({ access: credential.access, refresh: credential.refresh, expires_at: credential.expires_at, scopes: credential.scope ? credential.scope.split(/\s+/).filter(Boolean) : [], refresh_expires_at: null }), true);
    if (row.state !== "ready") throw new Failure(401, row.state === "exchanging" ? "refresh_uncertain" : row.state);
    const force = rejected !== undefined && rejected === row.generation;
    if (force && row.forced) throw new Failure(401, "credential_rejected");
    if (force || (token.expires_at !== null && token.expires_at - Date.now() <= margin)) {
      const current = token;
      ({ row, token } = await this.exchange(name, row, token, force, true, 0, scope => {
        const form = new URLSearchParams({ grant_type: "refresh_token", refresh_token: current.refresh, client_id: credential.client_id });
        if (credential.client_secret) form.set("client_secret", credential.client_secret);
        if (scope) form.set("scope", scope);
        return fetch(credential.token_url, { method: "POST", headers: { "content-type": "application/x-www-form-urlencoded", accept: "application/json" }, body: form, redirect: "manual", signal: AbortSignal.timeout(30_000) });
      }, credential.scope, undefined));
    }
    return { generation: row.generation, access: token.access, expires_at: token.expires_at };
  }

  private async exchange(name: string, row: CredentialRow, token: StoredToken, forced: boolean, required: boolean, minimumLifetime: number, send: (scope?: string) => Promise<Response>, scope: string | undefined, fallback: string | undefined, policy?: TokenPolicy): Promise<{ row: CredentialRow; token: StoredToken }> {
    const subscription = name === "model";
    if (token.refresh_expires_at !== null && Date.now() >= token.refresh_expires_at) {
      this.run("UPDATE credentials SET state = 'login_required', updated_at = ? WHERE name = ?", Date.now(), name);
      await this.ctx.storage.sync();
      throw new Failure(401, "login_required");
    }
    this.run("UPDATE credentials SET state = 'exchanging', updated_at = ? WHERE name = ?", Date.now(), name);
    await this.ctx.storage.sync();
    this.counters.refreshes++;
    const attempt = async (value?: string) => {
      const response = await send(value);
      return { status: response.status, body: (await readLimited(response, 65_536)) ?? "" };
    };
    let response: { status: number; body: string };
    try {
      response = await attempt(scope);
      if (response.status === 400 && fallback !== undefined && oauthError(response.body).error === "invalid_scope") response = await attempt(fallback);
    } catch {
      this.counters.refresh_failures++;
      throw new Failure(401, "refresh_uncertain");
    }
    if (response.status !== 200) {
      this.counters.refresh_failures++;
      const error = oauthError(response.body);
      let state: string | undefined;
      if (policy) state = policy.rejected(response.status, response.body);
      else if ([400, 401, 403].includes(response.status) && (error.error === "invalid_grant" || error.error === "access_denied") && error.error_description === "account_on_hold") state = "on_hold";
      else if ([400, 401].includes(response.status) && error.error === "invalid_grant") state = "login_required";
      if (state) {
        this.run("UPDATE credentials SET state = ?, updated_at = ? WHERE name = ?", state, Date.now(), name);
        await this.ctx.storage.sync();
        throw new Failure(401, state);
      }
      throw new Failure(401, "refresh_uncertain");
    }
    const body = oauthError(response.body) as Record<string, unknown>;
    const now = Date.now();
    if (policy) {
      const accepted = policy.accept(body, now);
      if (typeof accepted === "string") {
        this.counters.refresh_failures++;
        this.run("UPDATE credentials SET state = ?, updated_at = ? WHERE name = ?", accepted, Date.now(), name);
        await this.ctx.storage.sync();
        throw new Failure(401, accepted);
      }
      if (!accepted) {
        this.counters.refresh_failures++;
        throw new Failure(401, "refresh_uncertain");
      }
      return await this.commit(name, row, accepted, forced, required);
    }
    const scopes = typeof body.scope === "string" ? body.scope.split(/\s+/).filter(Boolean) : token.scopes;
    const expiresIn = body.expires_in;
    const validExpiry = expiresIn === undefined ? !subscription : Number.isSafeInteger(expiresIn) && (expiresIn as number) * 1000 > minimumLifetime && (expiresIn as number) > 0;
    const valid = typeof body.access_token === "string" && SECRET_PATTERN.test(body.access_token)
      && (body.refresh_token === undefined || (typeof body.refresh_token === "string" && SECRET_PATTERN.test(body.refresh_token)))
      && (body.token_type === undefined || (typeof body.token_type === "string" && body.token_type.toLowerCase() === "bearer"))
      && (body.refresh_token_expires_in === undefined || (Number.isSafeInteger(body.refresh_token_expires_in) && (body.refresh_token_expires_in as number) > 0))
      && validExpiry
      && (!subscription || (typeof body.scope === "string" && scopes.includes("user:profile") && scopes.includes("user:inference")));
    if (!valid) {
      this.counters.refresh_failures++;
      throw new Failure(401, "refresh_uncertain");
    }
    const next: StoredToken = {
      access: body.access_token as string,
      refresh: (body.refresh_token as string | undefined) ?? token.refresh,
      expires_at: expiresIn === undefined ? null : now + (expiresIn as number) * 1000,
      scopes,
      refresh_expires_at: body.refresh_token_expires_in !== undefined ? now + (body.refresh_token_expires_in as number) * 1000 : token.refresh_expires_at,
    };
    return await this.commit(name, row, next, forced, required);
  }

  private async commit(name: string, row: CredentialRow, next: StoredToken, forced: boolean, required: boolean): Promise<{ row: CredentialRow; token: StoredToken }> {
    const envelope = await seal(this.env, this.aad(name), next, required);
    const updated: CredentialRow = { ...row, envelope, generation: row.generation + 1, state: "ready", forced: forced ? 1 : 0, expires_at: next.expires_at, updated_at: Date.now() };
    this.run("UPDATE credentials SET envelope = ?, generation = ?, state = 'ready', forced = ?, expires_at = ?, updated_at = ? WHERE name = ?", updated.envelope, updated.generation, updated.forced, updated.expires_at, updated.updated_at, name);
    await this.ctx.storage.sync();
    return { row: updated, token: next };
  }
}

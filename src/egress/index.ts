import { WorkerEntrypoint } from "cloudflare:workers";
import { json, type AuthorityCommand, type AuthorityResult, type ModelDispatch, type SessionAuthority } from "../contracts";
import { AccountAuthority, type Access, type ServiceAccess, type ServiceRequest } from "./account";
import { ACCOUNT_PATTERN, connections, encoder, Failure, FORBIDDEN_HEADERS, mode, modelEndpoint, readLimited, setting, SUBSCRIPTION, type Connection, type EgressEnv } from "./common";

export { AccountAuthority };
export { RemoteExecutorEgress } from "../remote/egress";
export type { EgressEnv };

const PUBLIC_OPS = new Set(["health", "register_client", "create_grant", "authorize_code", "exchange_code", "refresh", "authorize", "revoke", "inspect", "list_connections"]);
const MODEL_REQUEST_HEADER = /^(accept|content-type|anthropic-version|anthropic-beta|anthropic-dangerous-direct-browser-access|user-agent|x-app|x-claude-code-session-id|x-stainless-[a-z0-9-]{1,64})$/;
const MODEL_RESPONSE_HEADER = /^(content-type|request-id|retry-after|x-should-retry|anthropic-ratelimit-[a-z0-9-]{1,64})$/;
const PROVIDER_ERRORS = new Set(["invalid_request_error", "authentication_error", "permission_error", "not_found_error", "request_too_large", "rate_limit_error", "api_error", "overloaded_error", "timeout_error", "billing_error"]);
const CACHE_LIMIT = 1024;
const AUTHORITY_BODY_LIMIT = 256 * 1024;
const SERVICE_BODY_LIMIT = 33 << 20;

type Granted = Extract<Access, { ok: true }>;
type Entry = { access: Granted; until: number };

const metrics = {
  model_requests: 0,
  model_cache_hits: 0,
  model_cache_misses: 0,
  model_coalesced: 0,
  model_authority_rpcs: 0,
  model_unauthorized: 0,
  model_recoveries: 0,
  model_provider_errors: 0,
  service_requests: 0,
  service_authority_rpcs: 0,
  service_denied: 0,
  service_recoveries: 0,
  service_redacted: 0,
  redirects_rejected: 0,
  upstream_failures: 0,
};
const cache = new Map<string, Entry>();
const inflight = new Map<string, Promise<Access>>();

function snapshot() {
  return { ...metrics, cache_entries: cache.size, inflight: inflight.size };
}

function account(env: EgressEnv, id: string) {
  return env.ACCOUNTS.get(env.ACCOUNTS.idFromName(id));
}

function denial(error: unknown): { status: number; error: string } {
  if (error instanceof Failure) return { status: error.status, error: error.code };
  metrics.upstream_failures++;
  return { status: 502, error: "upstream_unavailable" };
}

function providerError(status: number, type: string, message: string, retryAfter?: string | null): Response {
  const headers = new Headers({ "content-type": "application/json", "cache-control": "no-store" });
  if (retryAfter && /^[\x20-\x7e]{1,64}$/.test(retryAfter)) headers.set("retry-after", retryAfter);
  return new Response(JSON.stringify({ type: "error", error: { type, message } }), { status, headers });
}

function egressError(failure: { status: number; error: string }): Response {
  const type = failure.status === 401 ? "authentication_error" : failure.status === 403 ? "permission_error" : failure.status === 400 || failure.status === 409 ? "invalid_request_error" : "api_error";
  return providerError(failure.status, type, failure.error);
}

function authorityOf(value: any): SessionAuthority {
  if (!value || typeof value !== "object" || typeof value.account_id !== "string" || !ACCOUNT_PATTERN.test(value.account_id) || typeof value.session_id !== "string" || !value.session_id || value.session_id.length > 128) throw new Failure(400, "invalid_request");
  const authority: SessionAuthority = { account_id: value.account_id, session_id: value.session_id };
  if (value.grant_id !== undefined && value.grant_id !== null) {
    if (typeof value.grant_id !== "string" || value.grant_id.length > 64 || !Number.isSafeInteger(value.grant_revision)) throw new Failure(400, "invalid_request");
    authority.grant_id = value.grant_id;
    authority.grant_revision = value.grant_revision;
    if (value.grant_expires_at !== undefined) {
      if (!Number.isSafeInteger(value.grant_expires_at)) throw new Failure(400, "invalid_request");
      authority.grant_expires_at = value.grant_expires_at;
      if (value.grant_expires_at <= Date.now()) throw new Failure(403, "grant_expired");
    }
  }
  return authority;
}

function cacheKey(authority: SessionAuthority): string {
  return [authority.account_id, authority.grant_id ?? "", authority.grant_revision ?? "", authority.grant_id ? authority.session_id : ""].join("\u0000");
}

async function resolveModel(env: EgressEnv, authority: SessionAuthority, rejected?: number): Promise<Access> {
  const key = cacheKey(authority);
  const entry = cache.get(key);
  if (rejected === undefined) {
    if (entry && entry.until > Date.now()) {
      metrics.model_cache_hits++;
      return entry.access;
    }
  }
  if (entry && (rejected === undefined || entry.access.generation === rejected)) cache.delete(key);
  const flight = rejected === undefined ? key : `${key}\u0001${rejected}`;
  const existing = inflight.get(flight);
  if (existing) {
    metrics.model_coalesced++;
    return existing;
  }
  if (rejected === undefined) metrics.model_cache_misses++;
  const ttl = setting(env, "CREDENTIAL_CACHE_TTL_MS");
  const margin = setting(env, "REFRESH_MARGIN_MS");
  const pending = (async (): Promise<Access> => {
    metrics.model_authority_rpcs++;
    const access = await account(env, authority.account_id).modelAccess({ account_id: authority.account_id, authority, rejected_generation: rejected }) as Access;
    if (access.ok) {
      const now = Date.now();
      const until = Math.min(now + ttl, access.expires_at === null ? Number.MAX_SAFE_INTEGER : access.expires_at - margin, authority.grant_expires_at ?? Number.MAX_SAFE_INTEGER);
      if (until > now) {
        cache.delete(key);
        cache.set(key, { access, until });
        while (cache.size > CACHE_LIMIT) cache.delete(cache.keys().next().value!);
      }
    }
    return access;
  })();
  inflight.set(flight, pending);
  try {
    return await pending;
  } finally {
    inflight.delete(flight);
  }
}

function modelHeaders(input: unknown, subscription: boolean): Headers {
  if (!Array.isArray(input) || input.length > 128) throw new Failure(400, "invalid_request");
  const headers = new Headers();
  for (const pair of input) {
    if (!Array.isArray(pair) || pair.length !== 2 || typeof pair[0] !== "string" || typeof pair[1] !== "string") throw new Failure(400, "invalid_request");
    const name = pair[0].toLowerCase();
    if (!MODEL_REQUEST_HEADER.test(name)) continue;
    try { headers.append(name, pair[1]); } catch { throw new Failure(400, "invalid_request"); }
  }
  const betas = (headers.get("anthropic-beta") ?? "").split(",").map(beta => beta.trim()).filter(beta => beta && (subscription || beta !== SUBSCRIPTION.beta));
  if (subscription && !betas.includes(SUBSCRIPTION.beta)) betas.push(SUBSCRIPTION.beta);
  const unique = [...new Set(betas)];
  if (unique.length) headers.set("anthropic-beta", unique.join(","));
  else headers.delete("anthropic-beta");
  headers.set("content-type", "application/json");
  if (!headers.has("anthropic-version")) headers.set("anthropic-version", "2023-06-01");
  return headers;
}

function sendModel(endpoint: string, headers: Headers, access: Granted, body: string, signal: AbortSignal): Promise<Response> {
  const outgoing = new Headers(headers);
  for (const [name, value] of access.headers) outgoing.set(name, value);
  return fetch(endpoint, { method: "POST", headers: outgoing, body, redirect: "manual", signal });
}

async function projectModel(response: Response): Promise<Response> {
  if (response.status >= 300 && response.status < 400) {
    metrics.redirects_rejected++;
    await response.body?.cancel();
    return providerError(502, "api_error", "Provider redirect rejected");
  }
  if (response.ok) {
    const headers = new Headers({ "cache-control": "no-store" });
    for (const [name, value] of response.headers) if (MODEL_RESPONSE_HEADER.test(name)) headers.set(name, value);
    return new Response(response.body, { status: response.status, headers });
  }
  metrics.model_provider_errors++;
  const text = await readLimited(response, 65_536);
  let type = "api_error";
  try {
    const parsed = JSON.parse(text ?? "");
    if (typeof parsed?.error?.type === "string" && PROVIDER_ERRORS.has(parsed.error.type)) type = parsed.error.type;
  } catch {}
  return providerError(response.status, type, "Provider request failed", response.headers.get("retry-after"));
}

export class SessionEgress extends WorkerEntrypoint<EgressEnv> {
  async fetch(request: Request): Promise<Response> {
    if (request.method === "GET") return json({ ok: true, metrics: snapshot() });
    if (request.method !== "POST") return json({ error: "method_not_allowed" }, 405);
    const path = new URL(request.url).pathname;
    if (path === "/connections" || path === "/connector") {
      try {
        const input = await readService(request);
        const authority = authorityOf(input.authority);
        const target = account(this.env, authority.account_id);
        if (path === "/connections") {
          const result = await target.sessionCatalog({ account_id: authority.account_id, authority }) as AuthorityResult;
          return json(result, result.status ?? (result.ok ? 200 : 400));
        }
        metrics.service_requests++;
        return await dispatchConnector(this.env, request, input, rejected_generation => target.sessionServiceAccess({ account_id: authority.account_id, authority, connection_id: input.connection_id, kind: input.kind ?? "connector", rejected_generation }) as Promise<ServiceAccess>);
      } catch (error) {
        const failure = denial(error);
        return json({ ok: false, error: failure.error }, failure.status);
      }
    }
    if (path !== "/model") return json({ error: "not_found" }, 404);
    metrics.model_requests++;
    try {
      let dispatch: ModelDispatch;
      try { dispatch = await request.json<ModelDispatch>(); } catch { throw new Failure(400, "invalid_request"); }
      const authority = authorityOf(dispatch?.authority);
      const current = mode(this.env);
      if (current === "unconfigured") throw new Failure(500, "configuration_error");
      const subscription = current === "subscription";
      if (dispatch.subscription !== subscription) throw new Failure(409, "subscription_mismatch");
      if (typeof dispatch.body !== "string") throw new Failure(400, "invalid_request");
      const endpoint = modelEndpoint(this.env);
      const headers = modelHeaders(dispatch.headers, subscription);
      const first = await resolveModel(this.env, authority);
      if (!first.ok) return egressError(first);
      let response = await sendModel(endpoint, headers, first, dispatch.body, request.signal);
      if (response.status === 401) {
        metrics.model_unauthorized++;
        await response.body?.cancel();
        const next = await resolveModel(this.env, authority, first.generation);
        if (!next.ok) return egressError(next);
        if (next.generation === first.generation) return providerError(401, "authentication_error", "Provider request failed");
        metrics.model_recoveries++;
        response = await sendModel(endpoint, headers, next, dispatch.body, request.signal);
        if (response.status === 401) {
          const entry = cache.get(cacheKey(authority));
          if (entry?.access.generation === next.generation) cache.delete(cacheKey(authority));
        }
      }
      return await projectModel(response);
    } catch (error) {
      return egressError(denial(error));
    }
  }
}

export class ConnectAuthority extends WorkerEntrypoint<EgressEnv> {
  async fetch(request: Request): Promise<Response> {
    if (request.method !== "POST") return json({ ok: false, status: 405, error: "method_not_allowed" }, 405);
    let command: AuthorityCommand;
    try {
      const text = await request.text();
      if (text.length > AUTHORITY_BODY_LIMIT) throw new Error();
      command = JSON.parse(text);
    } catch {
      return json({ ok: false, status: 400, error: "invalid_request" }, 400);
    }
    if (!command || typeof command !== "object" || typeof command.op !== "string" || !PUBLIC_OPS.has(command.op) || typeof command.account_id !== "string" || !ACCOUNT_PATTERN.test(command.account_id)) {
      return json({ ok: false, status: 400, error: "invalid_request" }, 400);
    }
    let result: AuthorityResult;
    try {
      result = await account(this.env, command.account_id).command(command) as AuthorityResult;
    } catch {
      result = { ok: false, status: 503, error: "authority_unavailable" };
    }
    if (command.op === "health" && result.ok) result.metrics = snapshot();
    return json(result, result.status ?? (result.ok ? 200 : 400));
  }
}

function connectorUrl(connection: Connection, path: string): URL {
  if (!path.startsWith("/") || path.startsWith("//") || path.length > 4096 || /[\\#\u0000- \u007f]/.test(path)) throw new Failure(400, "invalid_request");
  const index = path.indexOf("?");
  const pathname = index < 0 ? path : path.slice(0, index);
  const query = index < 0 ? "" : path.slice(index);
  if (/%(2e|2f|5c|00)/i.test(pathname) || pathname.split("/").some(segment => segment === "." || segment === "..")) throw new Failure(400, "invalid_request");
  if (!connection.paths.some(prefix => pathname.startsWith(prefix))) throw new Failure(403, "forbidden");
  const basePath = connection.base.pathname.replace(/\/+$/, "");
  const url = new URL(`${connection.base.origin}${basePath}${pathname}${query}`);
  if (url.origin !== connection.base.origin || !(url.pathname === basePath || url.pathname.startsWith(`${basePath}/`))) throw new Failure(400, "invalid_request");
  return url;
}

function connectorHeaders(connection: Connection, input: unknown): Headers {
  const headers = new Headers();
  if (input === undefined || input === null) return headers;
  const entries = Array.isArray(input) ? input : typeof input === "object" ? Object.entries(input as Record<string, unknown>) : null;
  if (!entries || entries.length > 64) throw new Failure(400, "invalid_request");
  const credentialHeader = connection.auth.type === "header" ? connection.auth.name : "authorization";
  for (const pair of entries) {
    if (!Array.isArray(pair) || pair.length !== 2 || typeof pair[0] !== "string" || typeof pair[1] !== "string") throw new Failure(400, "invalid_request");
    const name = pair[0].toLowerCase();
    if (FORBIDDEN_HEADERS.has(name) || name === credentialHeader || !connection.request_headers.includes(name)) continue;
    try { headers.set(name, pair[1]); } catch { throw new Failure(400, "invalid_request"); }
  }
  return headers;
}

function sendConnector(url: URL, method: string, headers: Headers, access: Extract<ServiceAccess, { ok: true }>, body: string | undefined, signal: AbortSignal): Promise<Response> {
  const outgoing = new Headers(headers);
  for (const [name, value] of access.headers) outgoing.set(name, value);
  return fetch(url, { method, headers: outgoing, body, redirect: "manual", signal });
}

function secretsOf(access: Extract<ServiceAccess, { ok: true }>): string[] {
  const values = new Set(access.secrets);
  for (const [, value] of access.headers) {
    values.add(value);
    const space = value.indexOf(" ");
    if (space > 0) values.add(value.slice(space + 1));
  }
  return [...values].filter(value => value.length >= 8);
}

function pick(value: unknown, fields: string[]): unknown {
  const select = (item: unknown) => {
    if (!item || typeof item !== "object" || Array.isArray(item)) return undefined;
    const record = item as Record<string, unknown>;
    return Object.fromEntries(fields.filter(field => Object.hasOwn(record, field)).map(field => [field, record[field]]));
  };
  return Array.isArray(value) ? value.slice(0, 1000).map(select) : select(value);
}

async function projectConnector(connection: Connection, response: Response, secrets: string[]): Promise<Response> {
  const result: Record<string, unknown> = { ok: true, status: response.status };
  const headers: Record<string, string> = {};
  for (const name of connection.response_headers) {
    const value = response.headers.get(name);
    if (value !== null) headers[name] = value;
  }
  if (Object.keys(headers).length) result.headers = headers;
  const type = response.headers.get("content-type") ?? "";
  if (connection.response_fields.length && /^application\/([a-z0-9.+-]*\+)?json\s*(;|$)/i.test(type)) {
    const text = await readLimited(response, connection.max_response_bytes);
    if (text === null) result.truncated = true;
    else {
      try { result.data = pick(JSON.parse(text), connection.response_fields); } catch { result.invalid_json = true; }
    }
  } else {
    await response.body?.cancel();
  }
  const serialized = JSON.stringify(result);
  if (secrets.some(secret => serialized.includes(secret))) {
    metrics.service_redacted++;
    return json({ ok: true, status: response.status, redacted: true });
  }
  return new Response(serialized, { status: 200, headers: { "content-type": "application/json", "cache-control": "no-store" } });
}

async function readService(request: Request): Promise<any> {
  try {
    const text = await request.text();
    if (text.length > SERVICE_BODY_LIMIT) throw new Error();
    const input = JSON.parse(text);
    if (!input || typeof input !== "object") throw new Error();
    return input;
  } catch { throw new Failure(400, "invalid_request"); }
}

async function dispatchConnector(env: EgressEnv, request: Request, input: any, resolve: (rejected?: number) => Promise<ServiceAccess>): Promise<Response> {
  const method = input.method === undefined ? "GET" : typeof input.method === "string" ? input.method.toUpperCase() : "";
  const path = input.path ?? "/";
  const kind = input.kind ?? "connector";
  if (typeof input.connection_id !== "string" || typeof path !== "string" || (kind !== "connector" && kind !== "vault") || (input.body !== undefined && typeof input.body !== "string")) throw new Failure(400, "invalid_request");
  const connection = connections(env).get(input.connection_id);
  if (!connection) throw new Failure(404, "not_found");
  if (!connection.methods.includes(method)) throw new Failure(403, "forbidden");
  const url = connectorUrl(connection, path);
  const headers = connectorHeaders(connection, input.headers);
  const body = input.body as string | undefined;
  if (body !== undefined && (method === "GET" || method === "HEAD")) throw new Failure(400, "invalid_request");
  if (body !== undefined && encoder.encode(body).byteLength > connection.max_body_bytes) throw new Failure(413, "body_too_large");
  metrics.service_authority_rpcs++;
  let access = await resolve();
  if (!access.ok) {
    metrics.service_denied++;
    return json({ ok: false, error: access.error }, access.status);
  }
  const vault = kind === "vault" || connection.kind === "vault";
  let response = await sendConnector(url, method, headers, access, body, request.signal);
  if (response.status === 401 && access.refreshable) {
    await response.body?.cancel();
    metrics.service_authority_rpcs++;
    const next = await resolve(access.generation);
    if (!next.ok) {
      metrics.service_denied++;
      return json({ ok: false, error: next.error }, next.status);
    }
    if (next.generation !== access.generation) {
      metrics.service_recoveries++;
      access = next;
      response = await sendConnector(url, method, headers, access, body, request.signal);
    } else return json({ ok: false, status: 401 });
  }
  if (response.status >= 300 && response.status < 400) {
    metrics.redirects_rejected++;
    await response.body?.cancel();
    return json({ ok: false, error: "upstream_redirect" }, 502);
  }
  if (vault || !response.ok) {
    await response.body?.cancel();
    return json({ ok: response.ok, status: response.status });
  }
  return projectConnector(connection, response, secretsOf(access));
}

export class ConnectServiceEgress extends WorkerEntrypoint<EgressEnv> {
  async fetch(request: Request): Promise<Response> {
    if (request.method !== "POST") return json({ ok: false, error: "method_not_allowed" }, 405);
    metrics.service_requests++;
    try {
      const input = await readService(request);
      if (typeof input.account_id !== "string" || !ACCOUNT_PATTERN.test(input.account_id) || typeof input.token !== "string" || input.token.length > 512 || typeof input.origin !== "string" || (input.app_id !== undefined && typeof input.app_id !== "string")) throw new Failure(400, "invalid_request");
      const authority = account(this.env, input.account_id);
      const command: ServiceRequest = { account_id: input.account_id, token: input.token, origin: input.origin, app_id: input.app_id, connection_id: input.connection_id, kind: input.kind ?? "connector" };
      return await dispatchConnector(this.env, request, input, rejected_generation => authority.serviceAccess({ ...command, rejected_generation }) as Promise<ServiceAccess>);
    } catch (error) {
      const failure = denial(error);
      if (failure.status < 500) metrics.service_denied++;
      return json({ ok: false, error: failure.error }, failure.status);
    }
  }
}

export default {
  fetch(): Response {
    return json({ error: "not_found" }, 404);
  },
} satisfies ExportedHandler<EgressEnv>;

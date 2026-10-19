import { DEFAULT_EXECUTOR, EXECUTOR_ID, LEGACY_RUNNER, runnerConnection, type AuthorityResult, type ModelProvider, type PrivateBinding } from "../contracts";
import type { CredentialBroker } from "../vault";
import type { AccountAuthority } from "./account";

export interface EgressEnv {
  ACCOUNTS: DurableObjectNamespace<AccountAuthority>;
  VAULT: Service<CredentialBroker>;
  ANTHROPIC_ENDPOINT?: string;
  ACCOUNT_ID?: string;
  CREDENTIAL_CACHE_TTL_MS: string;
  REFRESH_MARGIN_MS: string;
  CONNECT_TOKEN_TTL_MS: string;
  CONNECT_REFRESH_TTL_MS: string;
  CONNECT_CODE_TTL_MS: string;
  RECOVERY_DELAY_MS: string;
  MODEL_RESPONSE_HEADERS_TIMEOUT_MS: string;
  MODEL_STREAM_IDLE_TIMEOUT_MS: string;
  CONNECTIONS_JSON?: string;
  SESSION_FENCES?: PrivateBinding;
  RUNNER_URL?: string;
  RUNNERS_JSON?: string;
  CODEX_TRANSPORT?: PrivateBinding;
  [binding: `RUNNER_VPC_${string}`]: unknown;
}

export const SUBSCRIPTION = { beta: "oauth-2025-04-20" };
export const CODEX = { endpoint: "https://chatgpt.com/backend-api/codex/responses" };

export const ACCOUNT_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;
export const encoder = new TextEncoder();
const decoder = new TextDecoder();

export class Failure extends Error {
  constructor(readonly status: number, readonly code: string) {
    super(code);
  }

  result(): AuthorityResult {
    return { ok: false, status: this.status, error: this.code };
  }
}

export function setting(env: EgressEnv, name: "CREDENTIAL_CACHE_TTL_MS" | "REFRESH_MARGIN_MS" | "CONNECT_TOKEN_TTL_MS" | "CONNECT_REFRESH_TTL_MS" | "CONNECT_CODE_TTL_MS" | "RECOVERY_DELAY_MS" | "MODEL_RESPONSE_HEADERS_TIMEOUT_MS" | "MODEL_STREAM_IDLE_TIMEOUT_MS"): number {
  const value = Number(env[name]);
  if (!Number.isSafeInteger(value) || value <= 0) throw new Failure(500, "configuration_error");
  return value;
}

export function modelAccount(env: Pick<EgressEnv, "ACCOUNT_ID">): string {
  const account = env.ACCOUNT_ID ?? "default";
  if (!ACCOUNT_PATTERN.test(account)) throw new Failure(500, "configuration_error");
  return account;
}

export function modelProvider(value: unknown): ModelProvider {
  if (value === undefined || value === null || value === "claude") return "claude";
  if (value === "codex") return "codex";
  throw new Failure(400, "unsupported_provider");
}

export function modelEndpoint(env: EgressEnv, subscription: boolean): string {
  const value = env.ANTHROPIC_ENDPOINT ?? (subscription ? "https://api.anthropic.com/v1/messages?beta=true" : "https://api.anthropic.com/v1/messages");
  let url: URL;
  try { url = new URL(value); } catch { throw new Failure(500, "configuration_error"); }
  if (!secureUrl(url) || url.hash) throw new Failure(500, "configuration_error");
  return url.toString();
}

export function secureUrl(url: URL): boolean {
  return !url.username && !url.password && (url.protocol === "https:" || (url.protocol === "http:" && loopback(url.hostname)));
}

function loopback(host: string): boolean {
  return host === "localhost" || host === "127.0.0.1" || host === "[::1]";
}

export function exactOrigin(value: unknown): string {
  if (typeof value !== "string" || value.length > 512) throw new Failure(400, "invalid_request");
  let url: URL;
  try { url = new URL(value); } catch { throw new Failure(400, "invalid_request"); }
  if (url.origin !== value || !secureUrl(url)) throw new Failure(400, "invalid_request");
  return value;
}

export function base64url(bytes: Uint8Array): string {
  let text = "";
  for (const byte of bytes) text += String.fromCharCode(byte);
  return btoa(text).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

export function unbase64url(value: string): Uint8Array {
  const text = atob(value.replace(/-/g, "+").replace(/_/g, "/") + "=".repeat((4 - value.length % 4) % 4));
  return Uint8Array.from(text, character => character.charCodeAt(0));
}

export function random(bytes = 32): string {
  return base64url(crypto.getRandomValues(new Uint8Array(bytes)));
}

export async function sha256(value: string): Promise<Uint8Array> {
  return new Uint8Array(await crypto.subtle.digest("SHA-256", encoder.encode(value)));
}

export async function sha256hex(value: string): Promise<string> {
  return Array.from(await sha256(value), byte => byte.toString(16).padStart(2, "0")).join("");
}

export type Placement = { type: "bearer" } | { type: "header"; name: string; prefix: string } | { type: "basic" };

export type Connection = {
  id: string;
  kind: "connector" | "vault";
  base: URL;
  accounts: string[];
  methods: string[];
  paths: string[];
  request_headers: string[];
  response_headers: string[];
  response_fields: string[];
  max_body_bytes: number;
  max_response_bytes: number;
  auth: Placement;
};

export const FORBIDDEN_HEADERS = new Set(["authorization", "proxy-authorization", "cookie", "set-cookie", "host", "x-api-key", "content-length", "transfer-encoding", "connection", "keep-alive", "upgrade", "te", "trailer", "forwarded", "x-forwarded-for", "x-forwarded-host", "x-real-ip", "cf-connecting-ip", "accept-encoding"]);
const HEADER_NAME = /^[!#$%&'*+.^_`|~0-9a-z-]{1,128}$/;
const METHODS = new Set(["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"]);
const IDENTIFIER = /^[A-Za-z0-9_-]{1,64}$/;

let parsedSource: string | undefined;
let parsedConnections = new Map<string, Connection>();

export function connections(env: EgressEnv): Map<string, Connection> {
  const source = env.CONNECTIONS_JSON ?? "";
  if (source === parsedSource) return parsedConnections;
  const result = new Map<string, Connection>();
  if (source) {
    let document: any;
    try { document = JSON.parse(source); } catch { throw new Failure(500, "configuration_error"); }
    if (!document || typeof document !== "object" || Array.isArray(document)) throw new Failure(500, "configuration_error");
    for (const [id, value] of Object.entries<any>(document)) result.set(id, connection(env, id, value));
  }
  parsedSource = source;
  parsedConnections = result;
  return result;
}

function connection(env: EgressEnv, id: string, value: any): Connection {
  const fail = () => { throw new Failure(500, "configuration_error"); };
  if (!IDENTIFIER.test(id) || !value || typeof value !== "object" || "credential" in value) fail();
  const kind = value.kind ?? "connector";
  if (kind !== "connector" && kind !== "vault") fail();
  let base: URL;
  try { base = new URL(value.base_url); } catch { return fail(); }
  if (!secureUrl(base) || base.search || base.hash) fail();
  const strings = (input: unknown, fallback: string[], test: (item: string) => boolean) => {
    if (input === undefined) return fallback;
    if (!Array.isArray(input) || input.length > 64 || !input.every(item => typeof item === "string" && test(item))) fail();
    return input as string[];
  };
  const accounts = strings(value.accounts, [modelAccount(env)], item => ACCOUNT_PATTERN.test(item));
  const methods = strings(value.methods, ["GET"], item => METHODS.has(item));
  const paths = strings(value.paths, ["/"], item => item.startsWith("/") && !/[?#\\\s]/.test(item));
  const auth = placement(value.auth, fail);
  const request_headers = strings(value.request_headers, ["accept", "content-type"], item => HEADER_NAME.test(item) && !FORBIDDEN_HEADERS.has(item) && (auth.type !== "header" || item !== auth.name));
  const response_headers = strings(value.response_headers, [], item => HEADER_NAME.test(item) && !FORBIDDEN_HEADERS.has(item) && !/auth|token|secret|cookie|key/.test(item));
  const response_fields = strings(value.response_fields, [], item => /^[A-Za-z0-9_.:-]{1,128}$/.test(item));
  const limit = (input: unknown, fallback: number) => {
    if (input === undefined) return fallback;
    if (!Number.isSafeInteger(input) || (input as number) <= 0 || (input as number) > 32 << 20) fail();
    return input as number;
  };
  return {
    id,
    kind,
    base,
    accounts,
    methods,
    paths,
    request_headers,
    response_headers,
    response_fields,
    max_body_bytes: limit(value.max_body_bytes, 1 << 20),
    max_response_bytes: limit(value.max_response_bytes, 4 << 20),
    auth,
  };
}

function placement(value: any, fail: () => never): Placement {
  if (value === undefined || value?.type === "bearer") return { type: "bearer" };
  if (value?.type === "basic") return { type: "basic" };
  if (value?.type === "header" && typeof value.name === "string" && HEADER_NAME.test(value.name) && !FORBIDDEN_HEADERS.has(value.name)) {
    const prefix = value.prefix ?? "";
    if (typeof prefix !== "string" || !/^[\x20-\x7e]{0,64}$/.test(prefix)) fail();
    return { type: "header", name: value.name, prefix };
  }
  return fail();
}

export function placeCredential(auth: Placement, secret: string): [string, string] {
  if (auth.type === "bearer") return ["authorization", `Bearer ${secret}`];
  if (auth.type === "basic") return ["authorization", `Basic ${btoa(secret)}`];
  return [auth.name, `${auth.prefix}${secret}`];
}

export type Executor = { id: string; url: string; binding?: string };
export type ExecutorTransport = { fetch(request: Request): Promise<Response> };
type ExecutorEnv = Pick<EgressEnv, "RUNNERS_JSON" | "RUNNER_URL"> & { [binding: `RUNNER_VPC_${string}`]: unknown };

const EXECUTOR_SCHEMES: Record<string, string> = { "wss:": "https:", "https:": "https:", "ws:": "http:", "http:": "http:" };
const EXECUTOR_LIMIT = 64;
const EXECUTOR_BINDING = /^RUNNER_VPC_[A-Z0-9_]+$/;

let executorSource: string | undefined;
let parsedExecutors = new Map<string, Executor>();

export function executors(env: ExecutorEnv): Map<string, Executor> {
  const source = JSON.stringify([env.RUNNERS_JSON ?? null, env.RUNNER_URL ?? null]);
  if (source === executorSource) return parsedExecutors;
  const fail = (): never => { throw new Failure(500, "configuration_error"); };
  const result = new Map<string, Executor>();
  if (env.RUNNERS_JSON) {
    let document: unknown;
    try { document = JSON.parse(env.RUNNERS_JSON); } catch { return fail(); }
    if (!Array.isArray(document) || document.length > EXECUTOR_LIMIT) fail();
    for (const entry of document as unknown[]) {
      if (!entry || typeof entry !== "object" || Array.isArray(entry)) fail();
      const { id, url, binding, ...rest } = entry as Record<string, unknown>;
      if (Object.keys(rest).length || typeof id !== "string" || !EXECUTOR_ID.test(id) || result.has(id)) fail();
      if (binding !== undefined && (typeof binding !== "string" || !EXECUTOR_BINDING.test(binding))) fail();
      const value = executor(id as string, url, fail);
      if (binding !== undefined) {
        executorFetcher(env, binding as string);
        value.binding = binding as string;
      }
      result.set(id as string, value);
    }
  }
  if (env.RUNNER_URL !== undefined) {
    if (result.has(DEFAULT_EXECUTOR)) fail();
    result.set(DEFAULT_EXECUTOR, executor(DEFAULT_EXECUTOR, env.RUNNER_URL, fail));
  }
  executorSource = source;
  parsedExecutors = result;
  return result;
}

function executor(id: string, value: unknown, fail: () => never): Executor {
  if (typeof value !== "string" || value.length > 2048) fail();
  let url: URL;
  try { url = new URL(value as string); } catch { return fail(); }
  const scheme = EXECUTOR_SCHEMES[url.protocol];
  if (!scheme || url.username || url.password || url.search || url.hash || url.href !== `${url.protocol}//${url.host}${url.pathname}`) fail();
  if (scheme === "http:" && !loopback(url.hostname)) fail();
  return { id, url: `${scheme}//${url.host}${url.pathname}` };
}

function executorFetcher(env: ExecutorEnv, name: string): ExecutorTransport {
  if (!EXECUTOR_BINDING.test(name)) throw new Failure(500, "configuration_error");
  const binding = env[name as `RUNNER_VPC_${string}`] as { fetch?: unknown } | null | undefined;
  if (!binding || (typeof binding !== "object" && typeof binding !== "function") || typeof binding.fetch !== "function") throw new Failure(500, "configuration_error");
  return binding as ExecutorTransport;
}

export function executorTransport(env: ExecutorEnv, executor: Executor): ExecutorTransport {
  if (executor.binding === undefined) return { fetch: request => fetch(request) };
  const binding = executorFetcher(env, executor.binding);
  return { fetch: request => binding.fetch(request) };
}

export function executorId(value: unknown): string {
  if (value === undefined || value === null) return DEFAULT_EXECUTOR;
  if (typeof value !== "string" || !EXECUTOR_ID.test(value)) throw new Failure(400, "invalid_request");
  return value;
}

export function executorConnection(value: string): string | undefined {
  if (value === LEGACY_RUNNER) return DEFAULT_EXECUTOR;
  const prefix = `${LEGACY_RUNNER}:`;
  if (!value.startsWith(prefix)) return undefined;
  const id = value.slice(prefix.length);
  return EXECUTOR_ID.test(id) ? id : undefined;
}

export function executorSelected(selected: string[], id: string): boolean {
  return selected.includes(runnerConnection(id)) || (id === DEFAULT_EXECUTOR && selected.includes(LEGACY_RUNNER));
}

export type Denied = { ok: false; status: number; error: string };
export type CacheOutcome = "hit" | "miss" | "coalesced" | "refresh" | "refresh_coalesced";
type Expiring = { ok: true; generation: number; expires_at: number | null };

export class AccessCache<T extends Expiring> {
  private readonly entries = new Map<string, { access: T; until: number }>();
  private readonly inflight = new Map<string, Promise<T | Denied>>();

  constructor(private readonly limit: number) {}

  get size(): number {
    return this.entries.size;
  }

  get pending(): number {
    return this.inflight.size;
  }

  async get(key: string, rejected: number | undefined, policy: { ttl: number; margin: number; horizon?: number }, load: () => Promise<T | Denied>, observe?: (outcome: CacheOutcome) => void): Promise<T | Denied> {
    const entry = this.entries.get(key);
    if (rejected === undefined && entry && entry.until > Date.now()) {
      observe?.("hit");
      return entry.access;
    }
    if (entry && (rejected === undefined || entry.access.generation === rejected)) this.entries.delete(key);
    const flight = rejected === undefined ? key : `${key}\u0001${rejected}`;
    const existing = this.inflight.get(flight);
    if (existing) {
      observe?.(rejected === undefined ? "coalesced" : "refresh_coalesced");
      return existing;
    }
    observe?.(rejected === undefined ? "miss" : "refresh");
    const pending = (async () => {
      const access = await load();
      if (access.ok) {
        const now = Date.now();
        const until = Math.min(now + policy.ttl, access.expires_at === null ? Number.MAX_SAFE_INTEGER : access.expires_at - policy.margin, policy.horizon ?? Number.MAX_SAFE_INTEGER);
        if (until > now) {
          this.entries.delete(key);
          this.entries.set(key, { access, until });
          while (this.entries.size > this.limit) this.entries.delete(this.entries.keys().next().value!);
        }
      }
      return access;
    })();
    this.inflight.set(flight, pending);
    try {
      return await pending;
    } finally {
      this.inflight.delete(flight);
    }
  }

  evict(key: string, generation: number): void {
    if (this.entries.get(key)?.access.generation === generation) this.entries.delete(key);
  }
}

export async function readLimited(response: Response, limit: number): Promise<string | null> {
  if (!response.body) return "";
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > limit) {
      await reader.cancel().catch(() => {});
      return null;
    }
    chunks.push(value);
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  return decoder.decode(bytes);
}

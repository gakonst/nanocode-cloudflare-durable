import type { AuthorityResult, ModelProvider, PrivateBinding } from "../contracts";
import type { AccountAuthority } from "./account";

export interface EgressEnv {
  ACCOUNTS: DurableObjectNamespace<AccountAuthority>;
  OAUTH_JSON?: string;
  CODEX_OAUTH_JSON?: string;
  ANTHROPIC_API_KEY?: string;
  ANTHROPIC_ENDPOINT?: string;
  ACCOUNT_ID?: string;
  CREDENTIAL_CACHE_TTL_MS: string;
  REFRESH_MARGIN_MS: string;
  CONNECT_TOKEN_TTL_MS: string;
  CONNECT_REFRESH_TTL_MS: string;
  CONNECT_CODE_TTL_MS: string;
  RECOVERY_DELAY_MS: string;
  CONNECTIONS_JSON?: string;
  VAULT_KEY?: string;
  SESSION_FENCES?: PrivateBinding;
  RUNNER_URL?: string;
}

export const SUBSCRIPTION = {
  token_url: "https://platform.claude.com/v1/oauth/token",
  client_id: "9d1c250a-e61b-44d9-88ed-5944d1962f5e",
  scopes: ["org:create_api_key", "user:profile", "user:inference", "user:sessions:claude_code", "user:mcp_servers", "user:file_upload", "user:plugins"],
  beta: "oauth-2025-04-20",
};

export const CODEX = {
  endpoint: "https://chatgpt.com/backend-api/codex/responses",
  token_url: "https://auth.openai.com/oauth/token",
  client_id: "app_EMoamEEZ73f0CkXaXp7hrann",
  claims: "https://api.openai.com/auth",
};

export const ACCOUNT_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;
export const SECRET_PATTERN = /^[A-Za-z0-9\-._~+/=]{1,16384}$/;
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

export function setting(env: EgressEnv, name: "CREDENTIAL_CACHE_TTL_MS" | "REFRESH_MARGIN_MS" | "CONNECT_TOKEN_TTL_MS" | "CONNECT_REFRESH_TTL_MS" | "CONNECT_CODE_TTL_MS" | "RECOVERY_DELAY_MS"): number {
  const value = Number(env[name]);
  if (!Number.isSafeInteger(value) || value <= 0) throw new Failure(500, "configuration_error");
  return value;
}

export function modelAccount(env: EgressEnv): string {
  const account = env.ACCOUNT_ID ?? "default";
  if (!ACCOUNT_PATTERN.test(account)) throw new Failure(500, "configuration_error");
  return account;
}

export function mode(env: EgressEnv): "subscription" | "api_key" | "unconfigured" {
  return env.OAUTH_JSON ? "subscription" : env.ANTHROPIC_API_KEY ? "api_key" : "unconfigured";
}

export function modelProvider(value: unknown): ModelProvider {
  if (value === undefined || value === null || value === "claude") return "claude";
  if (value === "codex") return "codex";
  throw new Failure(400, "unsupported_provider");
}

export function jwtClaims(token: string): Record<string, any> | null {
  const parts = token.split(".");
  if (parts.length !== 3 || !parts[1] || !/^[A-Za-z0-9_-]+$/.test(parts[1])) return null;
  try {
    const claims = JSON.parse(decoder.decode(unbase64url(parts[1])));
    return claims && typeof claims === "object" && !Array.isArray(claims) ? claims : null;
  } catch {
    return null;
  }
}

export function modelEndpoint(env: EgressEnv): string {
  const subscription = mode(env) === "subscription";
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

export type ConnectionCredential =
  | { type: "static"; value: string }
  | { type: "oauth"; access: string; refresh: string; expires_at: number; token_url: string; client_id: string; client_secret?: string; scope?: string };

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
  credential: ConnectionCredential;
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
  if (!IDENTIFIER.test(id) || !value || typeof value !== "object") fail();
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
    credential: credential(value.credential, auth, fail),
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

function credential(value: any, auth: Placement, fail: () => never): ConnectionCredential {
  if (value?.type === "static" && typeof value.value === "string" && /^[\x21-\x7e]{1,16384}$/.test(value.value)) return { type: "static", value: value.value };
  if (value?.type === "oauth" && auth.type !== "basic") {
    let url: URL;
    try { url = new URL(value.token_url); } catch { return fail(); }
    if (url.protocol !== "https:" || url.username || url.password || url.hash) fail();
    if (typeof value.access !== "string" || !SECRET_PATTERN.test(value.access) || typeof value.refresh !== "string" || !SECRET_PATTERN.test(value.refresh) || !Number.isSafeInteger(value.expires_at) || typeof value.client_id !== "string" || !value.client_id) fail();
    if ((value.client_secret !== undefined && typeof value.client_secret !== "string") || (value.scope !== undefined && typeof value.scope !== "string")) fail();
    return { type: "oauth", access: value.access, refresh: value.refresh, expires_at: value.expires_at, token_url: url.toString(), client_id: value.client_id, client_secret: value.client_secret, scope: value.scope };
  }
  return fail();
}

export function placeCredential(auth: Placement, secret: string): [string, string] {
  if (auth.type === "bearer") return ["authorization", `Bearer ${secret}`];
  if (auth.type === "basic") return ["authorization", `Basic ${btoa(secret)}`];
  return [auth.name, `${auth.prefix}${secret}`];
}

type VaultKey = { kid: string; key: CryptoKey };
let vaultSource: string | undefined;
let vaultKey: Promise<VaultKey> | undefined;

function keyFor(env: EgressEnv): Promise<VaultKey> | undefined {
  if (!env.VAULT_KEY) return undefined;
  if (env.VAULT_KEY.length < 32) throw new Failure(500, "configuration_error");
  if (vaultSource !== env.VAULT_KEY || !vaultKey) {
    const source = env.VAULT_KEY;
    vaultSource = source;
    vaultKey = (async () => {
      const material = await crypto.subtle.importKey("raw", encoder.encode(source), "HKDF", false, ["deriveKey"]);
      const key = await crypto.subtle.deriveKey({ name: "HKDF", hash: "SHA-256", salt: encoder.encode("nanocode-egress-vault"), info: encoder.encode("credential-envelope-v1") }, material, { name: "AES-GCM", length: 256 }, false, ["encrypt", "decrypt"]);
      return { kid: (await sha256hex(`nanocode-vault-kid:${source}`)).slice(0, 16), key };
    })();
  }
  return vaultKey;
}

export function vaultConfigured(env: EgressEnv): boolean {
  return !!env.VAULT_KEY;
}

export async function seal(env: EgressEnv, aad: string, value: unknown, required: boolean): Promise<string> {
  const pending = keyFor(env);
  if (!pending) {
    if (required) throw new Failure(503, "vault_key_required");
    return `p1.${JSON.stringify(value)}`;
  }
  const { kid, key } = await pending;
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const sealed = new Uint8Array(await crypto.subtle.encrypt({ name: "AES-GCM", iv, additionalData: encoder.encode(aad) }, key, encoder.encode(JSON.stringify(value))));
  return `v1.${kid}.${base64url(iv)}.${base64url(sealed)}`;
}

export async function open<T>(env: EgressEnv, aad: string, envelope: string): Promise<{ value: T; plaintext: boolean }> {
  if (envelope.startsWith("p1.")) return { value: JSON.parse(envelope.slice(3)) as T, plaintext: true };
  const [version, kid, iv, sealed] = envelope.split(".");
  const pending = keyFor(env);
  if (version !== "v1" || !pending) throw new Failure(503, "vault_key_required");
  const key = await pending;
  if (key.kid !== kid) throw new Failure(503, "vault_key_mismatch");
  try {
    const plain = await crypto.subtle.decrypt({ name: "AES-GCM", iv: unbase64url(iv), additionalData: encoder.encode(aad) }, key.key, unbase64url(sealed));
    return { value: JSON.parse(decoder.decode(plain)) as T, plaintext: false };
  } catch {
    throw new Failure(503, "vault_key_mismatch");
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

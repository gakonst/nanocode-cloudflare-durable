import { authorityCall, json, type AuthorityCommand, type AuthorityResult, type PrivateBinding, type SessionDescriptor } from "../contracts";

export interface ConnectEnv {
  AUTHORITY: PrivateBinding;
  SERVICE_EGRESS: PrivateBinding;
  SESSIONS: DurableObjectNamespace<any>;
  ACCOUNT_ID: string;
  CONNECT_STREAM_REVALIDATE_MS: string;
  PUBLIC_ORIGIN?: string;
  SESSION_LOCATION_HINT?: DurableObjectLocationHint;
}

export type Delegation = { token: string; account: string; origin: string; cors: string | null };

export const SCOPES = { write: "agent:write", read: "agent:read" } as const;
export const BODY_LIMIT = 1 << 20;
export const ACCOUNT_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;
export const IDENTIFIER = /^[A-Za-z0-9_.:-]{1,128}$/;
export const SCOPE_TOKEN = /^[\x21\x23-\x5B\x5D-\x7E]{1,128}$/;
const TOKEN = /^(nc1|nr1|na1)\.([A-Za-z0-9_-]{1,64})\.([A-Za-z0-9_-]{43,256})$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const AUTHORITY_CODE = /^[a-z_]{1,64}$/;
const CONTROL = /[\x00-\x1F\x7F]/;

export class ConnectError extends Error {
  constructor(readonly status: number, readonly code: string, message: string) {
    super(message);
  }
}

export function tokenAccount(token: string, prefix: "nc1" | "nr1" | "na1"): string | null {
  const match = TOKEN.exec(token);
  return match && match[1] === prefix ? match[2] : null;
}

export function bearer(request: Request): string | null {
  const match = /^Bearer ([\x21-\x7E]+)$/i.exec(request.headers.get("authorization") ?? "");
  return match ? match[1] : null;
}

export function validOrigin(value: unknown): value is string {
  if (typeof value !== "string" || value.length > 512) return false;
  try {
    const url = new URL(value);
    return url.origin === value && (url.protocol === "https:" || (url.protocol === "http:" && (url.hostname === "localhost" || url.hostname === "127.0.0.1" || url.hostname === "[::1]")));
  } catch {
    return false;
  }
}

export function requestOrigin(request: Request): string | null {
  const origin = request.headers.get("origin");
  if (origin === null) return null;
  if (!validOrigin(origin)) throw new ConnectError(403, "invalid_origin", "origin is not permitted");
  return origin;
}

export function publicOrigin(request: Request, env: ConnectEnv): string {
  return env.PUBLIC_ORIGIN && validOrigin(env.PUBLIC_ORIGIN) ? env.PUBLIC_ORIGIN : new URL(request.url).origin;
}

export function effectiveOrigin(request: Request, env: ConnectEnv): string {
  return requestOrigin(request) ?? publicOrigin(request, env);
}

export function delegation(request: Request, env: ConnectEnv, ownerAuthorized: boolean): Delegation {
  if (ownerAuthorized) throw new ConnectError(403, "invalid_token", "delegated routes require a Connect access token");
  const token = bearer(request);
  const account = token && tokenAccount(token, "nc1");
  if (!token || !account) throw new ConnectError(401, "invalid_token", "a valid Connect access token is required");
  const cors = requestOrigin(request);
  return { token, account, origin: cors ?? publicOrigin(request, env), cors };
}

export function requireOwner(request: Request, env: ConnectEnv, ownerAuthorized: boolean): void {
  if (!ownerAuthorized) throw new ConnectError(401, "unauthorized", "owner authorization required");
  const origin = request.headers.get("origin");
  if (origin !== null && origin !== publicOrigin(request, env)) throw new ConnectError(403, "invalid_origin", "owner requests must originate from the public origin");
}

export async function authority(env: ConnectEnv, command: AuthorityCommand): Promise<AuthorityResult> {
  if (!ACCOUNT_PATTERN.test(command.account_id)) throw new ConnectError(400, "invalid_request", "invalid account routing");
  let result: AuthorityResult;
  try {
    result = await authorityCall(env.AUTHORITY, command);
  } catch {
    throw new ConnectError(503, "temporarily_unavailable", "authority unavailable");
  }
  if (!result || typeof result !== "object") throw new ConnectError(502, "server_error", "invalid authority response");
  if (!result.ok) {
    const status = Number.isInteger(result.status) && result.status! >= 400 && result.status! < 600 ? result.status! : 403;
    throw new ConnectError(status, typeof result.error === "string" && AUTHORITY_CODE.test(result.error) ? result.error : errorCode(status), status >= 500 ? "authority could not complete the request" : "request denied by authority");
  }
  return result;
}

export function authorize(env: ConnectEnv, delegated: Delegation, fields: { required_scope?: string; thread_id?: string; create_thread?: boolean }): Promise<AuthorityResult> {
  return authority(env, { op: "authorize", account_id: delegated.account, token: delegated.token, origin: delegated.origin, ...fields });
}

export function descriptorOf(result: AuthorityResult, account: string, thread?: string): SessionDescriptor {
  const descriptor = result.descriptor;
  if (!descriptor || descriptor.account_id !== account || typeof descriptor.session_name !== "string" || !descriptor.session_name || typeof descriptor.session_id !== "string" || typeof descriptor.grant_id !== "string" || typeof descriptor.thread_id !== "string" || (thread !== undefined && descriptor.thread_id !== thread)) {
    throw new ConnectError(502, "server_error", "invalid authority descriptor");
  }
  return descriptor;
}

export function errorCode(status: number): string {
  if (status === 400) return "invalid_request";
  if (status === 401) return "invalid_token";
  if (status === 403) return "forbidden";
  if (status === 404) return "not_found";
  if (status === 409) return "conflict";
  if (status === 413) return "payload_too_large";
  if (status === 429) return "rate_limited";
  if (status === 503) return "temporarily_unavailable";
  return "server_error";
}

export function failure(error: unknown, metadata: string): Response {
  const failed = error instanceof ConnectError ? error : new ConnectError(500, "server_error", "internal error");
  const headers = new Headers();
  if (failed.status === 401) headers.set("www-authenticate", failed.code === "unauthorized" ? 'Bearer realm="owner"' : `Bearer error="invalid_token", resource_metadata="${metadata}"`);
  return json({ error: failed.code, error_description: failed.message }, failed.status, headers);
}

export function project(result: AuthorityResult, fallback = 200): Response {
  const { ok, status, ...rest } = result;
  return json(rest, Number.isInteger(status) && status! >= 200 && status! < 300 ? status! : fallback);
}

export function withCors(response: Response, origin: string | null, expose?: string): Response {
  if (!origin) return response;
  const result = new Response(response.body, response);
  result.headers.set("access-control-allow-origin", origin);
  result.headers.append("vary", "Origin");
  if (expose) result.headers.set("access-control-expose-headers", expose);
  return result;
}

export function preflight(request: Request, methods: string[], headers: string[], origin?: string): Response {
  const requested = request.headers.get("access-control-request-method");
  const requestedHeaders = (request.headers.get("access-control-request-headers") ?? "").split(",").map(value => value.trim().toLowerCase()).filter(Boolean);
  const caller = request.headers.get("origin");
  if (!validOrigin(caller) || (origin !== undefined && origin !== "*" && caller !== origin) || !requested || !methods.includes(requested) || !requestedHeaders.every(value => headers.includes(value))) {
    return new Response(null, { status: 403, headers: { vary: "Origin" } });
  }
  return new Response(null, {
    status: 204,
    headers: {
      "access-control-allow-origin": origin === "*" ? "*" : caller,
      "access-control-allow-methods": methods.join(", "),
      "access-control-allow-headers": headers.join(", "),
      "access-control-max-age": "600",
      vary: "Origin",
    },
  });
}

export async function readText(request: Request, limit = BODY_LIMIT): Promise<string> {
  const declared = Number(request.headers.get("content-length") ?? "0");
  if (declared > limit) throw new ConnectError(413, "payload_too_large", "request body too large");
  if (!request.body) return "";
  const reader = request.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    size += value.byteLength;
    if (size > limit) {
      await reader.cancel();
      throw new ConnectError(413, "payload_too_large", "request body too large");
    }
    chunks.push(value);
  }
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  try {
    return new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(bytes);
  } catch {
    throw new ConnectError(400, "invalid_request", "request body must be UTF-8");
  }
}

export async function readJson(request: Request): Promise<any> {
  const text = await readText(request);
  try {
    return JSON.parse(text);
  } catch {
    throw new ConnectError(400, "invalid_request", "invalid JSON");
  }
}

export async function readForm(request: Request): Promise<Record<string, unknown>> {
  const type = (request.headers.get("content-type") ?? "").split(";")[0].trim().toLowerCase();
  if (type === "application/json") {
    const value = await readJson(request);
    if (!value || typeof value !== "object" || Array.isArray(value)) throw new ConnectError(400, "invalid_request", "expected a JSON object");
    return value;
  }
  if (type !== "application/x-www-form-urlencoded") throw new ConnectError(400, "invalid_request", "unsupported content type");
  const result: Record<string, unknown> = {};
  for (const [key, value] of new URLSearchParams(await readText(request))) {
    if (key in result) throw new ConnectError(400, "invalid_request", `duplicate parameter ${key}`);
    result[key] = value;
  }
  return result;
}

export function text(value: unknown, name: string, limit = 256): string {
  if (typeof value !== "string" || !value.trim() || value.length > limit || CONTROL.test(value)) throw new ConnectError(400, "invalid_request", `${name} must be a nonempty string of at most ${limit} characters`);
  return value;
}

export function optionalText(value: unknown, name: string, limit = 256): string | undefined {
  return value === undefined ? undefined : text(value, name, limit);
}

export function strings(value: unknown, name: string, pattern: RegExp, limit = 64): string[] {
  if (!Array.isArray(value) || value.length > limit || !value.every(item => typeof item === "string" && pattern.test(item)) || new Set(value).size !== value.length) throw new ConnectError(400, "invalid_request", `${name} must be an array of unique valid strings`);
  return value;
}

export function threadId(value: unknown): string {
  if (typeof value !== "string" || !UUID.test(value)) throw new ConnectError(400, "invalid_request", "thread_id must be a UUID");
  return value.toLowerCase();
}

export function segment(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    throw new ConnectError(400, "invalid_request", "invalid path encoding");
  }
}

export function positiveInteger(value: string, name: string): number {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) throw new ConnectError(503, "temporarily_unavailable", `${name} must be a positive integer`);
  return parsed;
}

function headerJson(value: unknown): string {
  return JSON.stringify(value).replace(/[\u007f-￿]/g, character => `\\u${character.charCodeAt(0).toString(16).padStart(4, "0")}`);
}

export async function sessionFetch(env: ConnectEnv, descriptor: SessionDescriptor, path: string, method: string, body?: unknown, accept?: string): Promise<Response> {
  const sessions: DurableObjectNamespace = env.SESSIONS;
  const stub = sessions.get(sessions.idFromName(descriptor.session_name), { locationHint: env.SESSION_LOCATION_HINT });
  const headers = new Headers({ "x-nanocode-descriptor": headerJson(descriptor) });
  if (body !== undefined) headers.set("content-type", "application/json");
  if (accept) headers.set("accept", accept);
  let response: Response;
  try {
    response = await stub.fetch(new Request(`https://session.internal${path}`, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) }));
  } catch {
    throw new ConnectError(503, "temporarily_unavailable", "session unavailable");
  }
  return relay(response);
}

export function relay(response: Response): Response {
  const headers = new Headers({ "cache-control": "no-store" });
  const type = response.headers.get("content-type");
  if (type) headers.set("content-type", type);
  return new Response(response.body, { status: response.status, headers });
}

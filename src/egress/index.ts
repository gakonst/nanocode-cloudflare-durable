import { WorkerEntrypoint } from "cloudflare:workers";
import { json, sessionUuid, type ModelDispatch, type ModelProvider, type SessionAuthority, type SocketDispatch } from "../contracts";
import type { ModelAccess } from "../vault";
import { remoteExecutor } from "../remote/egress";
import { modelFetch, socketFetch } from "./model-transport";
import { AccessCache, ACCOUNT_PATTERN, CODEX, Failure, modelAccount, modelEndpoint, modelProvider, readLimited, setting, SUBSCRIPTION, type CacheOutcome, type EgressEnv } from "./common";

export type { EgressEnv };

const MODEL_REQUEST_HEADER = /^(accept|content-type|anthropic-version|anthropic-beta|anthropic-dangerous-direct-browser-access|user-agent|x-app|x-claude-code-session-id|x-stainless-[a-z0-9-]{1,64})$/;
const MODEL_RESPONSE_HEADER = /^(content-type|request-id|retry-after|x-should-retry|anthropic-ratelimit-[a-z0-9-]{1,64})$/;
const CODEX_REQUEST_HEADER = /^(accept|content-type|user-agent|originator|version|openai-beta|session-id|thread-id|x-client-request-id|x-codex-turn-state|x-codex-turn-metadata|x-openai-internal-codex-responses-lite)$/;
const CODEX_SOCKET_HEADER = /^(user-agent|originator|version|openai-beta|session-id|thread-id|x-client-request-id|x-codex-turn-state|x-codex-turn-metadata|x-openai-internal-codex-responses-lite|x-responsesapi-include-timing-metrics)$/;
const CODEX_RESPONSE_HEADER = /^(content-type|retry-after|x-request-id|x-oai-request-id|openai-model|openai-processing-ms|x-reasoning-included|x-models-etag|x-codex-[a-z0-9-]{1,64})$/;
const PROVIDER_ERRORS = new Set(["invalid_request_error", "authentication_error", "permission_error", "not_found_error", "request_too_large", "rate_limit_error", "api_error", "overloaded_error", "timeout_error", "billing_error"]);
const CODEX_ERROR_CODES = new Set(["context_length_exceeded", "insufficient_quota", "usage_not_included", "cyber_policy", "misalignment_policy_violation", "invalid_prompt", "bio_policy", "usage_limit_reached", "rate_limit_exceeded", "invalid_function_parameters", "previous_response_not_found", "invalid_encrypted_content", "server_error", "server_is_overloaded", "slow_down", "websocket_connection_limit_reached", "temporarily_unavailable", "model_not_found"]);
const CACHE_LIMIT = 1024;
const GZIP_HOST = "api.anthropic.com";
const GZIP_MIN_BODY_BYTES = 4096;
const GZIP_PADDING_LENGTHS = 257;
const GZIP_REJECTIONS = new Set([400, 403, 415]);
const INVALID_JSON_MESSAGES = ["The request body is not valid JSON", "Failed to parse request: could not parse request body as JSON"];
const BAD_JSON_DETAILS = new Set(["unexpected EOF", "unexpected end of JSON input", "EOF"]);
const DIAGNOSTIC_LIMIT = 64;
const DIAGNOSTIC_MAX_MS = 3_600_000;
const DIAGNOSTIC_ERRORS = new Set(["none", "other", "credential_unchanged", "invalid_request", "unsupported_provider", "subscription_mismatch", "configuration_error", "internal_error", "credential_rejected", "invalid_token", "login_required", "refresh_uncertain", "forbidden", "grant_expired", "grant_revoked", "insufficient_scope", "origin_mismatch", "account_mismatch", "catalog_mismatch", "not_found", "vault_key_mismatch", "vault_key_required", "credential_missing", "vault_unavailable", "upstream_cancelled", "upstream_unavailable", "upstream_timeout"]);
const CREDENTIAL_OUTCOMES = new Set<CredentialOutcome>(["none", "hit", "miss", "coalesced", "refresh", "refresh_coalesced"]);
const clock: () => number = typeof performance === "object" && performance !== null && typeof performance.now === "function" ? () => performance.now() : () => Date.now();

type Granted = Extract<ModelAccess, { ok: true }>;
type DiagnosticOrigin = "local_validation" | "authority" | "provider_http" | "transport" | "projection";
type DiagnosticMime = "json" | "html" | "event_stream" | "other" | "missing";
type CredentialOutcome = "none" | CacheOutcome;
type TransportKind = "direct" | "fixed_binding";
type AttemptPhase = "authority" | "transport" | "discard" | "projection";
type AttemptDiagnostic = { attempt: 1 | 2; status: number; origin: DiagnosticOrigin; credential: CredentialOutcome; transport: TransportKind; auth_lookup_ms: number; fetch_header_ms: number; discard_ms: number; error_projection_ms: number; mime: DiagnosticMime; mitigated: boolean; auth_error: boolean; request_id: boolean; error: string; body_bytes: number; sent_bytes: number; gzip: boolean; gzip_rejected: boolean };
type Encoded = { gzip?: Promise<ArrayBuffer>; sent_bytes: number; gzip_sent: boolean; gzip_rejected: boolean };
type ModelDiagnostic = { provider: "claude" | "codex"; websocket: boolean; attempt: 1 | 2; status: number; origin: DiagnosticOrigin; auth_lookup_ms: number; fetch_header_ms: number; error_projection_ms: number; mime: DiagnosticMime; mitigated: boolean; auth_error: boolean; request_id: boolean; credential: CredentialOutcome; transport: TransportKind; request_ms: number; total_ms: number; retried: boolean; rotated: boolean; error: string; attempts: AttemptDiagnostic[] };

const metrics = {
  model_requests: 0,
  model_sockets: 0,
  model_cache_hits: 0,
  model_cache_misses: 0,
  model_coalesced: 0,
  vault_rpcs: 0,
  model_unauthorized: 0,
  model_recoveries: 0,
  model_provider_errors: 0,
  redirects_rejected: 0,
  upstream_failures: 0,
  model_timeouts: 0,
  model_gzip_requests: 0,
  model_gzip_rejections: 0,
  model_gzip_input_bytes: 0,
  model_gzip_output_bytes: 0,
};
const models = new AccessCache<Granted>(CACHE_LIMIT);
const diagnostics: ModelDiagnostic[] = [];
let gzipRejected = false;

function snapshot() {
  return { ...metrics, cache_entries: models.size, inflight: models.pending };
}

function attemptDiagnostic(attempt: 1 | 2, origin: DiagnosticOrigin, transport: TransportKind): AttemptDiagnostic {
  return { attempt, status: 0, origin, credential: "none", transport, auth_lookup_ms: 0, fetch_header_ms: 0, discard_ms: 0, error_projection_ms: 0, mime: "missing", mitigated: false, auth_error: false, request_id: false, error: "none", body_bytes: 0, sent_bytes: 0, gzip: false, gzip_rejected: false };
}

function elapsed(start: number, end: number = clock()): number {
  const ms = Math.floor(end - start);
  return Number.isFinite(ms) ? Math.min(Math.max(ms, 0), DIAGNOSTIC_MAX_MS) : 0;
}

function milliseconds(value: unknown): number {
  return typeof value === "number" && Number.isSafeInteger(value) ? Math.min(Math.max(value, 0), DIAGNOSTIC_MAX_MS) : 0;
}

function statusOf(status: unknown): number {
  return typeof status === "number" && Number.isSafeInteger(status) && status >= 0 && status <= 999 ? status : 0;
}

function errorOf(code: unknown): string {
  return typeof code === "string" && DIAGNOSTIC_ERRORS.has(code) ? code : "other";
}

function mimeOf(type: string | null): DiagnosticMime {
  return type === null ? "missing" : /^application\/([a-z0-9.+-]*\+)?json\s*(;|$)/i.test(type) ? "json" : /^text\/html\s*(;|$)/i.test(type) ? "html" : /^text\/event-stream\s*(;|$)/i.test(type) ? "event_stream" : "other";
}

function classify(trace: AttemptDiagnostic, response: Response): void {
  trace.status = statusOf(response.status);
  trace.origin = "provider_http";
  trace.mime = mimeOf(response.headers.get("content-type"));
  trace.mitigated = response.headers.has("cf-mitigated");
  trace.auth_error = response.headers.has("x-openai-authorization-error");
  trace.request_id = response.headers.has("x-request-id") || response.headers.has("x-oai-request-id") || response.headers.has("request-id");
}

function attemptValue(trace: AttemptDiagnostic): AttemptDiagnostic {
  return { attempt: trace.attempt === 2 ? 2 : 1, status: statusOf(trace.status), origin: trace.origin, credential: CREDENTIAL_OUTCOMES.has(trace.credential) ? trace.credential : "none", transport: trace.transport === "fixed_binding" ? "fixed_binding" : "direct", auth_lookup_ms: milliseconds(trace.auth_lookup_ms), fetch_header_ms: milliseconds(trace.fetch_header_ms), discard_ms: milliseconds(trace.discard_ms), error_projection_ms: milliseconds(trace.error_projection_ms), mime: trace.mime, mitigated: trace.mitigated === true, auth_error: trace.auth_error === true, request_id: trace.request_id === true, error: errorOf(trace.error), body_bytes: bytesOf(trace.body_bytes), sent_bytes: bytesOf(trace.sent_bytes), gzip: trace.gzip === true, gzip_rejected: trace.gzip_rejected === true };
}

function bytesOf(value: unknown): number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : 0;
}

function diagnosticValue(provider: ModelProvider, attempts: AttemptDiagnostic[], request_ms: number, total_ms: number, rotated: boolean, websocket = false): ModelDiagnostic {
  const values = attempts.slice(0, 2).map(attemptValue);
  const last = values[values.length - 1] ?? attemptValue(attemptDiagnostic(1, "local_validation", "direct"));
  return { provider: provider === "codex" ? "codex" : "claude", websocket: websocket === true, attempt: last.attempt, status: last.status, origin: last.origin, auth_lookup_ms: last.auth_lookup_ms, fetch_header_ms: last.fetch_header_ms, error_projection_ms: last.error_projection_ms, mime: last.mime, mitigated: last.mitigated, auth_error: last.auth_error, request_id: last.request_id, credential: last.credential, transport: last.transport, request_ms: milliseconds(request_ms), total_ms: milliseconds(total_ms), retried: values.length > 1, rotated: rotated === true, error: last.error, attempts: values };
}

function record(trace: ModelDiagnostic): void {
  diagnostics.push(trace);
  if (diagnostics.length > DIAGNOSTIC_LIMIT) diagnostics.splice(0, diagnostics.length - DIAGNOSTIC_LIMIT);
}

function modelResponse(response: Response, trace: ModelDiagnostic): Response {
  const headers = new Headers(response.headers);
  headers.set("x-nanocode-model-diagnostic", JSON.stringify(trace));
  return new Response(response.body, { status: response.status, statusText: response.statusText, headers, webSocket: response.webSocket });
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

function codexError(status: number, error: Record<string, unknown>, retryAfter?: string | null): Response {
  const headers = new Headers({ "content-type": "application/json", "cache-control": "no-store" });
  if (retryAfter && /^[\x20-\x7e]{1,64}$/.test(retryAfter)) headers.set("retry-after", retryAfter);
  return new Response(JSON.stringify({ error }), { status, headers });
}

function egressError(failure: { status: number; error: string }, provider: ModelProvider = "claude"): Response {
  const type = failure.status === 401 ? "authentication_error" : failure.status === 403 ? "permission_error" : failure.status === 400 || failure.status === 409 ? "invalid_request_error" : "api_error";
  if (provider === "codex") return codexError(failure.status, { type, code: failure.error, message: failure.error });
  return providerError(failure.status, type, failure.error);
}

function authorityOf(value: any): SessionAuthority {
  if (!value || typeof value !== "object" || typeof value.account_id !== "string" || !ACCOUNT_PATTERN.test(value.account_id) || typeof value.session_id !== "string" || !value.session_id || value.session_id.length > 128) throw new Failure(400, "invalid_request");
  return { account_id: value.account_id, session_id: value.session_id };
}

function cacheKey(authority: SessionAuthority, provider: ModelProvider): string {
  return `${provider}\u0000${authority.account_id}`;
}

function resolveModel(env: EgressEnv, authority: SessionAuthority, provider: ModelProvider, rejected?: number, observe?: (outcome: CredentialOutcome) => void): Promise<ModelAccess> {
  return models.get(cacheKey(authority, provider), rejected, { ttl: setting(env, "CREDENTIAL_CACHE_TTL_MS"), margin: setting(env, "REFRESH_MARGIN_MS") }, () => vaultModel(env, authority.account_id, provider, rejected), outcome => {
    if (outcome === "hit") metrics.model_cache_hits++;
    else if (outcome === "miss") metrics.model_cache_misses++;
    else if (outcome !== "refresh") metrics.model_coalesced++;
    observe?.(outcome);
  });
}

async function vaultModel(env: EgressEnv, account_id: string, provider: ModelProvider, rejected?: number): Promise<ModelAccess> {
  metrics.vault_rpcs++;
  return await env.VAULT.model({ account_id, provider, rejected_generation: rejected }) as ModelAccess;
}

function modelHeaders(input: unknown): Headers {
  if (!Array.isArray(input) || input.length > 128) throw new Failure(400, "invalid_request");
  const headers = new Headers();
  for (const pair of input) {
    if (!Array.isArray(pair) || pair.length !== 2 || typeof pair[0] !== "string" || typeof pair[1] !== "string") throw new Failure(400, "invalid_request");
    const name = pair[0].toLowerCase();
    if (!MODEL_REQUEST_HEADER.test(name)) continue;
    try { headers.append(name, pair[1]); } catch { throw new Failure(400, "invalid_request"); }
  }
  return headers;
}

function subscriptionHeaders(headers: Headers, subscription: boolean): Headers {
  const betas = (headers.get("anthropic-beta") ?? "").split(",").map(beta => beta.trim()).filter(beta => beta && (subscription || beta !== SUBSCRIPTION.beta));
  if (subscription && !betas.includes(SUBSCRIPTION.beta)) betas.push(SUBSCRIPTION.beta);
  const unique = [...new Set(betas)];
  if (unique.length) headers.set("anthropic-beta", unique.join(","));
  else headers.delete("anthropic-beta");
  headers.set("content-type", "application/json");
  if (!headers.has("anthropic-version")) headers.set("anthropic-version", "2023-06-01");
  return headers;
}

function codexAllowed(input: unknown, allowed: RegExp): Headers {
  if (!Array.isArray(input) || input.length > 128) throw new Failure(400, "invalid_request");
  const headers = new Headers();
  for (const pair of input) {
    if (!Array.isArray(pair) || pair.length !== 2 || typeof pair[0] !== "string" || typeof pair[1] !== "string") throw new Failure(400, "invalid_request");
    const name = pair[0].toLowerCase();
    if (!allowed.test(name)) continue;
    try { headers.set(name, pair[1]); } catch { throw new Failure(400, "invalid_request"); }
  }
  return headers;
}

function codexHeaders(input: unknown): Headers {
  const headers = codexAllowed(input, CODEX_REQUEST_HEADER);
  headers.set("content-type", "application/json");
  if (!headers.has("accept")) headers.set("accept", "text/event-stream");
  return headers;
}

function gzipPadded(body: ArrayBuffer): Uint8Array {
  const random = crypto.getRandomValues(new Uint8Array(2 + GZIP_PADDING_LENGTHS));
  const length = (random[0] | (random[1] << 8)) % GZIP_PADDING_LENGTHS;
  const padded = new Uint8Array(body.byteLength + 1 + length);
  padded.set(new Uint8Array(body));
  padded[body.byteLength] = 0x20;
  for (let index = 0; index < length; index++) padded[body.byteLength + 1 + index] = random[2 + index] & 1 ? 0x09 : 0x20;
  return padded;
}

function gzipBody(body: ArrayBuffer): Promise<ArrayBuffer> {
  return new Response(new Blob([gzipPadded(body)]).stream().pipeThrough(new CompressionStream("gzip"))).arrayBuffer();
}

function badJson(text: string): boolean {
  if (!text.startsWith("bad json: ")) return false;
  const detail = text.slice("bad json: ".length);
  return detail.startsWith("invalid character ") || BAD_JSON_DETAILS.has(detail);
}

function invalidJson(body: string): boolean {
  let value: any;
  try { value = JSON.parse(body); } catch { return badJson(body.trimEnd()); }
  if (value?.type === "error" && typeof value.error?.message === "string") return INVALID_JSON_MESSAGES.some(prefix => value.error.message.startsWith(prefix));
  return value?.reason === "bad_json" && typeof value.error === "string" && badJson(value.error);
}

function sendModel(env: EgressEnv, provider: ModelProvider, endpoint: string, headers: Headers, access: Granted, body: ArrayBuffer, signal: AbortSignal, encoded: Encoded): Promise<Response> {
  const outgoing = new Headers(headers);
  for (const [name, value] of access.headers) outgoing.set(name, value);
  const send = (payload: ArrayBuffer, gzip: boolean) => {
    encoded.sent_bytes = payload.byteLength;
    encoded.gzip_sent = gzip;
    const request = new Request(endpoint, { method: "POST", headers: outgoing, body: payload, redirect: "manual", signal });
    if (gzip) request.headers.set("content-encoding", "gzip");
    return modelFetch(request, outgoing => provider === "codex" && env.CODEX_TRANSPORT ? env.CODEX_TRANSPORT.fetch(outgoing) : fetch(outgoing), setting(env, "MODEL_RESPONSE_HEADERS_TIMEOUT_MS"), setting(env, "MODEL_STREAM_IDLE_TIMEOUT_MS"), () => { metrics.model_timeouts++; });
  };
  if (provider !== "claude" || gzipRejected || body.byteLength < GZIP_MIN_BODY_BYTES || new URL(endpoint).hostname !== GZIP_HOST) return send(body, false);
  return (async () => {
    encoded.gzip ??= gzipBody(body);
    const compressed = await encoded.gzip;
    metrics.model_gzip_requests++;
    metrics.model_gzip_input_bytes += body.byteLength;
    metrics.model_gzip_output_bytes += compressed.byteLength;
    const response = await send(compressed, true);
    if (!GZIP_REJECTIONS.has(response.status)) return response;
    const text = await readLimited(response, 65_536);
    const anthropic = (response.headers.get("request-id") ?? "").startsWith("req_") || response.headers.has("cf-ray");
    if (anthropic && (response.status !== 400 || !invalidJson(text ?? ""))) return new Response(text, { status: response.status, statusText: response.statusText, headers: response.headers });
    gzipRejected = true;
    encoded.gzip_rejected = true;
    metrics.model_gzip_rejections++;
    return send(body, false);
  })();
}

function sendSocket(env: EgressEnv, headers: Headers, access: Granted, signal: AbortSignal): Promise<Response> {
  const outgoing = new Headers(headers);
  for (const [name, value] of access.headers) outgoing.set(name, value);
  const request = new Request(CODEX.endpoint, { headers: outgoing, redirect: "manual", signal });
  return socketFetch(request, outgoing => env.CODEX_TRANSPORT ? env.CODEX_TRANSPORT.fetch(outgoing) : fetch(outgoing), setting(env, "MODEL_RESPONSE_HEADERS_TIMEOUT_MS"), () => { metrics.model_timeouts++; });
}

function codexDetail(parsed: any): Record<string, unknown> {
  const source = [parsed?.error, parsed?.response?.error].find(item => item && typeof item === "object" && !Array.isArray(item)) ?? {};
  const detail: Record<string, unknown> = { type: "api_error", message: "Provider request failed" };
  if (typeof source.type === "string" && (PROVIDER_ERRORS.has(source.type) || CODEX_ERROR_CODES.has(source.type))) detail.type = source.type;
  const code = typeof source.code === "string" ? source.code : parsed?.code;
  if (typeof code === "string" && CODEX_ERROR_CODES.has(code)) detail.code = code;
  const param = typeof source.param === "string" ? source.param : parsed?.param;
  if (detail.code === "invalid_function_parameters" && typeof param === "string" && param.length <= 256) {
    const path = /^(input\[\d{1,6}\](?:\.tools\[\d{1,6}\])+\.parameters)(?:$|[.\[])/.exec(param);
    if (path) detail.param = path[1];
  }
  if (typeof source.retry_after === "number" && Number.isFinite(source.retry_after) && source.retry_after >= 0) detail.retry_after = source.retry_after;
  return detail;
}

async function projectModel(response: Response, provider: ModelProvider = "claude"): Promise<Response> {
  const codex = provider === "codex";
  if (response.status >= 300 && response.status < 400) {
    metrics.redirects_rejected++;
    await response.body?.cancel();
    if (codex) return codexError(502, { type: "api_error", message: "Provider redirect rejected" });
    return providerError(502, "api_error", "Provider redirect rejected");
  }
  if (response.ok) {
    const headers = new Headers({ "cache-control": "no-store" });
    const allowed = codex ? CODEX_RESPONSE_HEADER : MODEL_RESPONSE_HEADER;
    for (const [name, value] of response.headers) if (allowed.test(name)) headers.set(name, value);
    return new Response(response.body, { status: response.status, headers });
  }
  metrics.model_provider_errors++;
  const text = await readLimited(response, 65_536);
  if (codex) {
    let parsed: unknown;
    try { parsed = JSON.parse(text ?? ""); } catch {}
    return codexError(response.status, codexDetail(parsed), response.headers.get("retry-after"));
  }
  let type = "api_error";
  try {
    const parsed = JSON.parse(text ?? "");
    if (typeof parsed?.error?.type === "string" && PROVIDER_ERRORS.has(parsed.error.type)) type = parsed.error.type;
  } catch {}
  return providerError(response.status, type, "Provider request failed", response.headers.get("retry-after"));
}

async function projectSocket(response: Response): Promise<Response> {
  if (response.status >= 300) return projectModel(response, "codex");
  metrics.model_provider_errors++;
  await response.body?.cancel();
  return codexError(502, { type: "api_error", message: "Provider did not upgrade the connection" });
}

async function openSocket(env: EgressEnv, request: Request): Promise<Response> {
  const entered = clock();
  metrics.model_sockets++;
  const transport: TransportKind = env.CODEX_TRANSPORT ? "fixed_binding" : "direct";
  const attempts: AttemptDiagnostic[] = [];
  let request_ms = -1;
  let rotated = false;
  let mark = entered;
  const finish = (response: Response): Response => {
    const total_ms = elapsed(entered);
    const trace = diagnosticValue("codex", attempts, request_ms < 0 ? total_ms : request_ms, total_ms, rotated, true);
    record(trace);
    return modelResponse(response, trace);
  };
  try {
    let dispatch: SocketDispatch;
    try { dispatch = JSON.parse(request.headers.get("x-nanocode-socket") ?? ""); } catch { throw new Failure(400, "invalid_request"); }
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket" || !dispatch || typeof dispatch !== "object") throw new Failure(400, "invalid_request");
    if (modelProvider(dispatch.provider) !== "codex") throw new Failure(400, "unsupported_provider");
    const authority = authorityOf(dispatch.authority);
    const headers = codexAllowed(dispatch.headers, CODEX_SOCKET_HEADER);
    headers.set("upgrade", "websocket");
    const idle = setting(env, "MODEL_STREAM_IDLE_TIMEOUT_MS");
    request_ms = elapsed(entered);
    let previous: Granted | undefined;
    for (;;) {
      const trace = attemptDiagnostic(previous ? 2 : 1, "authority", transport);
      attempts.push(trace);
      mark = clock();
      const access = await resolveModel(env, authority, "codex", previous?.generation, outcome => { trace.credential = outcome; });
      trace.auth_lookup_ms = elapsed(mark);
      if (!access.ok) {
        trace.status = statusOf(access.status);
        trace.error = errorOf(access.error);
        return finish(egressError(access, "codex"));
      }
      if (previous) {
        if (access.generation === previous.generation) {
          trace.status = 401;
          trace.error = "credential_unchanged";
          return finish(codexError(401, { type: "authentication_error", message: "Provider request failed" }));
        }
        rotated = true;
        metrics.model_recoveries++;
      }
      trace.origin = "transport";
      mark = clock();
      const response = await sendSocket(env, headers, access, request.signal);
      trace.fetch_header_ms = elapsed(mark);
      classify(trace, response);
      if (response.status === 101 && response.webSocket) {
        const allowed = new Headers({ "cache-control": "no-store", "x-nanocode-stream-idle-ms": String(idle) });
        for (const [name, value] of response.headers) if (CODEX_RESPONSE_HEADER.test(name)) allowed.set(name, value);
        return finish(new Response(null, { status: 101, headers: allowed, webSocket: response.webSocket }));
      }
      if (response.status !== 401 || previous) {
        if (response.status === 401) models.evict(cacheKey(authority, "codex"), access.generation);
        mark = clock();
        const projected = await projectSocket(response);
        trace.error_projection_ms = elapsed(mark);
        return finish(projected);
      }
      metrics.model_unauthorized++;
      mark = clock();
      await response.body?.cancel();
      trace.discard_ms = elapsed(mark);
      previous = access;
    }
  } catch (error) {
    const failure = denial(error);
    const trace = attempts[attempts.length - 1];
    if (trace) {
      if (trace.origin === "transport" && trace.status === 0) trace.fetch_header_ms = elapsed(mark);
      trace.status = statusOf(failure.status);
      trace.error = errorOf(failure.error);
    } else {
      if (request_ms < 0) request_ms = elapsed(entered);
      const local = attemptDiagnostic(1, "local_validation", transport);
      local.status = statusOf(failure.status);
      local.error = errorOf(failure.error);
      attempts.push(local);
    }
    return finish(egressError(failure, "codex"));
  }
}

export class SessionEgress extends WorkerEntrypoint<EgressEnv> {
  async fetch(request: Request): Promise<Response> {
    if (request.method === "GET") {
      if (new URL(request.url).pathname === "/socket") return openSocket(this.env, request);
      if (new URL(request.url).pathname === "/remote") return remoteExecutor(this.env, request);
      if (new URL(request.url).pathname === "/probe/codex") {
        const id = sessionUuid();
        const access = await resolveModel(this.env, { account_id: modelAccount(this.env), session_id: id }, "codex");
        if (!access.ok) return json({ status: access.status, origin: "authority" });
        const headers = new Headers({ upgrade: "websocket", "openai-beta": "responses_websockets=2026-02-06", "x-openai-internal-codex-responses-lite": "true", "user-agent": "nanocode-worker/0.1.0", "session-id": id, "thread-id": id, "x-client-request-id": id });
        for (const [name, value] of access.headers) headers.set(name, value);
        const start = clock();
        try {
          const request = new Request(CODEX.endpoint, { headers, redirect: "manual" });
          const response = await (this.env.CODEX_TRANSPORT ? this.env.CODEX_TRANSPORT.fetch(request) : fetch(request));
          const duration = elapsed(start);
          const mime = mimeOf(response.headers.get("content-type"));
          if (response.webSocket) { response.webSocket.accept(); response.webSocket.close(1000, "transport probe"); }
          else await response.body?.cancel();
          return json({ status: response.status, mime, milliseconds: duration, websocket: !!response.webSocket });
        } catch { return json({ status: 0, origin: "transport", milliseconds: elapsed(start) }); }
      }
      return json({ ok: true, metrics: { ...snapshot(), model_diagnostics: diagnostics } });
    }
    if (request.method !== "POST") return json({ error: "method_not_allowed" }, 405);
    const path = new URL(request.url).pathname;
    if (path !== "/model") return json({ error: "not_found" }, 404);
    const entered = clock();
    metrics.model_requests++;
    let provider: ModelProvider = "claude";
    let transport: TransportKind = "direct";
    let request_ms = -1;
    let rotated = false;
    const attempts: AttemptDiagnostic[] = [];
    let active: AttemptDiagnostic | null = null;
    let phase: AttemptPhase = "authority";
    let mark = entered;
    let open = false;
    const begin = (next: AttemptPhase) => { phase = next; open = true; mark = clock(); };
    const end = () => {
      if (!open || !active) return;
      open = false;
      const ms = elapsed(mark);
      if (phase === "authority") active.auth_lookup_ms = ms;
      else if (phase === "transport") active.fetch_header_ms = ms;
      else if (phase === "discard") active.discard_ms = ms;
      else active.error_projection_ms = ms;
    };
    const start = (attempt: 1 | 2): AttemptDiagnostic => {
      const trace = attemptDiagnostic(attempt, "authority", transport);
      attempts.push(trace);
      active = trace;
      return trace;
    };
    const finish = (response: Response): Response => {
      const total_ms = elapsed(entered);
      const trace = diagnosticValue(provider, attempts, request_ms < 0 ? total_ms : request_ms, total_ms, rotated);
      record(trace);
      return modelResponse(response, trace);
    };
    try {
      let dispatch: ModelDispatch;
      try { dispatch = JSON.parse(request.headers.get("x-nanocode-model") ?? ""); } catch { throw new Failure(400, "invalid_request"); }
      if (!dispatch || typeof dispatch !== "object") throw new Failure(400, "invalid_request");
      provider = modelProvider(dispatch.provider);
      transport = provider === "codex" && this.env.CODEX_TRANSPORT ? "fixed_binding" : "direct";
      const authority = authorityOf(dispatch.authority);
      const body = await request.arrayBuffer();
      let endpoint = CODEX.endpoint;
      const headers = provider === "codex" ? codexHeaders(dispatch.headers) : modelHeaders(dispatch.headers);
      request_ms = elapsed(entered);
      const one = start(1);
      begin("authority");
      const first = await resolveModel(this.env, authority, provider, undefined, outcome => { one.credential = outcome; });
      end();
      if (!first.ok) {
        one.status = statusOf(first.status);
        one.error = errorOf(first.error);
        return finish(egressError(first, provider));
      }
      if (provider === "claude") {
        if (dispatch.subscription !== first.subscription) {
          one.origin = "local_validation";
          one.status = 409;
          one.error = "subscription_mismatch";
          return finish(egressError({ status: 409, error: "subscription_mismatch" }, provider));
        }
        endpoint = modelEndpoint(this.env, first.subscription);
        subscriptionHeaders(headers, first.subscription);
      }
      one.origin = "transport";
      begin("transport");
      const encoded: Encoded = { sent_bytes: 0, gzip_sent: false, gzip_rejected: false };
      const sent = (trace: AttemptDiagnostic) => { trace.body_bytes = body.byteLength; trace.sent_bytes = encoded.sent_bytes; trace.gzip = encoded.gzip_sent; trace.gzip_rejected = encoded.gzip_rejected; };
      let response = await sendModel(this.env, provider, endpoint, headers, first, body, request.signal, encoded);
      end();
      sent(one);
      classify(one, response);
      if (response.status === 401) {
        metrics.model_unauthorized++;
        begin("discard");
        await response.body?.cancel();
        end();
        const two = start(2);
        begin("authority");
        const next = await resolveModel(this.env, authority, provider, first.generation, outcome => { two.credential = outcome; });
        end();
        if (!next.ok) {
          two.status = statusOf(next.status);
          two.error = errorOf(next.error);
          return finish(egressError(next, provider));
        }
        if (next.generation === first.generation) {
          two.status = 401;
          two.error = "credential_unchanged";
          if (provider === "codex") return finish(codexError(401, { type: "authentication_error", message: "Provider request failed" }));
          return finish(providerError(401, "authentication_error", "Provider request failed"));
        }
        rotated = true;
        metrics.model_recoveries++;
        two.origin = "transport";
        begin("transport");
        response = await sendModel(this.env, provider, endpoint, headers, next, body, request.signal, encoded);
        end();
        sent(two);
        classify(two, response);
        if (response.status === 401) models.evict(cacheKey(authority, provider), next.generation);
      }
      begin("projection");
      const projected = await projectModel(response, provider);
      if (response.ok) open = false;
      else end();
      return finish(projected);
    } catch (error) {
      const failure = denial(error);
      const trace = active as AttemptDiagnostic | null;
      const failed = phase as AttemptPhase;
      if (trace) {
        end();
        if (failed === "projection") trace.origin = "projection";
        else if (failed === "discard") trace.origin = "transport";
        trace.status = statusOf(failure.status);
        trace.error = errorOf(failure.error);
      } else {
        if (request_ms < 0) request_ms = elapsed(entered);
        const local = attemptDiagnostic(1, "local_validation", transport);
        local.status = statusOf(failure.status);
        local.error = errorOf(failure.error);
        attempts.push(local);
      }
      return finish(egressError(failure, provider));
    }
  }
}

export default {
  fetch(): Response {
    return json({ error: "not_found" }, 404);
  },
} satisfies ExportedHandler<EgressEnv>;

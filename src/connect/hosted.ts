import { EXECUTOR_ID, json, type ModelProvider, type SessionDescriptor } from "../contracts";
import { authorize, ConnectError, descriptorOf, IDENTIFIER, positiveInteger, readJson, relay, sessionFetch, SCOPES, text, threadId, type ConnectEnv, type Delegation } from "./common";

const EVENT_PAGE_MAX = 500;
const PROMPT_LIMIT = 512 * 1024;
const STREAM_BUFFER_LIMIT = 4 << 20;
const SERVICE_METHODS = ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"];
const HEADER_NAME = /^[!#$%&'*+.^_`|~0-9A-Za-z-]{1,128}$/;
const HEADER_VALUE = /^[\t\x20-\x7E\x80-\xFF]{0,8192}$/;

export type TurnInput = { request_id: string; prompt: string; provider?: ModelProvider; executor?: string };
export type EventCursor = { after: number; limit?: number };
export type ServiceInput = { connection_id: string; app_id?: string; method?: string; path?: string; headers?: Record<string, string>; body?: string; kind?: "connector" | "vault" };

export function turnInput(value: any): TurnInput {
  if (!value || typeof value !== "object") throw new ConnectError(400, "invalid_request", "expected a JSON object");
  if (typeof value.prompt !== "string" || !value.prompt.trim() || value.prompt.length > PROMPT_LIMIT) throw new ConnectError(400, "invalid_request", `prompt must be a nonempty string of at most ${PROMPT_LIMIT} characters`);
  if (value.provider !== undefined && value.provider !== "claude" && value.provider !== "codex") throw new ConnectError(400, "invalid_request", "provider must be claude or codex");
  if (value.executor !== undefined && (typeof value.executor !== "string" || !EXECUTOR_ID.test(value.executor))) throw new ConnectError(400, "invalid_request", "invalid executor");
  return { request_id: text(value.request_id, "request_id"), prompt: value.prompt, provider: value.provider, executor: value.executor };
}

export function cursor(after: string | null, limit: string | null): EventCursor {
  const parsedAfter = after === null || after === "" ? 0 : Number(after);
  if (!Number.isSafeInteger(parsedAfter) || parsedAfter < 0) throw new ConnectError(400, "invalid_request", "after must be a nonnegative integer");
  if (limit === null || limit === "") return { after: parsedAfter };
  const parsedLimit = Number(limit);
  if (!Number.isSafeInteger(parsedLimit) || parsedLimit < 1 || parsedLimit > EVENT_PAGE_MAX) throw new ConnectError(400, "invalid_request", `limit must be between 1 and ${EVENT_PAGE_MAX}`);
  return { after: parsedAfter, limit: parsedLimit };
}

function eventPath(page: EventCursor): string {
  return `/internal/events?after=${page.after}${page.limit === undefined ? "" : `&limit=${page.limit}`}`;
}

export async function threadDescriptor(env: ConnectEnv, delegated: Delegation, scope: string, thread?: string): Promise<SessionDescriptor> {
  const result = await authorize(env, delegated, thread === undefined ? { required_scope: scope, create_thread: true } : { required_scope: scope, thread_id: thread });
  return descriptorOf(result, delegated.account, thread);
}

export async function createThread(env: ConnectEnv, delegated: Delegation): Promise<Response> {
  const descriptor = await threadDescriptor(env, delegated, SCOPES.write);
  return json({ thread_id: descriptor.thread_id }, 201);
}

export async function submitTurn(env: ConnectEnv, delegated: Delegation, thread: string | undefined, input: TurnInput): Promise<{ descriptor: SessionDescriptor; response: Response }> {
  const descriptor = await threadDescriptor(env, delegated, SCOPES.write, thread);
  const response = await sessionFetch(env, descriptor, "/internal/turns", "POST", input);
  return { descriptor, response };
}

export async function turnStatus(env: ConnectEnv, delegated: Delegation, thread: string, request: string): Promise<Response> {
  const descriptor = await threadDescriptor(env, delegated, SCOPES.read, thread);
  return sessionFetch(env, descriptor, `/internal/turns/${encodeURIComponent(request)}`, "GET");
}

export async function cancelTurn(env: ConnectEnv, delegated: Delegation, thread: string, request: string): Promise<Response> {
  const descriptor = await threadDescriptor(env, delegated, SCOPES.write, thread);
  return sessionFetch(env, descriptor, `/internal/cancel/${encodeURIComponent(request)}`, "POST");
}

export async function eventPage(env: ConnectEnv, delegated: Delegation, thread: string, page: EventCursor): Promise<Response> {
  const descriptor = await threadDescriptor(env, delegated, SCOPES.read, thread);
  return sessionFetch(env, descriptor, eventPath(page), "GET");
}

export async function eventStream(env: ConnectEnv, delegated: Delegation, thread: string, page: EventCursor): Promise<Response> {
  const interval = positiveInteger(env.CONNECT_STREAM_REVALIDATE_MS, "CONNECT_STREAM_REVALIDATE_MS");
  const descriptor = await threadDescriptor(env, delegated, SCOPES.read, thread);
  let validated = Date.now();
  const upstream = await sessionFetch(env, descriptor, eventPath(page), "GET", undefined, "text/event-stream");
  if (!upstream.ok || !upstream.body || !(upstream.headers.get("content-type") ?? "").startsWith("text/event-stream")) return upstream;
  const reader = upstream.body.getReader();
  const decoder = new TextDecoder();
  const encoder = new TextEncoder();
  let buffer = "";
  let pending: Promise<ReadableStreamReadResult<Uint8Array>> | undefined;
  let closed = false;
  const finish = (controller: ReadableStreamDefaultController<Uint8Array>, event?: string, data?: unknown) => {
    if (closed) return;
    closed = true;
    reader.cancel().catch(() => {});
    if (event) controller.enqueue(encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`));
    controller.close();
  };
  const body = new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        while (!closed) {
          const remaining = interval - (Date.now() - validated);
          if (remaining <= 0) {
            try {
              await threadDescriptor(env, delegated, SCOPES.read, thread);
              validated = Date.now();
            } catch (error) {
              const failed = error instanceof ConnectError ? error : new ConnectError(503, "temporarily_unavailable", "authority unavailable");
              finish(controller, failed.status >= 500 ? "error" : "revoked", { error: failed.code, error_description: failed.message });
              return;
            }
            continue;
          }
          pending ??= reader.read();
          let timer: number | null = null;
          const outcome = await Promise.race([pending, new Promise<null>(resolve => { timer = setTimeout(() => resolve(null), remaining); })]);
          clearTimeout(timer);
          if (closed) return;
          if (outcome === null) continue;
          pending = undefined;
          if (outcome.done) {
            finish(controller);
            return;
          }
          buffer += decoder.decode(outcome.value, { stream: true }).replace(/\r\n?/g, "\n");
          const boundary = buffer.lastIndexOf("\n\n");
          if (buffer.length > STREAM_BUFFER_LIMIT) {
            finish(controller, "error", { error: "server_error", error_description: "event exceeds stream limit" });
            return;
          }
          if (boundary < 0) continue;
          controller.enqueue(encoder.encode(buffer.slice(0, boundary + 2)));
          buffer = buffer.slice(boundary + 2);
          return;
        }
      } catch {
        finish(controller, "error", { error: "server_error", error_description: "stream failed" });
      }
    },
    cancel() {
      closed = true;
      reader.cancel().catch(() => {});
    },
  });
  return new Response(body, { headers: { "content-type": "text/event-stream", "cache-control": "no-store" } });
}
export function serviceInput(value: any, connection?: string): ServiceInput {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new ConnectError(400, "invalid_request", "expected a JSON object");
  const connection_id = connection ?? value.connection_id;
  if (typeof connection_id !== "string" || !IDENTIFIER.test(connection_id)) throw new ConnectError(400, "invalid_request", "invalid connection_id");
  const input: ServiceInput = { connection_id };
  if (value.app_id !== undefined) {
    if (typeof value.app_id !== "string" || !IDENTIFIER.test(value.app_id)) throw new ConnectError(400, "invalid_request", "invalid app_id");
    input.app_id = value.app_id;
  }
  if (value.method !== undefined) {
    if (typeof value.method !== "string" || !SERVICE_METHODS.includes(value.method.toUpperCase())) throw new ConnectError(400, "invalid_request", "unsupported method");
    input.method = value.method.toUpperCase();
  }
  if (value.path !== undefined) {
    if (typeof value.path !== "string" || !value.path.startsWith("/") || value.path.startsWith("//") || value.path.length > 2048 || /[\x00-\x20\x7F\\#]/.test(value.path)) throw new ConnectError(400, "invalid_request", "invalid path");
    input.path = value.path;
  }
  if (value.headers !== undefined) {
    if (!value.headers || typeof value.headers !== "object" || Array.isArray(value.headers)) throw new ConnectError(400, "invalid_request", "headers must be an object");
    const entries = Object.entries(value.headers);
    if (entries.length > 64 || !entries.every(([name, item]) => HEADER_NAME.test(name) && typeof item === "string" && HEADER_VALUE.test(item))) throw new ConnectError(400, "invalid_request", "invalid headers");
    input.headers = Object.fromEntries(entries) as Record<string, string>;
  }
  if (value.body !== undefined) {
    if (typeof value.body !== "string") throw new ConnectError(400, "invalid_request", "body must be a string");
    input.body = value.body;
  }
  if (value.kind !== undefined) {
    if (value.kind !== "connector" && value.kind !== "vault") throw new ConnectError(400, "invalid_request", "kind must be connector or vault");
    input.kind = value.kind;
  }
  return input;
}

export async function serviceCall(env: ConnectEnv, delegated: Delegation, input: ServiceInput): Promise<Response> {
  let response: Response;
  try {
    response = await env.SERVICE_EGRESS.fetch(new Request("https://service-egress.internal/", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ account_id: delegated.account, token: delegated.token, origin: delegated.origin, ...input }),
    }));
  } catch {
    throw new ConnectError(503, "temporarily_unavailable", "service egress unavailable");
  }
  return relay(response);
}

export async function handleThreads(request: Request, env: ConnectEnv, delegated: Delegation, parts: string[], url: URL): Promise<Response> {
  if (parts.length === 0) {
    if (request.method !== "POST") return methodNotAllowed("POST");
    return createThread(env, delegated);
  }
  const thread = threadId(parts[0]);
  if (parts.length === 2 && parts[1] === "turns") {
    if (request.method !== "POST") return methodNotAllowed("POST");
    const { response } = await submitTurn(env, delegated, thread, turnInput(await readJson(request)));
    return response;
  }
  if (parts.length === 3 && parts[1] === "turns") {
    if (request.method !== "GET") return methodNotAllowed("GET");
    return turnStatus(env, delegated, thread, text(parts[2], "request_id"));
  }
  if (parts.length === 4 && parts[1] === "turns" && parts[3] === "cancel") {
    if (request.method !== "POST") return methodNotAllowed("POST");
    return cancelTurn(env, delegated, thread, text(parts[2], "request_id"));
  }
  if (parts.length === 2 && parts[1] === "events") {
    if (request.method !== "GET") return methodNotAllowed("GET");
    const page = cursor(url.searchParams.get("after") ?? request.headers.get("last-event-id"), url.searchParams.get("limit"));
    return (request.headers.get("accept") ?? "").includes("text/event-stream") ? eventStream(env, delegated, thread, page) : eventPage(env, delegated, thread, page);
  }
  throw new ConnectError(404, "not_found", "route not found");
}

export function methodNotAllowed(allow: string): Response {
  return json({ error: "method_not_allowed", error_description: "method not allowed" }, 405, { allow });
}

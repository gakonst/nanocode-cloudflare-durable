import { EXECUTOR_ID, json } from "../contracts";
import { authorize, ConnectError, delegation, readText, SCOPES, text, threadId, withCors, type ConnectEnv, type Delegation } from "./common";
import { cancelTurn, cursor, eventPage, serviceCall, serviceInput, submitTurn, turnInput, turnStatus } from "./hosted";

const VERSIONS = ["2025-11-25", "2025-06-18", "2025-03-26"];

type Message = { jsonrpc: "2.0"; id?: string | number | null; method?: string; params?: any };

const TOOLS = {
  "agent.turn": {
    scope: SCOPES.write,
    description: "Submit a prompt to a hosted agent thread. Omit thread_id to create a thread. Returns the queued or saved turn.",
    inputSchema: { type: "object", properties: { thread_id: { type: "string", format: "uuid" }, request_id: { type: "string", minLength: 1, maxLength: 256 }, prompt: { type: "string", minLength: 1 }, provider: { type: "string", enum: ["claude", "codex"] }, executor: { type: "string", pattern: EXECUTOR_ID.source } }, required: ["request_id", "prompt"], additionalProperties: false },
  },
  "agent.status": {
    scope: SCOPES.read,
    description: "Read the status or saved result of a turn in a hosted agent thread.",
    inputSchema: { type: "object", properties: { thread_id: { type: "string", format: "uuid" }, request_id: { type: "string", minLength: 1, maxLength: 256 } }, required: ["thread_id", "request_id"], additionalProperties: false },
  },
  "agent.events": {
    scope: SCOPES.read,
    description: "Read one bounded page of events from a hosted agent thread after a cursor.",
    inputSchema: { type: "object", properties: { thread_id: { type: "string", format: "uuid" }, after: { type: "integer", minimum: 0 }, limit: { type: "integer", minimum: 1, maximum: 500 } }, required: ["thread_id"], additionalProperties: false },
  },
  "agent.cancel": {
    scope: SCOPES.write,
    description: "Request cancellation of a queued or running turn in a hosted agent thread.",
    inputSchema: { type: "object", properties: { thread_id: { type: "string", format: "uuid" }, request_id: { type: "string", minLength: 1, maxLength: 256 } }, required: ["thread_id", "request_id"], additionalProperties: false },
  },
  "connector.call": {
    scope: null,
    description: "Call a connection approved for this grant through the private service egress. No hosted agent is created.",
    inputSchema: {
      type: "object",
      properties: {
        connection_id: { type: "string" },
        method: { type: "string", enum: ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"] },
        path: { type: "string" },
        headers: { type: "object", additionalProperties: { type: "string" } },
        body: { type: "string" },
        kind: { type: "string", enum: ["connector", "vault"] },
      },
      required: ["connection_id"],
      additionalProperties: false,
    },
  },
} as const;

type ToolName = keyof typeof TOOLS;

function reply(id: Message["id"], result: unknown): Response {
  return json({ jsonrpc: "2.0", id, result });
}

function rpcError(id: Message["id"], code: number, message: string, status = 200): Response {
  return json({ jsonrpc: "2.0", id: id ?? null, error: { code, message } }, status);
}

async function toolResult(response: Response, extra?: Record<string, unknown>) {
  const body = await response.text();
  let value: unknown;
  try {
    value = JSON.parse(body);
  } catch {
    value = undefined;
  }
  const structured = value && typeof value === "object" && !Array.isArray(value) ? { ...extra, status: response.status, ...(value as object) } : { ...extra, status: response.status, body };
  return { content: [{ type: "text", text: JSON.stringify(structured) }], structuredContent: structured, isError: !response.ok };
}

function toolFailure(error: ConnectError) {
  const structured = { error: error.code, error_description: error.message };
  return { content: [{ type: "text", text: JSON.stringify(structured) }], structuredContent: structured, isError: true };
}

function grantView(result: Record<string, unknown>): { scopes: string[]; connections: string[] } {
  const grant = (result.grant && typeof result.grant === "object" ? result.grant : result) as Record<string, unknown>;
  const list = (value: unknown) => Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : [];
  return { scopes: list(grant.scopes), connections: list(grant.connections) };
}

async function callTool(env: ConnectEnv, delegated: Delegation, name: ToolName, args: any) {
  if (!args || typeof args !== "object" || Array.isArray(args)) throw new ConnectError(400, "invalid_request", "arguments must be an object");
  if (name === "agent.turn") {
    const thread = args.thread_id === undefined ? undefined : threadId(args.thread_id);
    const { descriptor, response } = await submitTurn(env, delegated, thread, turnInput(args));
    return toolResult(response, { thread_id: descriptor.thread_id });
  }
  if (name === "agent.status") return toolResult(await turnStatus(env, delegated, threadId(args.thread_id), text(args.request_id, "request_id")));
  if (name === "agent.cancel") return toolResult(await cancelTurn(env, delegated, threadId(args.thread_id), text(args.request_id, "request_id")));
  if (name === "agent.events") {
    const page = cursor(args.after === undefined ? null : String(args.after), args.limit === undefined ? null : String(args.limit));
    return toolResult(await eventPage(env, delegated, threadId(args.thread_id), page));
  }
  return toolResult(await serviceCall(env, delegated, serviceInput(args)));
}

async function admit(env: ConnectEnv, delegated: Delegation, id: Message["id"]): Promise<Record<string, unknown> | Response> {
  try {
    return await authorize(env, delegated, {});
  } catch (error) {
    if (!(error instanceof ConnectError) || error.status === 401) throw error;
    return rpcError(id, error.status >= 500 ? -32603 : -32001, error.status >= 500 ? "authorization temporarily unavailable" : `request denied: ${error.code}`, error.status);
  }
}

export async function handleMcp(request: Request, env: ConnectEnv, ownerAuthorized: boolean): Promise<Response> {
  if (request.method !== "POST") return json({ error: "method_not_allowed", error_description: "this server does not offer a standalone event stream" }, 405, { allow: "POST" });
  const version = request.headers.get("mcp-protocol-version");
  if (version !== null && !VERSIONS.includes(version)) return rpcError(null, -32600, "unsupported MCP protocol version", 400);
  const delegated = delegation(request, env, ownerAuthorized);
  let message: Message;
  try {
    message = JSON.parse(await readText(request));
  } catch (error) {
    if (error instanceof ConnectError) throw error;
    return rpcError(null, -32700, "parse error", 400);
  }
  if (Array.isArray(message)) return rpcError(null, -32600, "batch requests are not supported", 400);
  if (!message || typeof message !== "object" || message.jsonrpc !== "2.0") return rpcError(null, -32600, "invalid request", 400);
  const hasId = "id" in message && (typeof message.id === "string" || (typeof message.id === "number" && Number.isSafeInteger(message.id)));
  if ("id" in message && !hasId) return rpcError(null, -32600, "invalid request id", 400);
  const id = hasId ? message.id : null;
  const notification = typeof message.method === "string" ? !hasId : hasId && ("result" in message || "error" in message);
  if (typeof message.method !== "string" && !notification) return rpcError(id, -32600, "invalid request", 400);
  if (notification) {
    const admitted = await admit(env, delegated, id);
    return admitted instanceof Response ? admitted : withCors(new Response(null, { status: 202 }), delegated.cors);
  }
  const params = message.params ?? {};
  if (typeof params !== "object" || Array.isArray(params)) return rpcError(id, -32602, "params must be an object", 400);
  if (message.method === "tools/call" && typeof params.name === "string" && Object.hasOwn(TOOLS, params.name)) {
    try {
      return withCors(reply(id, await callTool(env, delegated, params.name as ToolName, params.arguments ?? {})), delegated.cors);
    } catch (error) {
      const failed = error instanceof ConnectError ? error : new ConnectError(500, "server_error", "tool call failed");
      if (failed.status === 401) throw failed;
      return reply(id, toolFailure(failed));
    }
  }
  const admitted = await admit(env, delegated, id);
  if (admitted instanceof Response) return admitted;
  if (message.method === "tools/call") return withCors(rpcError(id, -32602, "unknown tool"), delegated.cors);
  if (message.method === "initialize") {
    const requested = typeof params.protocolVersion === "string" && VERSIONS.includes(params.protocolVersion) ? params.protocolVersion : VERSIONS[0];
    return withCors(reply(id, { protocolVersion: requested, capabilities: { tools: { listChanged: false } }, serverInfo: { name: "nanocode-connect", version: "1.0.0" } }), delegated.cors);
  }
  if (message.method === "ping") return withCors(reply(id, {}), delegated.cors);
  if (message.method === "tools/list") {
    const grant = grantView(admitted);
    const tools = (Object.keys(TOOLS) as ToolName[])
      .filter(name => TOOLS[name].scope === null ? grant.connections.length > 0 : grant.scopes.includes(TOOLS[name].scope!))
      .map(name => ({ name, description: TOOLS[name].description, inputSchema: TOOLS[name].inputSchema }));
    return withCors(reply(id, { tools }), delegated.cors);
  }
  return withCors(rpcError(id, -32601, "method not found"), delegated.cors);
}

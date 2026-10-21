import type { ModelDispatch, ModelProvider, PrivateBinding } from "../src/index";

const HEADERS: Record<ModelProvider, RegExp> = {
  claude: /^(accept|content-type|anthropic-version|anthropic-beta)$/,
  codex: /^(accept|content-type)$/,
};
const RESPONSE_HEADER = /^(content-type|request-id|x-request-id|retry-after|x-should-retry|anthropic-ratelimit-[a-z0-9-]+|x-ratelimit-[a-z0-9-]+|openai-model|openai-processing-ms|cf-aig-[a-z0-9-]+)$/;

function json(value: unknown, status = 200): Response {
  return Response.json(value, { status, headers: { "cache-control": "no-store" } });
}

function shape(query: unknown): { input_items: number; continued: boolean } {
  const body = typeof query === "object" && query !== null ? query as { input?: unknown; messages?: unknown; previous_response_id?: unknown } : {};
  const items = Array.isArray(body.input) ? body.input : Array.isArray(body.messages) ? body.messages : [];
  return { input_items: items.length, continued: typeof body.previous_response_id === "string" };
}

function modelError(provider: ModelProvider, status: number, type: string, message: string): Response {
  if (provider === "codex") return json({ error: { type, message } }, status);
  return json({ type: "error", error: { type, message } }, status);
}

function catalogModel(model: string): string {
  return `anthropic/${model.replace(/-(\d+)-(\d+)$/, "-$1.$2")}`;
}

async function model(ai: Ai, gateway: string, request: Request): Promise<Response> {
  let dispatch: ModelDispatch;
  try { dispatch = JSON.parse(request.headers.get("x-nanocode-model") ?? ""); } catch { return modelError("claude", 400, "invalid_request_error", "invalid model dispatch"); }
  const provider = dispatch.provider ?? "claude";
  const allowed = HEADERS[provider];
  if (!allowed) return modelError("claude", 400, "invalid_request_error", "unsupported model provider");
  if (dispatch.subscription) return modelError(provider, 409, "invalid_request_error", "AI Gateway uses API-key authentication; set MODEL_AUTH to api_key");
  const headers: Record<string, string> = {};
  for (const [name, value] of dispatch.headers) if (allowed.test(name.toLowerCase())) headers[name.toLowerCase()] = value;
  const body = await request.arrayBuffer();
  let query: Record<string, unknown>;
  try { query = JSON.parse(new TextDecoder().decode(body)); } catch { return modelError(provider, 400, "invalid_request_error", "model request body is not JSON"); }
  let upstream: Response;
  if (provider === "claude") {
    if (typeof query.model !== "string") return modelError(provider, 400, "invalid_request_error", "model request names no model");
    const catalog = catalogModel(query.model);
    upstream = await ai.run(catalog, { ...query, model: catalog }, { gateway: { id: gateway }, returnRawResponse: true, extraHeaders: headers, signal: request.signal }) as unknown as Response;
  } else {
    const opened = ai.gateway(gateway);
    try {
      upstream = await opened.run({ provider: "openai", endpoint: "responses", headers, query }, { signal: request.signal });
    } finally {
      (opened as Partial<Disposable>)[Symbol.dispose]?.();
    }
  }
  const forwarded = new Headers({ "cache-control": "no-store" });
  for (const [name, value] of upstream.headers) if (RESPONSE_HEADER.test(name)) forwarded.set(name, value);
  forwarded.set("x-nanocode-model-diagnostic", JSON.stringify({ provider, transport: provider === "claude" ? "unified" : "gateway", status: upstream.status, body_bytes: body.byteLength, ...shape(query) }));
  return new Response(upstream.body, { status: upstream.status, headers: forwarded });
}

export function aiGateway(ai: Ai, gateway: string): PrivateBinding {
  return {
    async fetch(request: Request): Promise<Response> {
      const { pathname } = new URL(request.url);
      if (pathname === "/model" && request.method === "POST") return model(ai, gateway, request);
      if (pathname === "/socket") return json({ error: { type: "invalid_request_error", message: "AI Gateway carries model requests over HTTPS only" } }, 426);
      if (pathname === "/remote") return json({ error: "this deployment has no remote executors; attach a hand instead" }, 501);
      if (pathname === "/metrics") return json({ ok: true, egress: "ai-gateway" });
      return json({ error: "not_found" }, 404);
    },
  };
}

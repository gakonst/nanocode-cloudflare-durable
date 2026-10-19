import type { ModelDispatch, ModelProvider, PrivateBinding } from "../src/index";

const ROUTES: Record<ModelProvider, { provider: string; endpoint: string; headers: RegExp }> = {
  claude: { provider: "anthropic", endpoint: "v1/messages", headers: /^(accept|content-type|anthropic-version|anthropic-beta)$/ },
  codex: { provider: "openai", endpoint: "responses", headers: /^(accept|content-type)$/ },
};
const RESPONSE_HEADER = /^(content-type|request-id|x-request-id|retry-after|x-should-retry|anthropic-ratelimit-[a-z0-9-]+|x-ratelimit-[a-z0-9-]+|openai-model|openai-processing-ms|cf-aig-[a-z0-9-]+)$/;

function json(value: unknown, status = 200): Response {
  return Response.json(value, { status, headers: { "cache-control": "no-store" } });
}

function modelError(provider: ModelProvider, status: number, type: string, message: string): Response {
  if (provider === "codex") return json({ error: { type, message } }, status);
  return json({ type: "error", error: { type, message } }, status);
}

async function model(open: () => AiGateway, request: Request): Promise<Response> {
  let dispatch: ModelDispatch;
  try { dispatch = JSON.parse(request.headers.get("x-nanocode-model") ?? ""); } catch { return modelError("claude", 400, "invalid_request_error", "invalid model dispatch"); }
  const provider = dispatch.provider ?? "claude";
  const route = ROUTES[provider];
  if (!route) return modelError("claude", 400, "invalid_request_error", "unsupported model provider");
  if (dispatch.subscription) return modelError(provider, 409, "invalid_request_error", "AI Gateway uses API-key authentication; set MODEL_AUTH to api_key");
  const headers: Record<string, string> = {};
  for (const [name, value] of dispatch.headers) if (route.headers.test(name.toLowerCase())) headers[name.toLowerCase()] = value;
  let query: unknown;
  try { query = JSON.parse(await request.text()); } catch { return modelError(provider, 400, "invalid_request_error", "model request body is not JSON"); }
  const gateway = open();
  let upstream: Response;
  try {
    upstream = await gateway.run({ provider: route.provider, endpoint: route.endpoint, headers, query }, { signal: request.signal });
  } finally {
    (gateway as Partial<Disposable>)[Symbol.dispose]?.();
  }
  const forwarded = new Headers({ "cache-control": "no-store" });
  for (const [name, value] of upstream.headers) if (RESPONSE_HEADER.test(name)) forwarded.set(name, value);
  return new Response(upstream.body, { status: upstream.status, headers: forwarded });
}

export function aiGateway(open: () => AiGateway): PrivateBinding {
  return {
    async fetch(request: Request): Promise<Response> {
      const { pathname } = new URL(request.url);
      if (pathname === "/model" && request.method === "POST") return model(open, request);
      if (pathname === "/socket") return json({ error: { type: "invalid_request_error", message: "AI Gateway carries model requests over HTTPS only" } }, 426);
      if (pathname === "/remote") return json({ error: "this deployment has no remote executors; attach a hand instead" }, 501);
      if (pathname === "/metrics") return json({ ok: true, egress: "ai-gateway" });
      return json({ error: "not_found" }, 404);
    },
  };
}

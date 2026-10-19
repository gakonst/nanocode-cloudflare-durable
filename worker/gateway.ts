import type { ModelDispatch, PrivateBinding } from "../src/index";

const REQUEST_HEADER = /^(accept|content-type|anthropic-version|anthropic-beta)$/;
const RESPONSE_HEADER = /^(content-type|request-id|retry-after|x-should-retry|anthropic-ratelimit-[a-z0-9-]+|cf-aig-[a-z0-9-]+)$/;

function json(value: unknown, status = 200): Response {
  return Response.json(value, { status, headers: { "cache-control": "no-store" } });
}

function claudeError(status: number, type: string, message: string): Response {
  return json({ type: "error", error: { type, message } }, status);
}

async function model(open: () => AiGateway, request: Request): Promise<Response> {
  let dispatch: ModelDispatch;
  try { dispatch = JSON.parse(request.headers.get("x-nanocode-model") ?? ""); } catch { return claudeError(400, "invalid_request_error", "invalid model dispatch"); }
  if ((dispatch.provider ?? "claude") !== "claude") return json({ error: { type: "invalid_request_error", code: "unsupported_provider", message: "This deployment routes only Claude through AI Gateway" } }, 400);
  if (dispatch.subscription) return claudeError(409, "invalid_request_error", "AI Gateway uses API-key authentication; set MODEL_AUTH to api_key");
  const headers: Record<string, string> = {};
  for (const [name, value] of dispatch.headers) if (REQUEST_HEADER.test(name.toLowerCase())) headers[name.toLowerCase()] = value;
  let query: unknown;
  try { query = JSON.parse(await request.text()); } catch { return claudeError(400, "invalid_request_error", "model request body is not JSON"); }
  const gateway = open();
  let upstream: Response;
  try {
    upstream = await gateway.run({ provider: "anthropic", endpoint: "v1/messages", headers, query }, { signal: request.signal });
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
      if (pathname === "/socket") return json({ error: { type: "invalid_request_error", message: "AI Gateway carries Claude over HTTPS only" } }, 426);
      if (pathname === "/remote") return json({ error: "this deployment has no remote executors; attach a hand instead" }, 501);
      if (pathname === "/metrics") return json({ ok: true, egress: "ai-gateway" });
      return json({ error: "not_found" }, 404);
    },
  };
}

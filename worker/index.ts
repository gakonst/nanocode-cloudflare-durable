import { createNanocode, type NanocodeEnv } from "../src/index";
import { aiGateway } from "./gateway";
import { client, PublicLimit } from "./limit";
import { charge } from "./tools";

interface Env extends NanocodeEnv {
  AI: Ai;
  AI_GATEWAY_ID: string;
  ACCESS_TOKEN_SHA256: string;
  CHAT_PUBLIC: string;
  PUBLIC_LIMITS: DurableObjectNamespace<PublicLimit>;
}

const DIGEST = /^[0-9a-f]{64}$/;
const ACCOUNT = "owner";
const PUBLIC_ACCOUNT = "public";
const PUBLIC_PROMPT_CHARACTERS = 2000;
const PUBLIC_ROUTE = /^\/sessions\/[^/]+\/(?:turns(?:\/[^/]+(?:\/cancel)?)?|events|crash)$/;
const PUBLIC_MODELS: Record<string, readonly string[]> = {
  "claude-haiku-5-5": ["none", "low", "medium", "high", "xhigh", "max"],
};

async function owner(request: Request, env: Env): Promise<boolean> {
  const header = request.headers.get("authorization") ?? "";
  if (!DIGEST.test(env.ACCESS_TOKEN_SHA256) || !header.startsWith("Bearer ")) return false;
  const actual = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(header.slice("Bearer ".length)));
  const expected = Uint8Array.from(env.ACCESS_TOKEN_SHA256.match(/../g)!, byte => parseInt(byte, 16));
  return crypto.subtle.timingSafeEqual(actual, expected);
}

async function authenticate(request: Request, env: Env): Promise<string | null> {
  if (await owner(request, env)) return ACCOUNT;
  return env.CHAT_PUBLIC === "1" ? PUBLIC_ACCOUNT : null;
}

function refuse(error: string, status: number): Response {
  return Response.json({ error }, { status });
}

async function admit(request: Request, env: Env): Promise<Response | undefined> {
  const { pathname } = new URL(request.url);
  if (pathname === "/health") return;
  if (!PUBLIC_ROUTE.test(pathname)) return refuse("this route needs the access token", 403);
  if (!(await env.PUBLIC_LIMITS.get(env.PUBLIC_LIMITS.idFromName(client(request.headers.get("cf-connecting-ip")!))).admit())) return refuse("too many requests, try again in a minute", 429);
  if (request.method !== "POST" || !pathname.endsWith("/turns")) return;
  const body = await request.clone().json().catch(() => undefined) as { model?: unknown; prompt?: unknown; provider?: unknown; thinking?: unknown } | undefined;
  if (typeof body?.prompt === "string" && body.prompt.length > PUBLIC_PROMPT_CHARACTERS) return refuse(`prompts are limited to ${PUBLIC_PROMPT_CHARACTERS} characters`, 413);
  if (body?.provider !== undefined && body.provider !== "claude") return refuse("this deployment serves Claude only", 403);
  const model = String(body?.model ?? env.CLAUDE_MODEL ?? "claude-opus-5-5");
  const efforts = PUBLIC_MODELS[model];
  if (!efforts) return refuse(`this deployment serves ${Object.keys(PUBLIC_MODELS).join(", ")}`, 403);
  if (body?.thinking !== undefined && !efforts.includes(String(body.thinking))) return refuse(`${model} takes ${efforts.join(", ")}`, 403);
}

const nanocode = createNanocode<Env>({
  authenticate,
  egress: env => aiGateway(env.AI, env.AI_GATEWAY_ID),
  tools: charge,
  chaos: true,
});

export const { AgentSession } = nanocode;
export { PublicLimit };

export default {
  async fetch(request, env) {
    const refused = env.CHAT_PUBLIC === "1" && !(await owner(request, env)) ? await admit(request, env) : undefined;
    return refused ?? nanocode.fetch(request, env);
  },
} satisfies ExportedHandler<Env>;

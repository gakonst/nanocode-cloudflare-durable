import { aiGateway, client, createNanocode, type NanocodeEnv, PublicLimit } from "nanocode-ts/cloudflare";
import { PUBLIC_MODELS } from "./models";
import { charge } from "./tools";

interface Env extends NanocodeEnv {
  AI: Ai;
  AI_GATEWAY_ID: string;
  OPENAI_SERVICE_TIER: string;
  ACCESS_TOKEN_SHA256: string;
  CHAT_PUBLIC: string;
  PUBLIC_LIMITS: DurableObjectNamespace<PublicLimit>;
}

const DIGEST = /^[0-9a-f]{64}$/;
const ACCOUNT = "owner";
const PUBLIC_ACCOUNT = "public";
const PUBLIC_PROMPT_CHARACTERS = 2000;
const PUBLIC_ROUTE = /^\/sessions\/[^/]+\/(?:turns(?:\/[^/]+(?:\/cancel)?)?|events|crash|warm|hands(?:\/browser)?)$/;

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

const DEVICE = "https://wallet.tempo.xyz/api/auth/device";
const ARCHIVE = /^\/api\/github\/([\w.-]+)\/([\w.-]+)\/(.+)$/;

const INSTRUCTIONS = [
  "When a browser tab is attached as the hand `browser`, your Code Mode cells run in the visitor's tab while this session stays durable in a Cloudflare Durable Object.",
  "There the cells' global `bash` works on the tab's /workspace (git clone, python3 with numpy, WASI programs, curl), `WebAssembly` runs natively, and `fetch` pays MPP services (HTTP 402) from the visitor's Tempo Wallet.",
  "Paid services are also tools in cells: `tools.mcp__mercator__*` (Mercator: about 2,800 paid APIs such as web search, scraping and market data; search and quote before you pay) and `tools.mcp__gateway__*` (the Tempo gateway: kv, bucket storage, AI and GPU compute), each call paid in MACH.",
  "If a result says tempo_wallet_required, ask the visitor to click Connect Tempo Wallet, then try again. Keep answers short.",
].join(" ");

async function relay(request: Request, pathname: string): Promise<Response> {
  const response = await fetch(DEVICE + pathname.slice("/api/device".length), { body: await request.text(), headers: { accept: "application/json", "content-type": "application/json" }, method: "POST" });
  return new Response(response.body, { headers: { "cache-control": "no-store", "content-type": response.headers.get("content-type") ?? "application/json" }, status: response.status });
}

async function archive(pathname: string): Promise<Response> {
  const match = ARCHIVE.exec(pathname);
  if (!match) return refuse("use /api/github/OWNER/REPO/REF", 404);
  const upstream = await fetch(`https://codeload.github.com/${match[1]}/${match[2]}/tar.gz/${match[3]}`, { headers: { "user-agent": "nanocode-cloudflare-durable" } });
  return new Response(upstream.body, { headers: { "cache-control": "public, max-age=300", "content-type": upstream.headers.get("content-type") ?? "application/x-gzip" }, status: upstream.status });
}


async function admit(request: Request, env: Env): Promise<Response | undefined> {
  const { pathname } = new URL(request.url);
  if (pathname === "/health") return;
  if (!PUBLIC_ROUTE.test(pathname)) return refuse("this route needs the access token", 403);
  if (!(await env.PUBLIC_LIMITS.get(env.PUBLIC_LIMITS.idFromName(client(request.headers.get("cf-connecting-ip")!))).admit())) return refuse("too many requests, try again in a minute", 429);
  if (request.method !== "POST" || !/\/(?:turns|warm)$/.test(pathname)) return;
  const body = await request.clone().json().catch(() => undefined) as { model?: unknown; prompt?: unknown; provider?: unknown; thinking?: unknown } | undefined;
  if (typeof body?.prompt === "string" && body.prompt.length > PUBLIC_PROMPT_CHARACTERS) return refuse(`prompts are limited to ${PUBLIC_PROMPT_CHARACTERS} characters`, 413);
  const provider = body?.provider ?? "claude";
  const model = String(body?.model ?? (provider === "codex" ? "gpt-6.1-sol" : env.CLAUDE_MODEL ?? "claude-opus-5-5"));
  const allowed = PUBLIC_MODELS[model];
  if (!allowed || allowed.provider !== provider) return refuse(`this deployment serves ${Object.keys(PUBLIC_MODELS).join(", ")}`, 403);
  if (body?.thinking !== undefined && !allowed.efforts.includes(String(body.thinking))) return refuse(`${model} takes ${allowed.efforts.join(", ")}`, 403);
}

const nanocode = createNanocode<Env>({
  authenticate,
  egress: env => aiGateway(env.AI, env.AI_GATEWAY_ID, env.OPENAI_SERVICE_TIER),
  tools: charge,
  chaos: true,
  instructions: INSTRUCTIONS,
});

export const { AgentSession } = nanocode;
export { PublicLimit };

export default {
  async fetch(request, env) {
    const { pathname } = new URL(request.url);
    if (request.method === "POST" && (pathname === "/api/device/register" || pathname === "/api/device/token")) return relay(request, pathname);
    if (request.method === "GET" && pathname.startsWith("/api/github/")) return archive(pathname);
    const refused = env.CHAT_PUBLIC === "1" && !(await owner(request, env)) ? await admit(request, env) : undefined;
    return refused ?? nanocode.fetch(request, env);
  },
} satisfies ExportedHandler<Env>;

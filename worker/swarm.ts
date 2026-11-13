import { App, Gateway } from "nanocode-ts/cloudflare";

interface Env extends App.Env {
  AI: Ai;
  AI_GATEWAY_ID: string;
  ACCESS_TOKEN_SHA256: string;
}

const DIGEST = /^[0-9a-f]{64}$/;

async function authenticate(request: Request, env: Env): Promise<string | null> {
  const header = request.headers.get("authorization") ?? "";
  if (!DIGEST.test(env.ACCESS_TOKEN_SHA256) || !header.startsWith("Bearer ")) return null;
  const actual = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(header.slice("Bearer ".length)));
  const expected = Uint8Array.from(env.ACCESS_TOKEN_SHA256.match(/../g)!, byte => parseInt(byte, 16));
  return crypto.subtle.timingSafeEqual(actual, expected) ? "owner" : null;
}

const nanocode = App.create<Env>({
  authenticate,
  egress: env => Gateway.create(env.AI, { id: env.AI_GATEWAY_ID }),
  maxConcurrentSubagents: 10000,
  chaos: true,
  remoteSubagents: true,
});

export const { AgentSession } = nanocode;

export default {
  fetch: (request, env) => nanocode.fetch(request, env),
} satisfies ExportedHandler<Env>;

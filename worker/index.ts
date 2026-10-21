import { createNanocode, type NanocodeEnv } from "../src/index";
import { aiGateway } from "./gateway";
import { charge } from "./tools";

interface Env extends NanocodeEnv {
  AI: Ai;
  AI_GATEWAY_ID: string;
  ACCESS_TOKEN_SHA256: string;
}

const DIGEST = /^[0-9a-f]{64}$/;
const ACCOUNT = "owner";

async function authenticate(request: Request, env: Env): Promise<string | null> {
  const header = request.headers.get("authorization") ?? "";
  if (!DIGEST.test(env.ACCESS_TOKEN_SHA256) || !header.startsWith("Bearer ")) return null;
  const actual = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(header.slice("Bearer ".length)));
  const expected = Uint8Array.from(env.ACCESS_TOKEN_SHA256.match(/../g)!, byte => parseInt(byte, 16));
  return crypto.subtle.timingSafeEqual(actual, expected) ? ACCOUNT : null;
}

const nanocode = createNanocode<Env>({
  authenticate,
  egress: env => aiGateway(() => env.AI.gateway(env.AI_GATEWAY_ID)),
  tools: charge,
  chaos: true,
});

export const { AgentSession } = nanocode;

export default { fetch: nanocode.fetch } satisfies ExportedHandler<Env>;

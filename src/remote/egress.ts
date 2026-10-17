import { WorkerEntrypoint } from "cloudflare:workers";
import { json, type SessionAuthority } from "../contracts";

export interface RemoteEnv {
  RUNNER_URL?: string;
  RUNNER_TOKEN?: string;
  ACCOUNT_ID: string;
}

const schemes: Record<string, string> = { "wss:": "https:", "https:": "https:", "ws:": "http:", "http:": "http:" };
const loopback = new Set(["localhost", "127.0.0.1", "[::1]"]);

class Rejection extends Error {
  constructor(readonly status: number, message: string) { super(message); }
}

function authorize(request: Request, env: RemoteEnv): SessionAuthority {
  const header = request.headers.get("x-nanocode-authority");
  if (!header) throw new Rejection(403, "session authority required");
  let authority: SessionAuthority;
  try { authority = JSON.parse(header); } catch { throw new Rejection(400, "invalid session authority"); }
  if (!authority || typeof authority !== "object" || typeof authority.account_id !== "string" || typeof authority.session_id !== "string") throw new Rejection(400, "invalid session authority");
  if (authority.account_id !== env.ACCOUNT_ID) throw new Rejection(403, "session account does not own the remote executor");
  if (authority.grant_id !== undefined) {
    if (typeof authority.grant_id !== "string") throw new Rejection(400, "invalid session authority");
    if (!Array.isArray(authority.connections) || !authority.connections.includes("runner")) throw new Rejection(403, "grant does not select the runner connection");
  }
  return authority;
}

function destination(value: string | undefined): string {
  if (!value) throw new Rejection(503, "remote executor is not configured");
  let url: URL;
  try { url = new URL(value); } catch { throw new Rejection(503, "remote executor destination is invalid"); }
  const scheme = schemes[url.protocol];
  if (!scheme || url.href !== `${url.protocol}//${url.host}${url.pathname}`) throw new Rejection(503, "remote executor destination must be a ws, wss, http or https URL without credentials, query or fragment");
  if (scheme === "http:" && !loopback.has(url.hostname)) throw new Rejection(503, "plaintext remote executor destinations require a loopback host");
  return `${scheme}//${url.host}${url.pathname}`;
}

export class RemoteExecutorEgress extends WorkerEntrypoint<RemoteEnv> {
  async fetch(request: Request): Promise<Response> {
    try {
      if (request.method !== "GET" || request.headers.get("upgrade")?.toLowerCase() !== "websocket") throw new Rejection(426, "websocket upgrade required");
      authorize(request, this.env);
      const target = destination(this.env.RUNNER_URL);
      const token = this.env.RUNNER_TOKEN;
      if (!token?.trim()) throw new Rejection(503, "remote executor is not configured");
      let upstream: Response;
      try {
        upstream = await fetch(target, { headers: { upgrade: "websocket", authorization: `Bearer ${token}` }, redirect: "manual" });
      } catch {
        throw new Rejection(502, "remote executor is unreachable");
      }
      const socket = upstream.webSocket;
      if (upstream.status !== 101 || !socket) {
        await upstream.body?.cancel();
        throw new Rejection(502, `remote executor rejected the connection with status ${upstream.status}`);
      }
      return new Response(null, { status: 101, webSocket: socket });
    } catch (error) {
      if (error instanceof Rejection) return json({ error: error.message }, error.status);
      throw error;
    }
  }
}

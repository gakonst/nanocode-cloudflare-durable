import { WorkerEntrypoint } from "cloudflare:workers";
import { EXECUTOR_ID, json, type SessionAuthority } from "../contracts";
import type { AccountAuthority, ExecutorAccess } from "../egress/account";
import { ACCOUNT_PATTERN, executorId, executors, executorTransport, Failure, modelAccount, type EgressEnv, type Executor, type ExecutorTransport } from "../egress/common";

export interface RemoteEnv extends Pick<EgressEnv, "ACCOUNT_ID" | "RUNNERS_JSON" | "RUNNER_URL" | "RUNNER_TOKEN"> {
  ACCOUNTS: DurableObjectNamespace<AccountAuthority>;
  [binding: `RUNNER_VPC_${string}`]: unknown;
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const INVALID = "invalid session authority";
const FORBIDDEN = "session is not authorized for the remote executor";
const UNCONFIGURED = "remote executor is not configured";

class Rejection extends Error {
  constructor(readonly status: number, message: string) { super(message); }
}

function authority(request: Request): SessionAuthority {
  const header = request.headers.get("x-nanocode-authority");
  if (!header) throw new Rejection(403, "session authority required");
  let value: any;
  try { value = JSON.parse(header); } catch { throw new Rejection(400, INVALID); }
  const invalid = !value || typeof value !== "object" || Array.isArray(value)
    || typeof value.account_id !== "string" || !ACCOUNT_PATTERN.test(value.account_id)
    || typeof value.session_id !== "string" || !value.session_id || value.session_id.length > 128
    || (value.executor_id !== undefined && (typeof value.executor_id !== "string" || !EXECUTOR_ID.test(value.executor_id)))
    || (value.grant_id !== undefined && (typeof value.grant_id !== "string" || !UUID.test(value.grant_id) || !Number.isSafeInteger(value.grant_revision)));
  if (invalid) throw new Rejection(400, INVALID);
  return { account_id: value.account_id, session_id: value.session_id, grant_id: value.grant_id, grant_revision: value.grant_revision, executor_id: value.executor_id };
}

function registry(env: RemoteEnv): Map<string, Executor> {
  try { return executors(env); } catch (error) {
    if (error instanceof Failure) throw new Rejection(503, UNCONFIGURED);
    throw error;
  }
}

async function select(request: Request, env: RemoteEnv): Promise<Executor> {
  const session = authority(request);
  let owner: string;
  try { owner = modelAccount(env); } catch { throw new Rejection(503, UNCONFIGURED); }
  if (session.account_id !== owner) throw new Rejection(403, "session account does not own the remote executor");
  const configured = registry(env);
  if (session.grant_id === undefined) {
    const executor = configured.get(executorId(session.executor_id));
    if (!executor) throw new Rejection(503, UNCONFIGURED);
    return executor;
  }
  let access: ExecutorAccess;
  try {
    access = await env.ACCOUNTS.get(env.ACCOUNTS.idFromName(owner)).executorAccess({ account_id: owner, authority: session });
  } catch {
    throw new Rejection(503, "remote executor authority is unavailable");
  }
  if (!access.ok) {
    if (access.status === 500 || access.status === 503) throw new Rejection(503, UNCONFIGURED);
    if (access.status === 400) throw new Rejection(400, INVALID);
    throw new Rejection(403, FORBIDDEN);
  }
  const executor = configured.get(access.executor_id);
  if (!executor) throw new Rejection(503, UNCONFIGURED);
  return executor;
}

export class RemoteExecutorEgress extends WorkerEntrypoint<RemoteEnv> {
  async fetch(request: Request): Promise<Response> {
    try {
      if (request.method !== "GET" || request.headers.get("upgrade")?.toLowerCase() !== "websocket") throw new Rejection(426, "websocket upgrade required");
      const executor = await select(request, this.env);
      let transport: ExecutorTransport;
      try { transport = executorTransport(this.env, executor); } catch (error) {
        if (error instanceof Failure) throw new Rejection(503, UNCONFIGURED);
        throw error;
      }
      let upstream: Response;
      try {
        upstream = await transport.fetch(new Request(executor.url, { headers: { upgrade: "websocket", authorization: `Bearer ${executor.token}` }, redirect: "manual" }));
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

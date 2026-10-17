export type SessionAuthority = {
  account_id: string;
  session_id: string;
  grant_id?: string;
  grant_revision?: number;
  app_id?: string;
  origin?: string;
  principal?: string;
  connections?: string[];
  catalog_digest?: string;
};

export type SessionDescriptor = SessionAuthority & {
  session_name: string;
  thread_id?: string;
  subscription: boolean;
};

export type ModelDispatch = {
  authority: SessionAuthority;
  endpoint: string;
  headers: [string, string][];
  body: string;
  subscription: boolean;
};

export type AuthorityCommand = {
  op: string;
  account_id: string;
  [key: string]: unknown;
};

export type AuthorityResult = {
  ok: boolean;
  status?: number;
  error?: string;
  descriptor?: SessionDescriptor;
  [key: string]: unknown;
};

export interface PrivateBinding {
  fetch(request: Request): Promise<Response>;
}

export type AgentConfiguration = {
  session_id: string;
  subscription: boolean;
  endpoint: string;
};

export type AgentInput = { request_id: string; prompt: string };

export type AgentResult = {
  ok?: { session_id: string; model: string; message: unknown };
  error?: string;
  cancelled?: boolean;
  retry?: boolean;
};

export type RuntimeOptions = {
  dispatch(request: { endpoint: string; headers: [string, string][]; body: string }): Promise<Response>;
  event(value: unknown): Promise<void>;
};

export function json(value: unknown, status = 200, headers?: HeadersInit): Response {
  const result = new Headers(headers);
  result.set("content-type", "application/json");
  result.set("cache-control", "no-store");
  return new Response(JSON.stringify(value), { status, headers: result });
}

export async function authorityCall(binding: PrivateBinding, command: AuthorityCommand): Promise<AuthorityResult> {
  const response = await binding.fetch(new Request("https://authority.internal/", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(command) }));
  return response.json<AuthorityResult>();
}

export type ModelProvider = "claude" | "codex";

export const EXECUTOR_ID = /^[a-z0-9][a-z0-9_-]{0,47}$/;
export const DEFAULT_EXECUTOR = "default";
export const LEGACY_RUNNER = "runner";

export function runnerConnection(executor: string): string {
  return `${LEGACY_RUNNER}:${executor}`;
}

export function sessionUuid(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  let time = BigInt(Date.now());
  for (let index = 5; index >= 0; index--) {
    bytes[index] = Number(time & 255n);
    time >>= 8n;
  }
  bytes[6] = (bytes[6] & 15) | 112;
  bytes[8] = (bytes[8] & 63) | 128;
  const hex = Array.from(bytes, byte => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

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
  grant_expires_at?: number;
  executor_id?: string;
};

export type SessionDescriptor = SessionAuthority & {
  provider?: ModelProvider;
  session_name: string;
  thread_id?: string;
  subscription: boolean;
  hand?: HandSelection;
};

export type HandSelection = string | string[];

export type ModelDispatch = {
  provider?: ModelProvider;
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
  provider?: ModelProvider;
  session_id: string;
  subscription: boolean;
  endpoint: string;
  remote?: boolean;
  executor?: string;
};

export type AgentInput = { request_id: string; prompt: string; hand?: HandSelection };

export type AgentResult = {
  ok?: { session_id: string; model: string; message: unknown };
  error?: string;
  cancelled?: boolean;
  retry?: boolean;
  waiting?: boolean;
};

export const RECENT_MODEL_LIMIT = 64;

export type ModelTiming = {
  dispatch_id: number;
  started_ms: number;
  headers_ms?: number;
  first_byte_ms?: number;
  last_byte_ms?: number;
  body_read_wait_ms: number;
  body_reads: number;
  bytes: number;
  max_chunk_gap_ms: number;
  duration_ms?: number;
  status?: number;
  outcome?: "eof" | "closed" | "cancelled" | "failed";
  diagnostic?: unknown;
};

export type RuntimeOptions = {
  dispatch(request: { endpoint: string; headers: [string, string][]; body: string }, signal?: AbortSignal, timing?: ModelTiming & { runtime_id: string }): Promise<Response>;
  wake?(): void;
  event(value: unknown): Promise<void>;
  events?(values: unknown[]): Promise<void>;
  connection?(request: { kind?: string; input?: unknown; op: string }): Promise<unknown>;
  remote?: {
    failed?(): boolean;
    execute(request: unknown, id: number): Promise<unknown>;
    cancel(id: number): void;
    dispose(): Promise<void>;
  };
};

export function json(value: unknown, status = 200, headers?: HeadersInit): Response {
  const result = new Headers(headers);
  result.set("content-type", "application/json");
  result.set("cache-control", "no-store");
  return new Response(JSON.stringify(value), { status, headers: result });
}

export function headerJson(value: unknown): string {
  return JSON.stringify(value).replace(/[\u007f-\uffff]/g, character => `\\u${character.charCodeAt(0).toString(16).padStart(4, "0")}`);
}

export async function authorityCall(binding: PrivateBinding, command: AuthorityCommand): Promise<AuthorityResult> {
  const response = await binding.fetch(new Request("https://authority.internal/", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(command) }));
  return response.json<AuthorityResult>();
}

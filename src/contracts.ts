export type ModelProvider = "claude" | "codex";

export const EXECUTOR_ID = /^[a-z0-9][a-z0-9_-]{0,47}$/;
export const DEFAULT_EXECUTOR = "default";

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
  executor_id?: string;
};

export type SessionDescriptor = SessionAuthority & {
  provider?: ModelProvider;
  session_name: string;
  subscription: boolean;
  hand?: string;
};

export type ModelDispatch = {
  provider?: ModelProvider;
  authority: SessionAuthority;
  endpoint: string;
  headers: [string, string][];
  subscription: boolean;
};

export type SocketDispatch = Pick<ModelDispatch, "provider" | "authority" | "headers">;

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
  hands?: boolean;
  event_coalesce_ms?: number;
};

export type AgentInput = { request_id: string; prompt: string; hand?: string };

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
  transport?: "websocket";
  started_ms: number;
  headers_ms?: number;
  first_byte_ms?: number;
  last_byte_ms?: number;
  body_read_wait_ms: number;
  body_reads: number;
  bytes: number;
  max_chunk_gap_ms: number;
  sends?: number;
  duration_ms?: number;
  status?: number;
  outcome?: "eof" | "closed" | "cancelled" | "failed";
  diagnostic?: unknown;
};

export type RuntimeOptions = {
  dispatch(request: { endpoint: string; headers: [string, string][]; body: Uint8Array }, signal?: AbortSignal, timing?: ModelTiming & { runtime_id: string }): Promise<Response>;
  wake?(): void;
  event(value: unknown): Promise<void>;
  events?(values: unknown[]): Promise<void>;
  socket?: {
    open(request: { headers: [string, string][] }, signal: AbortSignal, timing: ModelTiming & { runtime_id: string }): Promise<Response>;
    admit(): Promise<void>;
  };
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

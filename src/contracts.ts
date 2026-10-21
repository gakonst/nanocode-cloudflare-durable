import type { AgentConfiguration, ModelProvider } from "nanocode-ts";

export type Thinking = NonNullable<AgentConfiguration["thinking"]>;

export const THINKING: readonly Thinking[] = ["none", "low", "medium", "high", "xhigh", "max"];

export const EXECUTOR_ID = /^[a-z0-9][a-z0-9_-]{0,47}$/;
export const DEFAULT_EXECUTOR = "default";

export type SessionAuthority = {
  account_id: string;
  session_id: string;
  executor_id?: string;
};

export type SessionDescriptor = SessionAuthority & {
  provider?: ModelProvider;
  model?: string;
  thinking?: Thinking;
  session_name: string;
  subscription: boolean;
  codex_subscription?: boolean;
  hand?: string;
  read_only?: boolean;
  grant?: unknown;
  expires_at?: number;
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

export type HostToolCall = { authority: SessionAuthority; name: string; input: unknown; grant?: unknown };

export function json(value: unknown, status = 200, headers?: HeadersInit): Response {
  const result = new Headers(headers);
  result.set("content-type", "application/json");
  result.set("cache-control", "no-store");
  return new Response(JSON.stringify(value), { status, headers: result });
}

export function headerJson(value: unknown): string {
  return JSON.stringify(value).replace(/[\u007f-\uffff]/g, character => `\\u${character.charCodeAt(0).toString(16).padStart(4, "0")}`);
}

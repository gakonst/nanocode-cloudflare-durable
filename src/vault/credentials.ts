import { base64url, encoder, Failure, sha256hex, unbase64url } from "../egress/common";

export type StoredToken = { access: string; refresh: string; expires_at: number | null; scopes: string[]; refresh_expires_at: number | null; account_id?: string; fedramp?: boolean };
export type TokenPolicy = { rejected(status: number, body: string): string | undefined; accept(body: Record<string, unknown>, now: number): StoredToken | string | null };
export type ConnectionCredential =
  | { type: "static"; value: string }
  | { type: "oauth"; access: string; refresh: string; expires_at: number; token_url: string; client_id: string; client_secret?: string; scope?: string };
type Identity = { account_id?: string; fedramp?: boolean };

export const SUBSCRIPTION = {
  token_url: "https://platform.claude.com/v1/oauth/token",
  client_id: "9d1c250a-e61b-44d9-88ed-5944d1962f5e",
  scopes: ["org:create_api_key", "user:profile", "user:inference", "user:sessions:claude_code", "user:mcp_servers", "user:file_upload", "user:plugins"],
};

export const CODEX = {
  token_url: "https://auth.openai.com/oauth/token",
  client_id: "app_EMoamEEZ73f0CkXaXp7hrann",
  claims: "https://api.openai.com/auth",
};

export const SECRET_PATTERN = /^[A-Za-z0-9\-._~+/=]{1,16384}$/;
export const CODEX_ACCOUNT = /^[A-Za-z0-9_.:-]{1,128}$/;
const CODEX_LOGIN_REQUIRED = new Set(["refresh_token_expired", "refresh_token_reused", "refresh_token_invalidated", "invalid_grant"]);
const decoder = new TextDecoder();

export function oauthError(body: string): { error?: unknown; error_description?: unknown } {
  try {
    const value = JSON.parse(body);
    return value && typeof value === "object" ? value : {};
  } catch {
    return {};
  }
}

function jwtClaims(token: string): Record<string, any> | null {
  const parts = token.split(".");
  if (parts.length !== 3 || !parts[1] || !/^[A-Za-z0-9_-]+$/.test(parts[1])) return null;
  try {
    const claims = JSON.parse(decoder.decode(unbase64url(parts[1])));
    return claims && typeof claims === "object" && !Array.isArray(claims) ? claims : null;
  } catch {
    return null;
  }
}

export function subscriptionSeed(source: string): StoredToken {
  let document: any;
  try { document = JSON.parse(source); } catch { throw new Failure(500, "configuration_error"); }
  const entry = document?.anthropic ?? document?.claudeAiOauth ?? document;
  const access = entry?.access ?? entry?.accessToken;
  const refresh = entry?.refresh ?? entry?.refreshToken;
  const expires = entry?.expires ?? entry?.expiresAt;
  const scopes = Array.isArray(entry?.scopes) ? entry.scopes.filter((item: unknown) => typeof item === "string") : typeof entry?.scope === "string" ? entry.scope.split(/\s+/).filter(Boolean) : [];
  if (typeof access !== "string" || !SECRET_PATTERN.test(access) || typeof refresh !== "string" || !SECRET_PATTERN.test(refresh) || !Number.isSafeInteger(expires)) throw new Failure(500, "configuration_error");
  return { access, refresh, expires_at: expires, scopes, refresh_expires_at: null };
}

function codexIdentity(jwt: string): Identity | null {
  const claims = jwtClaims(jwt);
  if (!claims) return null;
  const auth = claims[CODEX.claims];
  if (auth === undefined) return {};
  if (!auth || typeof auth !== "object" || Array.isArray(auth)) return null;
  const account = auth.chatgpt_account_id ?? undefined;
  const fedramp = auth.chatgpt_account_is_fedramp ?? undefined;
  if ((account !== undefined && (typeof account !== "string" || !CODEX_ACCOUNT.test(account))) || (fedramp !== undefined && typeof fedramp !== "boolean")) return null;
  return { account_id: account, fedramp };
}

function codexExpiry(access: string): number | null {
  const exp = jwtClaims(access)?.exp;
  return Number.isSafeInteger(exp) && exp > 0 ? exp * 1000 : null;
}

export function codexSeed(source: string): StoredToken {
  const fail = (): never => { throw new Failure(500, "configuration_error"); };
  let document: any;
  try { document = JSON.parse(source); } catch { return fail(); }
  if (!document || typeof document !== "object" || Array.isArray(document)) fail();
  if (document.auth_mode !== undefined && document.auth_mode !== null && document.auth_mode !== "chatgpt") fail();
  const entry = document.tokens ?? document;
  if (!entry || typeof entry !== "object") fail();
  const access = entry.access_token ?? entry.access;
  const refresh = entry.refresh_token ?? entry.refresh;
  const id = entry.id_token ?? undefined;
  const explicit = entry.account_id ?? undefined;
  if (typeof access !== "string" || !SECRET_PATTERN.test(access) || typeof refresh !== "string" || !SECRET_PATTERN.test(refresh)) fail();
  if ((id !== undefined && typeof id !== "string") || (explicit !== undefined && (typeof explicit !== "string" || !CODEX_ACCOUNT.test(explicit)))) fail();
  const accessIdentity = codexIdentity(access) ?? fail();
  const idIdentity = id === undefined ? {} : codexIdentity(id) ?? fail();
  const accounts = new Set([explicit, idIdentity.account_id, accessIdentity.account_id].filter((value): value is string => value !== undefined));
  if (accounts.size !== 1) fail();
  const stated = entry.expires ?? undefined;
  if (stated !== undefined && !Number.isSafeInteger(stated)) fail();
  const expires = codexExpiry(access) ?? stated ?? fail();
  return { access, refresh, expires_at: expires, scopes: [], refresh_expires_at: null, account_id: [...accounts][0], fedramp: idIdentity.fedramp ?? accessIdentity.fedramp ?? false };
}

export function codexPolicy(current: StoredToken, minimumLifetime: number): TokenPolicy {
  return {
    rejected(status, body) {
      const value = oauthError(body) as Record<string, any>;
      const code = typeof value.error === "string" ? value.error : typeof value.error?.code === "string" ? value.error.code : typeof value.code === "string" ? value.code : undefined;
      return status === 401 || (code !== undefined && [400, 401, 403].includes(status) && CODEX_LOGIN_REQUIRED.has(code)) ? "login_required" : undefined;
    },
    accept(body, now) {
      const access = body.access_token;
      const refresh = body.refresh_token ?? undefined;
      const id = body.id_token ?? undefined;
      if (typeof access !== "string" || !SECRET_PATTERN.test(access)) return null;
      if (refresh !== undefined && (typeof refresh !== "string" || !SECRET_PATTERN.test(refresh))) return null;
      if (id !== undefined && typeof id !== "string") return null;
      if (body.token_type !== undefined && (typeof body.token_type !== "string" || body.token_type.toLowerCase() !== "bearer")) return null;
      const accessIdentity = codexIdentity(access);
      const idIdentity = id === undefined ? {} : codexIdentity(id);
      if (!accessIdentity || !idIdentity) return null;
      if ([accessIdentity.account_id, idIdentity.account_id].some(account => account !== undefined && account !== current.account_id)) return "account_mismatch";
      const expiresIn = body.expires_in;
      const expires = codexExpiry(access) ?? (Number.isSafeInteger(expiresIn) && (expiresIn as number) > 0 ? now + (expiresIn as number) * 1000 : null);
      if (expires === null || expires - now <= minimumLifetime) return null;
      return {
        access,
        refresh: refresh ?? current.refresh,
        expires_at: expires,
        scopes: current.scopes,
        refresh_expires_at: current.refresh_expires_at,
        account_id: current.account_id,
        fedramp: idIdentity.fedramp ?? accessIdentity.fedramp ?? current.fedramp ?? false,
      };
    },
  };
}

export function connectionCredential(value: any): ConnectionCredential {
  const fail = (): never => { throw new Failure(500, "configuration_error"); };
  if (value?.type === "static" && typeof value.value === "string" && /^[\x21-\x7e]{1,16384}$/.test(value.value)) return { type: "static", value: value.value };
  if (value?.type !== "oauth") return fail();
  let url: URL;
  try { url = new URL(value.token_url); } catch { return fail(); }
  if (url.protocol !== "https:" || url.username || url.password || url.hash) fail();
  if (typeof value.access !== "string" || !SECRET_PATTERN.test(value.access) || typeof value.refresh !== "string" || !SECRET_PATTERN.test(value.refresh) || !Number.isSafeInteger(value.expires_at) || typeof value.client_id !== "string" || !value.client_id) fail();
  if ((value.client_secret !== undefined && typeof value.client_secret !== "string") || (value.scope !== undefined && typeof value.scope !== "string")) fail();
  return { type: "oauth", access: value.access, refresh: value.refresh, expires_at: value.expires_at, token_url: url.toString(), client_id: value.client_id, client_secret: value.client_secret, scope: value.scope };
}

type Key = { kid: string; key: CryptoKey };
let keySource: string | undefined;
let derived: Promise<Key> | undefined;

function keyFor(source: string | undefined): Promise<Key> {
  if (!source) throw new Failure(503, "vault_key_required");
  if (source.length < 32) throw new Failure(500, "configuration_error");
  if (keySource !== source || !derived) {
    keySource = source;
    derived = (async () => {
      const material = await crypto.subtle.importKey("raw", encoder.encode(source), "HKDF", false, ["deriveKey"]);
      const key = await crypto.subtle.deriveKey({ name: "HKDF", hash: "SHA-256", salt: encoder.encode("nanocode-egress-vault"), info: encoder.encode("credential-envelope-v1") }, material, { name: "AES-GCM", length: 256 }, false, ["encrypt", "decrypt"]);
      return { kid: (await sha256hex(`nanocode-vault-kid:${source}`)).slice(0, 16), key };
    })();
  }
  return derived;
}

export async function seal(source: string | undefined, aad: string, value: unknown): Promise<string> {
  const { kid, key } = await keyFor(source);
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const sealed = new Uint8Array(await crypto.subtle.encrypt({ name: "AES-GCM", iv, additionalData: encoder.encode(aad) }, key, encoder.encode(JSON.stringify(value))));
  return `v1.${kid}.${base64url(iv)}.${base64url(sealed)}`;
}

export async function open<T>(source: string | undefined, aad: string, envelope: string): Promise<T> {
  if (envelope.startsWith("p1.")) return JSON.parse(envelope.slice(3)) as T;
  const [version, kid, iv, sealed] = envelope.split(".");
  if (version !== "v1") throw new Failure(503, "vault_key_required");
  const key = await keyFor(source);
  if (key.kid !== kid) throw new Failure(503, "vault_key_mismatch");
  try {
    const plain = await crypto.subtle.decrypt({ name: "AES-GCM", iv: unbase64url(iv), additionalData: encoder.encode(aad) }, key.key, unbase64url(sealed));
    return JSON.parse(decoder.decode(plain)) as T;
  } catch {
    throw new Failure(503, "vault_key_mismatch");
  }
}

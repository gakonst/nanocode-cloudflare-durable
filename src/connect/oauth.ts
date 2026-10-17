import { json, type AuthorityResult } from "../contracts";
import { authority, ConnectError, IDENTIFIER, publicOrigin, readForm, readJson, requestOrigin, requireOwner, SCOPE_TOKEN, SCOPES, strings, text, tokenAccount, validOrigin, withCors, type ConnectEnv } from "./common";

const VERIFIER = /^[A-Za-z0-9._~-]{43,128}$/;
const CHALLENGE = /^[A-Za-z0-9_-]{43,128}$/;

export function authorizationServerMetadata(request: Request, env: ConnectEnv): Response {
  const issuer = publicOrigin(request, env);
  return json({
    issuer,
    authorization_endpoint: `${issuer}/oauth/authorize`,
    token_endpoint: `${issuer}/oauth/token`,
    revocation_endpoint: `${issuer}/oauth/revoke`,
    response_types_supported: ["code"],
    response_modes_supported: ["query"],
    grant_types_supported: ["authorization_code", "refresh_token"],
    code_challenge_methods_supported: ["S256"],
    token_endpoint_auth_methods_supported: ["none"],
    revocation_endpoint_auth_methods_supported: ["none"],
    authorization_response_iss_parameter_supported: true,
    scopes_supported: [SCOPES.write, SCOPES.read],
  }, 200, { "access-control-allow-origin": "*" });
}

export function protectedResourceMetadata(request: Request, env: ConnectEnv, mcp: boolean): Response {
  const issuer = publicOrigin(request, env);
  return json({
    resource: mcp ? `${issuer}/mcp` : issuer,
    authorization_servers: [issuer],
    bearer_methods_supported: ["header"],
    scopes_supported: [SCOPES.write, SCOPES.read],
    resource_name: "Nanocode Connect",
  }, 200, { "access-control-allow-origin": "*" });
}

function redirectUri(value: unknown): URL {
  const raw = text(value, "redirect_uri", 2048);
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw new ConnectError(400, "invalid_request", "redirect_uri must be an absolute URI");
  }
  if (raw.includes("#")) throw new ConnectError(400, "invalid_request", "redirect_uri must not contain a fragment");
  if (url.username || url.password) throw new ConnectError(400, "invalid_request", "redirect_uri must not contain credentials");
  if (url.protocol === "javascript:" || url.protocol === "data:" || url.protocol === "file:") throw new ConnectError(400, "invalid_request", "redirect_uri scheme is not permitted");
  return url;
}

function scopes(value: unknown): string[] {
  if (value === undefined || value === "") return [];
  const list = text(value, "scope", 2048).split(" ");
  return strings(list, "scope", SCOPE_TOKEN);
}

function authorizationRequest(request: Request, env: ConnectEnv, url: URL) {
  const params = url.searchParams;
  for (const key of ["response_type", "client_id", "redirect_uri", "code_challenge", "code_challenge_method", "state", "scope", "resource"]) {
    if (params.getAll(key).length > 1) throw new ConnectError(400, "invalid_request", `duplicate parameter ${key}`);
  }
  if (params.get("response_type") !== "code") throw new ConnectError(400, "unsupported_response_type", "response_type must be code");
  const client_id = text(params.get("client_id") ?? undefined, "client_id");
  if (!IDENTIFIER.test(client_id)) throw new ConnectError(400, "invalid_request", "invalid client_id");
  redirectUri(params.get("redirect_uri") ?? undefined);
  const code_challenge = params.get("code_challenge") ?? "";
  if (!CHALLENGE.test(code_challenge)) throw new ConnectError(400, "invalid_request", "code_challenge is required");
  if (params.get("code_challenge_method") !== "S256") throw new ConnectError(400, "invalid_request", "code_challenge_method must be S256");
  const state = params.get("state") ?? undefined;
  if (state !== undefined) text(state, "state", 1024);
  const resource = params.get("resource");
  const issuer = publicOrigin(request, env);
  if (resource !== null && resource !== issuer && resource !== `${issuer}/` && resource !== `${issuer}/mcp`) throw new ConnectError(400, "invalid_target", "unknown resource");
  return { client_id, redirect_uri: params.get("redirect_uri")!, code_challenge, state, scopes: scopes(params.get("scope") ?? undefined), issuer };
}

export async function authorizeEndpoint(request: Request, env: ConnectEnv, url: URL, ownerAuthorized: boolean): Promise<Response> {
  if (request.method !== "GET" && request.method !== "POST") return json({ error: "method_not_allowed", error_description: "method not allowed" }, 405, { allow: "GET, POST" });
  const pending = authorizationRequest(request, env, url);
  requireOwner(request, env, ownerAuthorized);
  if (request.method === "GET") {
    return json({ authorization_request: { client_id: pending.client_id, redirect_uri: pending.redirect_uri, scopes: pending.scopes, state: pending.state ?? null, default_origin: pending.issuer } });
  }
  const decision = await readJson(request);
  if (!decision || typeof decision !== "object" || Array.isArray(decision)) throw new ConnectError(400, "invalid_request", "expected a JSON object");
  const principal = text(decision.principal, "principal");
  const origin = decision.origin ?? pending.issuer;
  if (!validOrigin(origin)) throw new ConnectError(400, "invalid_request", "origin must be an exact https or loopback http origin");
  const granted = decision.scopes === undefined ? pending.scopes : strings(decision.scopes, "scopes", SCOPE_TOKEN);
  if (pending.scopes.length && !granted.every(scope => pending.scopes.includes(scope))) throw new ConnectError(400, "invalid_scope", "approved scopes must be a subset of the requested scopes");
  if (!granted.length) throw new ConnectError(400, "invalid_scope", "at least one scope must be approved");
  const connections = decision.connections === undefined ? [] : strings(decision.connections, "connections", IDENTIFIER);
  const result = await authority(env, {
    op: "authorize_code",
    account_id: env.ACCOUNT_ID,
    client_id: pending.client_id,
    origin,
    redirect_uri: pending.redirect_uri,
    principal,
    scopes: granted,
    connections,
    code_challenge: pending.code_challenge,
    code_challenge_method: "S256",
  });
  if (typeof result.code !== "string" || !tokenAccount(result.code, "na1")) throw new ConnectError(502, "server_error", "invalid authority response");
  const target = new URL(pending.redirect_uri);
  target.searchParams.set("code", result.code);
  if (pending.state !== undefined) target.searchParams.set("state", pending.state);
  target.searchParams.set("iss", pending.issuer);
  return json({ redirect_to: target.href });
}

function tokenResponse(result: AuthorityResult): Response {
  if (typeof result.access_token !== "string" || !tokenAccount(result.access_token, "nc1")) throw new ConnectError(502, "server_error", "invalid authority response");
  const body: Record<string, unknown> = { access_token: result.access_token, token_type: "Bearer" };
  if (Number.isSafeInteger(result.expires_in)) body.expires_in = result.expires_in;
  if (Number.isSafeInteger(result.refresh_expires_in)) body.refresh_expires_in = result.refresh_expires_in;
  if (typeof result.refresh_token === "string" && tokenAccount(result.refresh_token, "nr1")) body.refresh_token = result.refresh_token;
  if (typeof result.scope === "string") body.scope = result.scope;
  return json(body, 200, { pragma: "no-cache" });
}

function oauthFailure(error: unknown): never {
  if (!(error instanceof ConnectError) || error.status >= 500) throw error;
  if (error.code === "invalid_request" || error.code === "invalid_client") throw new ConnectError(400, error.code, error.code === "invalid_client" ? "client is not registered for this request" : "token request is invalid");
  throw new ConnectError(400, "invalid_grant", "authorization grant is invalid, expired or revoked");
}

export async function tokenEndpoint(request: Request, env: ConnectEnv): Promise<Response> {
  if (request.method !== "POST") return json({ error: "method_not_allowed", error_description: "method not allowed" }, 405, { allow: "POST" });
  const origin = requestOrigin(request);
  const form = await readForm(request);
  if (form.grant_type === "authorization_code") {
    const code = text(form.code, "code", 512);
    const account = tokenAccount(code, "na1");
    if (!account) throw new ConnectError(400, "invalid_grant", "authorization grant is invalid, expired or revoked");
    const verifier = typeof form.code_verifier === "string" && VERIFIER.test(form.code_verifier) ? form.code_verifier : null;
    if (!verifier) throw new ConnectError(400, "invalid_request", "code_verifier is invalid");
    const client_id = text(form.client_id, "client_id");
    redirectUri(form.redirect_uri);
    const bound = origin ?? publicOrigin(request, env);
    const result = await authority(env, { op: "exchange_code", account_id: account, code, code_verifier: verifier, client_id, redirect_uri: form.redirect_uri as string, origin: bound }).catch(oauthFailure);
    return withCors(tokenResponse(result), origin);
  }
  if (form.grant_type === "refresh_token") {
    const refresh_token = text(form.refresh_token, "refresh_token", 512);
    const account = tokenAccount(refresh_token, "nr1");
    if (!account) throw new ConnectError(400, "invalid_grant", "authorization grant is invalid, expired or revoked");
    const command: Record<string, unknown> = { op: "refresh", account_id: account, refresh_token };
    if (form.client_id !== undefined) command.client_id = text(form.client_id, "client_id");
    if (origin) command.origin = origin;
    const result = await authority(env, command as any).catch(oauthFailure);
    return withCors(tokenResponse(result), origin);
  }
  throw new ConnectError(400, "unsupported_grant_type", "grant_type must be authorization_code or refresh_token");
}

export async function revokeEndpoint(request: Request, env: ConnectEnv): Promise<Response> {
  if (request.method !== "POST") return json({ error: "method_not_allowed", error_description: "method not allowed" }, 405, { allow: "POST" });
  const origin = requestOrigin(request);
  const form = await readForm(request);
  const token = text(form.token, "token", 512);
  const account = tokenAccount(token, "nc1") ?? tokenAccount(token, "nr1");
  if (!account) return withCors(json({}), origin);
  try {
    const result = await authority(env, { op: "revoke", account_id: account, token });
    return withCors(json(result.status === 202 ? { pending: true } : {}, result.status === 202 ? 202 : 200), origin);
  } catch (error) {
    if (error instanceof ConnectError && error.status >= 500) throw error;
    return withCors(json({}), origin);
  }
}

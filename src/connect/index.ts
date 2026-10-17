import { authority, ConnectError, delegation, failure, IDENTIFIER, preflight, project, publicOrigin, readJson, requireOwner, SCOPE_TOKEN, segment, strings, text, validOrigin, withCors, type ConnectEnv } from "./common";
import { handleThreads, methodNotAllowed, serviceCall, serviceInput } from "./hosted";
import { handleMcp } from "./mcp";
import { authorizationServerMetadata, authorizeEndpoint, protectedResourceMetadata, revokeEndpoint, tokenEndpoint } from "./oauth";

export type { ConnectEnv } from "./common";

const DELEGATED_HEADERS = ["authorization", "content-type", "accept", "last-event-id", "mcp-protocol-version"];
const OAUTH_HEADERS = ["content-type", "accept"];
const DISCOVERY_HEADERS = ["accept", "mcp-protocol-version"];
const OWNER_HEADERS = ["authorization", "content-type", "accept"];

function object(value: unknown): Record<string, any> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new ConnectError(400, "invalid_request", "expected a JSON object");
  return value as Record<string, any>;
}

function redirectUris(value: unknown): string[] {
  if (!Array.isArray(value) || value.length > 16 || new Set(value).size !== value.length) throw new ConnectError(400, "invalid_request", "redirect_uris must be an array of unique absolute URIs");
  for (const item of value) {
    let url: URL;
    try {
      url = new URL(text(item, "redirect_uri", 2048));
    } catch (error) {
      if (error instanceof ConnectError) throw error;
      throw new ConnectError(400, "invalid_request", "redirect_uris must be absolute URIs");
    }
    if (item.includes("#") || url.username || url.password || url.protocol === "javascript:" || url.protocol === "data:" || url.protocol === "file:") throw new ConnectError(400, "invalid_request", "redirect_uri is not permitted");
  }
  return value;
}

function origins(value: unknown): string[] {
  if (!Array.isArray(value) || value.length > 16 || new Set(value).size !== value.length || !value.every(validOrigin)) throw new ConnectError(400, "invalid_request", "origins must be an array of unique exact https or loopback http origins");
  return value;
}

async function ownerRoute(request: Request, env: ConnectEnv, parts: string[], ownerAuthorized: boolean): Promise<Response | null> {
  if (parts.length === 1 && parts[0] === "clients") {
    if (request.method !== "POST") return methodNotAllowed("POST");
    requireOwner(request, env, ownerAuthorized);
    const input = object(await readJson(request));
    const app_id = text(input.app_id, "app_id", 128);
    if (!IDENTIFIER.test(app_id)) throw new ConnectError(400, "invalid_request", "invalid app_id");
    const command = { op: "register_client", account_id: env.ACCOUNT_ID, app_id, origins: origins(input.origins ?? []), redirect_uris: redirectUris(input.redirect_uris ?? []) };
    if (!command.origins.length && !command.redirect_uris.length) throw new ConnectError(400, "invalid_request", "at least one origin or redirect_uri is required");
    return project(await authority(env, command), 201);
  }
  if (parts.length === 1 && parts[0] === "grants") {
    if (request.method !== "POST") return methodNotAllowed("POST");
    requireOwner(request, env, ownerAuthorized);
    const input = object(await readJson(request));
    const app_id = text(input.app_id, "app_id", 128);
    if (!IDENTIFIER.test(app_id)) throw new ConnectError(400, "invalid_request", "invalid app_id");
    if (!validOrigin(input.origin)) throw new ConnectError(400, "invalid_request", "origin must be an exact http(s) origin");
    const command: Record<string, unknown> = {
      op: "create_grant",
      account_id: env.ACCOUNT_ID,
      app_id,
      origin: input.origin,
      principal: text(input.principal, "principal"),
      scopes: strings(input.scopes, "scopes", SCOPE_TOKEN),
      connections: strings(input.connections ?? [], "connections", IDENTIFIER),
    };
    if (input.catalog_digest !== undefined) command.catalog_digest = text(input.catalog_digest, "catalog_digest", 256);
    if (input.ttl_ms !== undefined) {
      if (!Number.isSafeInteger(input.ttl_ms) || input.ttl_ms <= 0) throw new ConnectError(400, "invalid_request", "ttl_ms must be a positive integer");
      command.ttl_ms = input.ttl_ms;
    }
    return project(await authority(env, command as any), 201);
  }
  if (parts.length === 2 && parts[0] === "grants") {
    const grant_id = text(parts[1], "grant_id", 128);
    if (!IDENTIFIER.test(grant_id)) throw new ConnectError(400, "invalid_request", "invalid grant_id");
    if (request.method === "GET") {
      requireOwner(request, env, ownerAuthorized);
      return project(await authority(env, { op: "inspect", account_id: env.ACCOUNT_ID, grant_id }));
    }
    if (request.method === "DELETE") {
      requireOwner(request, env, ownerAuthorized);
      return project(await authority(env, { op: "revoke", account_id: env.ACCOUNT_ID, grant_id }));
    }
    return methodNotAllowed("GET, DELETE");
  }
  if (parts.length === 1 && parts[0] === "connections") {
    if (request.method !== "GET") return methodNotAllowed("GET");
    requireOwner(request, env, ownerAuthorized);
    return project(await authority(env, { op: "list_connections", account_id: env.ACCOUNT_ID }));
  }
  return null;
}

function ownerPath(parts: string[]): boolean {
  return (parts.length === 1 && (parts[0] === "clients" || parts[0] === "grants" || parts[0] === "connections")) || (parts.length === 2 && parts[0] === "grants");
}

function delegatedPath(parts: string[]): boolean {
  return (parts[0] === "threads" && parts.length <= 5) || (parts[0] === "services" && parts.length === 2);
}

async function connectRoute(request: Request, env: ConnectEnv, parts: string[], url: URL, ownerAuthorized: boolean): Promise<Response | null> {
  if (ownerPath(parts)) {
    if (request.method === "OPTIONS") return preflight(request, ["GET", "POST", "DELETE"], OWNER_HEADERS, publicOrigin(request, env));
    return ownerRoute(request, env, parts, ownerAuthorized);
  }
  if (!delegatedPath(parts)) return null;
  if (request.method === "OPTIONS") return preflight(request, parts[0] === "services" ? ["POST"] : ["GET", "POST"], DELEGATED_HEADERS);
  const delegated = delegation(request, env, ownerAuthorized);
  if (parts[0] === "services") {
    if (request.method !== "POST") return methodNotAllowed("POST");
    const response = await serviceCall(env, delegated, serviceInput(await readJson(request), parts[1]));
    return response.status === 200 ? withCors(response, delegated.cors) : response;
  }
  return withCors(await handleThreads(request, env, delegated, parts.slice(1), url), delegated.cors);
}

export async function handleConnect(request: Request, env: ConnectEnv, ownerAuthorized: boolean): Promise<Response | null> {
  const url = new URL(request.url);
  const path = url.pathname;
  const metadata = `${publicOrigin(request, env)}/.well-known/oauth-protected-resource${path === "/mcp" ? "/mcp" : ""}`;
  try {
    if (path === "/.well-known/oauth-authorization-server") {
      if (request.method === "OPTIONS") return preflight(request, ["GET"], DISCOVERY_HEADERS, "*");
      return request.method === "GET" || request.method === "HEAD" ? authorizationServerMetadata(request, env) : methodNotAllowed("GET");
    }
    if (path === "/.well-known/oauth-protected-resource" || path === "/.well-known/oauth-protected-resource/mcp") {
      if (request.method === "OPTIONS") return preflight(request, ["GET"], DISCOVERY_HEADERS, "*");
      return request.method === "GET" || request.method === "HEAD" ? protectedResourceMetadata(request, env, path.endsWith("/mcp")) : methodNotAllowed("GET");
    }
    if (path === "/oauth/authorize") {
      if (request.method === "OPTIONS") return preflight(request, ["GET", "POST"], OWNER_HEADERS, publicOrigin(request, env));
      return await authorizeEndpoint(request, env, url, ownerAuthorized);
    }
    if (path === "/oauth/token") {
      if (request.method === "OPTIONS") return preflight(request, ["POST"], OAUTH_HEADERS);
      return await tokenEndpoint(request, env);
    }
    if (path === "/oauth/revoke") {
      if (request.method === "OPTIONS") return preflight(request, ["POST"], OAUTH_HEADERS);
      return await revokeEndpoint(request, env);
    }
    if (path === "/mcp") {
      if (request.method === "OPTIONS") return preflight(request, ["POST"], DELEGATED_HEADERS);
      return await handleMcp(request, env, ownerAuthorized);
    }
    if (path !== "/connect" && !path.startsWith("/connect/")) return null;
    const parts = path.split("/").slice(2).map(segment);
    return await connectRoute(request, env, parts, url, ownerAuthorized);
  } catch (error) {
    return failure(error, metadata);
  }
}

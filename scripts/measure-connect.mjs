import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { randomBytes, createHash } from "node:crypto";
import { writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { localOptions } from "./local-runtime.mjs";

const origin = "https://app.invalid";
const connectorCredential = randomBytes(32).toString("base64url");
const vaultCredential = randomBytes(32).toString("base64url");
const observed = { model: 0, connector: 0, vault: 0 };
const assertions = [];
const check = (value, message) => { if (!value) throw new Error(message); assertions.push(message); };
const provider = async request => {
  const url = new URL(request.url);
  if (url.hostname === "connector.invalid") {
    observed.connector++;
    if (request.headers.get("authorization") !== `Bearer ${connectorCredential}`) throw new Error("Connector injection failed");
    return Response.json({ answer: 42, secret: connectorCredential });
  }
  if (url.hostname === "vault.invalid") {
    observed.vault++;
    if (request.headers.get("authorization") !== `Bearer ${vaultCredential}`) throw new Error("Vault injection failed");
    return Response.json({ echo: vaultCredential, headers: Object.fromEntries(request.headers) });
  }
  if (url.hostname !== "provider.invalid") throw new Error("Unexpected upstream destination");
  observed.model++;
  const input = await request.json();
  if (JSON.stringify(input.messages).includes("Fresh grant hello") && JSON.stringify(input.messages).includes("Say hello")) throw new Error("Fresh grant inherited a prior transcript");
  const events = [
    ["message_start", { type: "message_start", message: { id: `connect-${observed.model}`, type: "message", role: "assistant", model: input.model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 8, output_tokens: 0 } } }],
    ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }],
    ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "connect measurement complete" } }],
    ["content_block_stop", { type: "content_block_stop", index: 0 }],
    ["message_delta", { type: "message_delta", delta: { stop_reason: "end_turn", stop_sequence: null }, usage: { output_tokens: 4 } }],
    ["message_stop", { type: "message_stop" }],
  ];
  return new Response(events.map(([event, data]) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`).join(""), { headers: { "content-type": "text/event-stream" } });
};
const connections = {
  demo: { base_url: "https://connector.invalid", methods: ["GET"], paths: ["/safe"], response_fields: ["answer"], max_body_bytes: 4096, max_response_bytes: 4096 },
  vault: { kind: "vault", base_url: "https://vault.invalid", methods: ["GET"], paths: ["/safe"], max_body_bytes: 4096, max_response_bytes: 4096 },
};
const connectionCredentials = { demo: { type: "static", value: connectorCredential }, vault: { type: "static", value: vaultCredential } };
const options = localOptions({ provider, egress: { CONNECTIONS_JSON: JSON.stringify(connections) }, vault: { CONNECTION_CREDENTIALS_JSON: JSON.stringify(connectionCredentials), VAULT_KEY: randomBytes(32).toString("base64url") } });
const mf = new Miniflare(convertV4MiniflareOptions(options));
const owner = { authorization: "Bearer local-measurement", "content-type": "application/json" };
const request = (path, method = "GET", body, headers = owner) => mf.dispatchFetch(`https://worker${path}`, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
const delegated = token => ({ authorization: `Bearer ${token}`, origin, "content-type": "application/json" });
async function accepted(response, expected = 200) {
  const value = await response.json();
  if (response.status !== expected) throw new Error(`HTTP ${response.status}: ${JSON.stringify(value)}`);
  return value;
}
async function until(condition, detail) {
  const start = performance.now();
  while (!await condition()) {
    if (performance.now() - start > 20000) throw new Error(detail);
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}
async function grant(extra = {}) {
  return accepted(await request("/connect/grants", "POST", { app_id: "measurement", origin, principal: "owner", scopes: ["agent:write", "agent:read", "connectors", "vault", "connector:call", "vault:call"], connections: ["demo", "vault"], ...extra }), 201);
}
try {
  await mf.ready;
  const metadata = await accepted(await request("/.well-known/oauth-authorization-server", "GET", undefined, {}));
  check(metadata.code_challenge_methods_supported.includes("S256"), "PKCE S256 discovery");
  const client = (await accepted(await request("/connect/clients", "POST", { app_id: "measurement", origins: [origin], redirect_uris: [`${origin}/callback`] }), 201)).client;
  const verifier = randomBytes(32).toString("base64url");
  const query = new URLSearchParams({ response_type: "code", client_id: client.client_id, redirect_uri: `${origin}/callback`, code_challenge: createHash("sha256").update(verifier).digest("base64url"), code_challenge_method: "S256", scope: "agent:write agent:read", state: "measurement-state" });
  const consent = await accepted(await request(`/oauth/authorize?${query}`, "POST", { principal: "owner", origin, connections: ["demo"] }));
  const code = new URL(consent.redirect_to).searchParams.get("code");
  const tokens = await accepted(await request("/oauth/token", "POST", { grant_type: "authorization_code", code, code_verifier: verifier, client_id: client.client_id, redirect_uri: `${origin}/callback` }, { origin, "content-type": "application/json" }));
  check(tokens.access_token.startsWith("nc1.default."), "PKCE exchange issues account-routed opaque access token");
  const initialize = await accepted(await request("/mcp", "POST", { jsonrpc: "2.0", id: 1, method: "initialize", params: { protocolVersion: "2025-11-25", clientInfo: { name: "measurement", version: "1" }, capabilities: {} } }, delegated(tokens.access_token)));
  check(initialize.result.serverInfo.name === "nanocode-connect", "MCP initializes with live grant authority");
  const active = await grant();
  const service = await accepted(await request("/connect/services/demo", "POST", { path: "/safe" }, delegated(active.access_token)));
  check(service.data?.answer === 42 && !JSON.stringify(service).includes(connectorCredential), "Connector response projection excludes secret-bearing fields");
  const vault = await accepted(await request("/connect/services/vault", "POST", { path: "/safe", kind: "vault" }, delegated(active.access_token)));
  check(vault.status === 200 && !JSON.stringify(vault).includes(vaultCredential) && !vault.data, "Vault echo response remains status-only");
  const accountStorage = await mf.unsafeGetDurableObjectStorage("nanocode-egress", "AccountAuthority", { name: "default" });
  const healthBindings = await mf.getBindings("nanocode-worker");
  const health = await (await healthBindings.AUTHORITY.fetch(new Request("https://authority.internal/", { method: "POST", body: JSON.stringify({ op: "health", account_id: "default" }) }))).json();
  check(health.threads === 0, "Service-only and MCP initialization allocate no hosted sessions");
  const denied = await request("/connect/services/demo", "POST", { path: "/denied" }, delegated(active.access_token));
  check(denied.status >= 400 && observed.connector === 1, "Connector destination policy rejects disallowed paths before dispatch");
  const thread = (await accepted(await request("/connect/threads", "POST", {}, delegated(active.access_token)), 201)).thread_id;
  const base = `/connect/threads/${thread}`;
  await accepted(await request(`${base}/turns`, "POST", { request_id: "connect-turn", prompt: "Say hello" }, delegated(active.access_token)), 202);
  let result;
  await until(async () => { result = await accepted(await request(`${base}/turns/connect-turn`, "GET", undefined, delegated(active.access_token))); if (result.status === "failed") throw new Error("Hosted Connect inference failed"); return result.status === "completed"; }, "Hosted Connect turn did not complete");
  const before = observed.model;
  await accepted(await request(`${base}/turns`, "POST", { request_id: "connect-turn", prompt: "Say hello" }, delegated(active.access_token)));
  check(observed.model === before, "Connect duplicate returns the saved result");
  const events = await accepted(await request(`${base}/events?limit=100`, "GET", undefined, delegated(active.access_token)));
  check(events.events.length > 0 && Number.isSafeInteger(events.next_cursor), "Connect event history uses durable cursors");
  const wrongOrigin = await request(`${base}/turns/connect-turn`, "GET", undefined, { ...delegated(active.access_token), origin: "https://other.invalid" });
  check(wrongOrigin.status === 403, "Live authorization rejects a different origin");
  const fresh = await grant();
  const isolated = await request(`${base}/turns/connect-turn`, "GET", undefined, delegated(fresh.access_token));
  check(isolated.status === 404, "Fresh grant cannot read an older grant's saved turn");
  const empty = await accepted(await request(`${base}/events`, "GET", undefined, delegated(fresh.access_token)));
  check(empty.events.length === 0, "Fresh grant cannot replay an older grant's events");
  await accepted(await request(`${base}/turns`, "POST", { request_id: "fresh-turn", prompt: "Fresh grant hello" }, delegated(fresh.access_token)), 202);
  let freshResult;
  await until(async () => {
    freshResult = await accepted(await request(`${base}/turns/fresh-turn`, "GET", undefined, delegated(fresh.access_token)));
    if (freshResult.status === "failed") throw new Error("Fresh grant inference failed");
    return freshResult.status === "completed";
  }, "Fresh grant turn did not complete");
  check(freshResult.ok.session_id !== result.ok.session_id, "Fresh grant uses a separate SDK identity in the same thread object");
  const rotated = await accepted(await request("/oauth/token", "POST", { grant_type: "refresh_token", refresh_token: fresh.refresh_token }, { origin, "content-type": "application/json" }));
  const reused = await request("/oauth/token", "POST", { grant_type: "refresh_token", refresh_token: fresh.refresh_token }, { origin, "content-type": "application/json" });
  check(reused.status === 400, "Consumed refresh token reuse is rejected");
  const family = await request(`${base}/events`, "GET", undefined, delegated(rotated.access_token));
  check(family.status === 401 || family.status === 403, "Refresh token reuse revokes the issued family");
  const revoked = await request(`/connect/grants/${active.grant.grant_id}`, "DELETE");
  check(revoked.status === 200 || revoked.status === 202, "Owner revocation persists canonical authority and fences");
  await until(async () => (await accepted(await request(`/connect/grants/${active.grant.grant_id}`))).pending_fences === 0, "Revocation fence outbox did not drain");
  const deniedStatus = await request(`${base}/turns/connect-turn`, "GET", undefined, delegated(active.access_token));
  check(deniedStatus.status === 401 || deniedStatus.status === 403, "Revoked grant cannot read saved output");
  const encrypted = await accountStorage.exec("SELECT name FROM sqlite_master WHERE type = 'table'");
  const report = { runtime: "local workerd", assertions, observed, account_tables: encrypted.map(row => row.name), elapsed_check_count: assertions.length };
  writeFileSync("output/connect-measurement.json", JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
} finally { await mf.dispose(); }

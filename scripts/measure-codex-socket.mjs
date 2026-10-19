import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { randomBytes } from "node:crypto";
import { writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { localOptions } from "./local-runtime.mjs";

const SAMPLES = 8;
const CONNECT_DELAY_MS = 50;
const BETA = "responses_websockets=2026-02-06";
const MOCK = `
const state = { handshakes: [], requests: [], rejections: [], sockets: new Map(), calls: new Set(), stalled: new Set(), responses: 0, closed: [] };

function inspect(request, env, transport) {
  const expected = JSON.parse(env.EXPECTED);
  const bearer = (request.headers.get("authorization") ?? "").replace(/^Bearer /, "");
  let claims;
  try { claims = JSON.parse(atob(bearer.split(".")[1].replace(/-/g, "+").replace(/_/g, "/"))); } catch {}
  return {
    transport,
    session: request.headers.get("session-id"),
    authorized: claims?.["https://api.openai.com/auth"]?.chatgpt_account_id === expected.account && request.headers.get("chatgpt-account-id") === expected.account,
    original: request.headers.get("authorization") === expected.authorization,
    beta: request.headers.get("openai-beta"),
    lite: request.headers.get("x-openai-internal-codex-responses-lite"),
    timing_metrics: request.headers.get("x-responsesapi-include-timing-metrics"),
    thread: request.headers.get("thread-id") === request.headers.get("session-id") && request.headers.get("x-client-request-id") === request.headers.get("session-id"),
    user_agent: request.headers.get("user-agent"),
    header_names: [...request.headers.keys()].sort(),
  };
}

function reply(body, id, entry) {
  const items = Array.isArray(body.input) ? body.input : [];
  const last = [...items].reverse().find(item => item.type === "function_call_output" || (item.type === "message" && item.role === "user"));
  entry.last = last?.type === "function_call_output" ? "tool_output" : last ? "user" : "none";
  let output;
  if (last?.type === "function_call_output") {
    entry.tool_call_known = state.calls.has(last.call_id);
    entry.tool_output_ok = /ok\\W+true/.test(JSON.stringify(last.output));
    output = [{ type: "message", id: "msg_" + id, role: "assistant", status: "completed", content: [{ type: "output_text", text: entry.tool_call_known && entry.tool_output_ok ? "tool-ok" : "tool-mismatch" }] }];
  } else if (last && JSON.stringify(last.content).includes("then close the socket")) {
    entry.close_after = true;
    output = [{ type: "message", id: "msg_" + id, role: "assistant", status: "completed", content: [{ type: "output_text", text: "socket-ok" }] }];
  } else if (last && JSON.stringify(last.content).includes("stall the stream") && !state.stalled.has(entry.session)) {
    state.stalled.add(entry.session);
    entry.stalled = true;
    return [{ type: "response.created", response: { id, status: "in_progress" } }];
  } else if (last && JSON.stringify(last.content).includes("hold the response")) {
    entry.held = true;
    return [];
  } else if (last && JSON.stringify(last.content).includes("use a tool")) {
    state.calls.add("call_" + id);
    output = [{ type: "function_call", id: "fc_" + id, call_id: "call_" + id, name: "list_connections", arguments: "{}", status: "completed" }];
  } else {
    output = [{ type: "message", id: "msg_" + id, role: "assistant", status: "completed", content: [{ type: "output_text", text: "socket-ok" }] }];
  }
  const events = [{ type: "response.created", response: { id, status: "in_progress" } }];
  output.forEach((item, output_index) => {
    events.push({ type: "response.output_item.added", output_index, item: item.type === "message" ? { ...item, status: "in_progress", content: [] } : item });
    if (item.type === "message") events.push({ type: "response.output_text.delta", output_index, item_id: item.id, content_index: 0, delta: item.content[0].text });
    events.push({ type: "response.output_item.done", output_index, item });
  });
  events.push({ type: "response.completed", response: { id, status: "completed", model: "gpt-6.1-sol", output, usage: { input_tokens: 8, output_tokens: 4, total_tokens: 12 } } });
  return events;
}

function request(env, text, transport, connection) {
  const body = JSON.parse(text);
  const entry = { transport, connection, type: body.type ?? null, model: body.model, store: body.store, stream: body.stream, previous: body.previous_response_id ?? null, input_items: Array.isArray(body.input) ? body.input.length : 0, session: body.client_metadata?.session_id ?? null, lite_metadata: body.client_metadata?.ws_request_header_x_openai_internal_codex_responses_lite ?? null, credential_free: !JSON.parse(env.SECRETS).some(value => text.includes(value)) };
  state.requests.push(entry);
  return { body, entry };
}

async function upgrade(incoming, env) {
  await new Promise(resolve => setTimeout(resolve, Number(env.CONNECT_DELAY_MS)));
  const handshake = inspect(incoming, env, "websocket");
  state.handshakes.push(handshake);
  const echoed = incoming.headers.get("authorization") ?? "";
  const rejection = state.rejections.shift();
  if (rejection) {
    handshake.rejected = rejection.status;
    const headers = { "content-type": "application/json", "x-echo-authorization": echoed, "set-cookie": "session=" + echoed };
    if (rejection.retry_after !== undefined) headers["retry-after"] = String(rejection.retry_after);
    return new Response(JSON.stringify({ error: { type: "invalid_request_error", code: rejection.code, message: "echo " + echoed } }), { status: rejection.status, headers });
  }
  const [client, server] = Object.values(new WebSocketPair());
  server.accept();
  const connection = state.handshakes.length;
  const known = new Set();
  state.sockets.set(server, handshake.session);
  server.addEventListener("message", event => {
    const { body, entry } = request(env, String(event.data), "websocket", connection);
    if (body.previous_response_id && !known.has(body.previous_response_id)) {
      entry.missing = true;
      server.send(JSON.stringify({ type: "error", status: 400, error: { type: "invalid_request_error", code: "previous_response_not_found", message: "Previous response not found." } }));
      return;
    }
    entry.continued = !!body.previous_response_id;
    const id = "resp_" + ++state.responses;
    known.add(id);
    for (const value of reply(body, id, entry)) server.send(JSON.stringify(value));
    if (!entry.close_after) return;
    state.sockets.delete(server);
    state.closed.push({ session: handshake.session, by: "upstream" });
    server.close(1000, "idle");
  });
  const closed = () => {
    const session = state.sockets.get(server);
    if (state.sockets.delete(server)) state.closed.push({ session, by: "client" });
    try { server.close(1000, "closed"); } catch {}
  };
  server.addEventListener("close", closed);
  server.addEventListener("error", closed);
  return new Response(null, { status: 101, webSocket: client, headers: { "x-request-id": "req_ws_" + connection, "openai-model": "gpt-6.1-sol", "x-echo-authorization": echoed, "set-cookie": "session=" + echoed } });
}

async function https(incoming, env) {
  await new Promise(resolve => setTimeout(resolve, Number(env.CONNECT_DELAY_MS)));
  const handshake = inspect(incoming, env, "https");
  const { body, entry } = request(env, await incoming.text(), "https", null);
  entry.authorized = handshake.authorized;
  entry.lite = handshake.lite;
  const id = "resp_" + ++state.responses;
  return new Response(reply(body, id, entry).map(value => "data: " + JSON.stringify(value) + "\\n\\n").join(""), { headers: { "content-type": "text/event-stream", "x-request-id": "req_https_" + id } });
}

async function control(incoming, url) {
  if (url.pathname === "/stats") return Response.json({ handshakes: state.handshakes, requests: state.requests, open: state.sockets.size, closed: state.closed });
  if (url.pathname === "/reject") { state.rejections.push(...(await incoming.json())); return Response.json({ ok: true }); }
  return new Response("not found", { status: 404 });
}

export default {
  async fetch(incoming, env) {
    const url = new URL(incoming.url);
    if (url.hostname === "mock.control") return control(incoming, url);
    if (url.origin !== "https://chatgpt.com" || url.pathname !== "/backend-api/codex/responses") return new Response("unexpected upstream", { status: 404 });
    return incoming.headers.get("upgrade")?.toLowerCase() === "websocket" ? upgrade(incoming, env) : https(incoming, env);
  },
};
`;
const PROBE = `
export default {
  async fetch(incoming, env) {
    const input = await incoming.json();
    const headers = { "x-nanocode-socket": JSON.stringify(input.dispatch) };
    if (input.upgrade) headers.upgrade = "websocket";
    const response = await env.EGRESS.fetch(new Request("https://egress.internal/socket", { headers }));
    const socket = response.webSocket;
    if (socket) { socket.accept(); socket.close(1000, "probe"); }
    return Response.json({ status: response.status, websocket: !!socket, headers: [...response.headers], body: socket ? "" : await response.text() });
  },
};
`;

const secret = () => randomBytes(32).toString("base64url");
const segment = value => Buffer.from(JSON.stringify(value)).toString("base64url");
const account = `account-${randomBytes(9).toString("base64url")}`;
const token = () => [segment({ alg: "RS256", typ: "JWT" }), segment({ exp: Math.floor(Date.now() / 1000) + 3600, "https://api.openai.com/auth": { chatgpt_account_id: account } }), secret()].join(".");
const access = token();
const refresh = secret();
const credentials = [access, access.split(".")[2], refresh, account];
let currentRefresh = refresh;
let refreshes = 0;
const redact = value => credentials.reduce((text, item) => text.split(item).join("[redacted]"), String(value));
const exposed = value => credentials.some(item => JSON.stringify(value ?? null).includes(item));
let outbound = 0;
const options = localOptions({
  provider: async incoming => {
    const url = new URL(incoming.url);
    if (url.origin !== "https://auth.openai.com" || url.pathname !== "/oauth/token") { outbound++; throw new Error("Private egress attempted direct network access"); }
    const input = await incoming.json();
    if (input.grant_type !== "refresh_token" || input.refresh_token !== currentRefresh) throw new Error("Unexpected Codex credential refresh");
    refreshes++;
    const rotated = token();
    currentRefresh = secret();
    credentials.push(rotated, rotated.split(".")[2], currentRefresh);
    return Response.json({ access_token: rotated, refresh_token: currentRefresh, expires_in: 3600, token_type: "Bearer" });
  },
  egress: { MODEL_STREAM_IDLE_TIMEOUT_MS: "1000" },
  vault: { CODEX_OAUTH_JSON: JSON.stringify({ auth_mode: "chatgpt", tokens: { access_token: access, refresh_token: refresh, account_id: account } }), VAULT_KEY: secret() },
  egressServices: { CODEX_TRANSPORT: "codex-mock" },
  workers: [
    { name: "codex-mock", routes: ["mock.control/*"], modules: [{ type: "ESModule", path: "codex-mock.mjs", contents: MOCK }], compatibilityDate: "2026-10-16", bindings: { CONNECT_DELAY_MS: String(CONNECT_DELAY_MS), EXPECTED: JSON.stringify({ authorization: `Bearer ${access}`, account }), SECRETS: JSON.stringify(credentials) } },
    { name: "egress-probe", routes: ["probe.local/*"], modules: [{ type: "ESModule", path: "egress-probe.mjs", contents: PROBE }], compatibilityDate: "2026-10-16", serviceBindings: { EGRESS: { name: "nanocode-egress", entrypoint: "SessionEgress" } } },
  ],
});
let mf;
const headers = { authorization: "Bearer local-measurement", "content-type": "application/json" };
const request = async (url, init = {}, attempts = 3) => {
  try { return await mf.dispatchFetch(url, { ...init, headers }); }
  catch (error) {
    if (attempts <= 1 || error?.cause?.code !== "ECONNRESET") throw error;
    return request(url, init, attempts - 1);
  }
};
const launch = async () => { mf = new Miniflare(convertV4MiniflareOptions(options)); await mf.ready; };
const mock = (path, init) => mf.dispatchFetch(`https://mock.control${path}`, init);
const stats = async () => (await mock("/stats")).json();
const probe = async input => (await mf.dispatchFetch("https://probe.local/", { method: "POST", body: JSON.stringify(input) })).json();
const assertions = [];
const check = (value, message) => { if (!value) throw new Error(message); assertions.push(message); };
const stamp = Date.now();
const median = values => { const sorted = [...values].sort((a, b) => a - b); return sorted.length ? sorted[Math.floor((sorted.length - 1) / 2)] : null; };
const round = value => value === null || value === undefined ? value : Math.round(value * 100) / 100;

async function until(condition, detail, timeout = 30000) {
  const started = performance.now();
  while (!await condition()) {
    if (performance.now() - started > timeout) throw new Error(detail);
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}

async function submit(name, id, prompt) {
  const submitted = await request(`http://worker/sessions/${name}/turns`, { method: "POST", body: JSON.stringify({ request_id: id, prompt, provider: "codex" }) });
  if (submitted.status !== 202) throw new Error(`Submit rejected with ${submitted.status}: ${redact(await submitted.text())}`);
}

async function settled(name, id, expected = "completed") {
  let result;
  await until(async () => {
    result = await (await request(`http://worker/sessions/${name}/turns/${id}`)).json();
    return ["completed", "failed", "cancelled"].includes(result.status);
  }, `Turn ${id} stalled`);
  if (result.status !== expected) throw new Error(`Turn ${id} ended ${result.status}: ${redact(JSON.stringify(result)).slice(0, 4000)}`);
  return result;
}

async function turn(name, id, prompt) {
  const started = performance.now();
  await submit(name, id, prompt);
  const result = await settled(name, id);
  return { id, milliseconds: performance.now() - started, message: result.ok?.message, session: result.ok?.session_id };
}

async function events(name) {
  const values = [];
  let after = 0;
  for (;;) {
    const page = await (await request(`http://worker/sessions/${name}/events?after=${after}&limit=128`)).json();
    values.push(...page.events);
    after = page.next_cursor;
    if (!page.has_more) return values.map(entry => ({ request_id: entry.request_id, kind: entry.event?.event?.type, payload: entry.event?.event?.payload }));
  }
}

const metrics = async name => (await request(`http://worker/sessions/${name}/metrics`)).json();
const storage = name => mf.unsafeGetDurableObjectStorage("nanocode-worker", "AgentSession", { name });
const persisted = async name => (await storage(name)).exec("SELECT payload FROM states UNION ALL SELECT value AS payload FROM records UNION ALL SELECT body AS payload FROM events UNION ALL SELECT result AS payload FROM jobs");
const ofSession = (values, session) => values.filter(value => value.session === session);
const kinds = (list, kind, request_id) => list.filter(entry => entry.kind === kind && (request_id === undefined || entry.request_id === request_id));
const calls = (list, ids) => kinds(list, "model.call.completed").filter(entry => ids.includes(entry.request_id)).map(entry => entry.payload.duration_ns / 1e6);

try {
  await launch();
  const name = `codex-socket-${stamp}`;
  const closedByClient = async session => (await stats()).closed.filter(entry => entry.session === session && entry.by === "client").length;
  const released = async () => until(async () => (await stats()).open === 0, "The idle session kept its socket open");
  const turns = [await turn(name, "first", "Say hello"), await turn(name, "tool", "Please use a tool to list connections")];
  for (let index = 0; index < SAMPLES; index++) turns.push(await turn(name, `warm-${index}`, "Say hello again"));
  const session = turns[0].session;
  await released();
  let state = await stats();
  let list = await events(name);
  const handshakes = ofSession(state.handshakes, session);
  const requests = ofSession(state.requests, session);
  const socketMetrics = await metrics(name);
  check(handshakes.length === turns.length && requests.length === turns.length + 1, "Each of ten turns opened one upstream WebSocket and the tool turn's two model calls shared it");
  check(requests.every(entry => entry.transport === "websocket" && entry.type === "response.create" && entry.store === false && entry.lite_metadata === "true"), "Every model call was a response.create frame on a socket");
  check(requests[2].connection === requests[1].connection && requests[2].continued && !requests[2].missing && requests.filter((_, index) => index !== 2).every(entry => entry.previous === null), "Calls within a turn send only new input on that turn's socket, and each turn's first call replays the full history on a fresh socket");
  check(handshakes.every(entry => entry.authorized && entry.original && entry.beta === BETA && entry.lite === "true" && entry.timing_metrics === "true" && entry.thread && entry.user_agent?.startsWith("nanocode-worker/") && !entry.header_names.some(header => header.startsWith("x-nanocode"))), "The handshake carries egress-injected credentials, the native SDK WebSocket headers and no internal headers");
  const connected = kinds(list, "model.connection.completed");
  check(connected.length === turns.length && connected.every(entry => entry.payload.http_status === 101 && entry.payload.server_model === "gpt-6.1-sol") && connected[0].payload.request_id === "req_ws_1", "Handshake metadata from each projected 101 response reached the SDK");
  const toolRequest = requests.find(entry => entry.last === "tool_output");
  check(turns[1].message === "tool-ok" && toolRequest?.tool_call_known && toolRequest.tool_output_ok && toolRequest.continued && kinds(list, "tool.result", "tool").length === 1, "A list_connections tool-call loop completed over the socket with incremental tool output");
  check(socketMetrics.runtime.model_sockets === turns.length && socketMetrics.runtime.model_requests === 0 && socketMetrics.runtime.socket_sends === requests.length, "The resident runtime opened one socket per turn and sent no HTTPS model requests");
  check(socketMetrics.model_diagnostics.length === turns.length && socketMetrics.model_diagnostics.every(entry => entry.websocket && entry.status === 101 && entry.transport === "fixed_binding"), "Each handshake recorded one safe websocket diagnostic through the fixed binding");
  check(await closedByClient(session) === turns.length, "The session closed its socket each time it went idle");
  const evictable = await mf.unsafeEvictDurableObject("nanocode-worker", "AgentSession", { name }).then(() => true, () => false);
  check(evictable, "An idle session holds no Codex socket, so ordinary eviction proceeds");
  const socketCalls = calls(list, turns.slice(2).map(entry => entry.id));
  const firstCall = calls(list, ["first"]);

  let before = ofSession((await stats()).handshakes, session).length;
  await turn(name, "upstream-close", "Say hello then close the socket");
  const reopened = await turn(name, "after-upstream-close", "Say hello after close");
  state = await stats();
  list = await events(name);
  const afterClose = ofSession(state.requests, session).at(-1);
  const closeRun = kinds(list, "run.completed", "after-upstream-close")[0]?.payload;
  check(ofSession(state.handshakes, session).length === before + 2 && afterClose.previous === null && closeRun?.response_retries === 0, "A socket closed by upstream is replaced by the next turn with a full replay and no retry");

  await released();
  before = ofSession((await stats()).handshakes, session).length;
  const closedBefore = await closedByClient(session);
  await submit(name, "held", "Please hold the response");
  await until(async () => ofSession((await stats()).requests, session).some(entry => entry.held), "Held request did not reach the socket");
  await submit(name, "queued-1", "Say hello again");
  await submit(name, "queued-2", "Say hello again");
  const cancelStarted = performance.now();
  await request(`http://worker/sessions/${name}/turns/held/cancel`, { method: "POST" });
  await settled(name, "held", "cancelled");
  await until(async () => await closedByClient(session) > closedBefore, "Cancellation did not close the in-flight socket");
  const cancelMs = performance.now() - cancelStarted;
  const afterCancel = await settled(name, "queued-1");
  await settled(name, "queued-2");
  state = await stats();
  const queued = ofSession(state.requests, session).slice(-2);
  check(ofSession(state.handshakes, session).length === before + 2 && queued[0].previous === null && queued[1].connection === queued[0].connection && queued[1].continued && afterCancel.ok?.message === "socket-ok", "Cancelling an in-flight call closes its socket, and the turns queued behind it share one new socket without releasing it between them");

  await released();
  before = ofSession((await stats()).handshakes, session).length;
  const stalled = await turn(name, "stall", "Please stall the stream once");
  state = await stats();
  list = await events(name);
  const stallRequests = ofSession(state.requests, session).slice(-2);
  check(stalled.message === "socket-ok" && stalled.milliseconds >= 1000 && ofSession(state.handshakes, session).length === before + 2 && stallRequests[0].stalled && stallRequests[1].connection !== stallRequests[0].connection && stallRequests[1].previous === null && kinds(list, "model.attempt.retrying", "stall").some(entry => entry.payload.error_class === "premature_close"), "A stalled socket response is closed at the private stream idle deadline and retried on a fresh socket");

  await released();
  await mf.unsafeEvictDurableObject("nanocode-worker", "AgentSession", { name });
  before = ofSession((await stats()).handshakes, session).length;
  const evicted = await turn(name, "after-eviction", "Say hello after eviction");
  state = await stats();
  const evictedMetrics = await metrics(name);
  const handshakesTotal = ofSession(state.handshakes, session).length;
  check(handshakesTotal === before + 1 && ofSession(state.requests, session).at(-1).previous === null && evicted.session === session && evictedMetrics.runtime_initializations === 1 && evictedMetrics.runtime.model_sockets === 1, "The evicted session restores, reconnects once with a full replay and completes");

  const framesBeforeRestart = (await stats()).requests;
  await mf.dispose();
  await launch();
  const restarted = await turn(name, "after-restart", "Say hello after restart");
  state = await stats();
  const restartRequest = state.requests[0];
  check(state.handshakes.length === 1 && state.requests.length === 1 && restartRequest.previous === null && restartRequest.input_items > requests[0].input_items && restarted.session === session, "After workerd replacement the session reconnects with a full-history replay and completes");

  const fallbackName = `codex-fallback-${stamp}`;
  await mock("/reject", { method: "POST", body: JSON.stringify([{ status: 426, code: "upgrade_required" }]) });
  const fallbackTurns = [await turn(fallbackName, "fallback", "Say hello over fallback")];
  for (let index = 0; index < SAMPLES; index++) fallbackTurns.push(await turn(fallbackName, `https-${index}`, "Say hello again"));
  const fallbackSession = fallbackTurns[0].session;
  state = await stats();
  const fallbackList = await events(fallbackName);
  const fallbackHandshakes = ofSession(state.handshakes, fallbackSession);
  const fallbackRequests = ofSession(state.requests, fallbackSession);
  check(fallbackHandshakes.length === 1 && fallbackHandshakes[0].rejected === 426 && fallbackRequests.length === fallbackTurns.length && fallbackRequests.every(entry => entry.transport === "https" && entry.authorized && entry.lite === "true" && entry.type === null && entry.previous === null), "A 426 handshake rejection falls back to HTTPS and the session stays on HTTPS");
  check((kinds(fallbackList, "model.attempt.retrying", "fallback").some(entry => entry.payload.error_class === "websocket_fallback") || kinds(fallbackList, "model.connection.failed").some(entry => entry.payload.error?.includes("HTTP 426"))) && fallbackTurns.every(entry => entry.message === "socket-ok"), "The SDK reports the rejected WebSocket handshake and every fallback turn completes");
  const httpsCalls = calls(fallbackList, fallbackTurns.slice(1).map(entry => entry.id));

  const forbiddenName = `codex-forbidden-${stamp}`;
  await mock("/reject", { method: "POST", body: JSON.stringify(Array.from({ length: 6 }, () => ({ status: 403, retry_after: 0, code: "forbidden" }))) });
  const forbidden = await turn(forbiddenName, "forbidden", "Say hello after forbidden");
  state = await stats();
  const forbiddenHandshakes = ofSession(state.handshakes, forbidden.session);
  const forbiddenRequests = ofSession(state.requests, forbidden.session);
  const forbiddenList = await events(forbiddenName);
  check(forbiddenHandshakes.length === 6 && forbiddenHandshakes.every(entry => entry.rejected === 403) && forbiddenRequests.length === 1 && forbiddenRequests[0].transport === "https" && forbidden.message === "socket-ok", "A rejected background connect and five rejected model-call handshakes honour Retry-After, exhaust the socket budget and fall back to HTTPS");
  const forbiddenFailures = kinds(forbiddenList, "model.connection.failed");

  const probeSession = "01a151aa-0000-7000-8000-000000000000";
  const socketHeaders = [["session-id", probeSession], ["thread-id", probeSession], ["openai-beta", BETA], ["authorization", "Bearer agent-selected"], ["x-forwarded-host", "attacker.invalid"]];
  const accepted = await probe({ upgrade: true, dispatch: { provider: "codex", authority: { account_id: "default", session_id: probeSession }, headers: socketHeaders } });
  const allowedHeaders = /^(cache-control|x-request-id|openai-model|x-nanocode-model-diagnostic|x-nanocode-stream-idle-ms|content-type|retry-after|x-oai-request-id|openai-processing-ms|x-reasoning-included|x-models-etag|x-codex-[a-z0-9-]+)$/;
  state = await stats();
  const probeHandshake = ofSession(state.handshakes, probeSession).at(-1);
  check(accepted.status === 101 && accepted.websocket && accepted.headers.every(([header]) => allowedHeaders.test(header)) && !exposed(accepted) && probeHandshake.authorized && !probeHandshake.header_names.includes("x-forwarded-host"), "The egress returns the upstream socket with only allowlisted headers, replaces agent-selected authorization and drops unlisted request headers");
  await mock("/reject", { method: "POST", body: JSON.stringify([{ status: 403, code: "forbidden" }]) });
  const rejected = await probe({ upgrade: true, dispatch: { provider: "codex", authority: { account_id: "default", session_id: probeSession }, headers: socketHeaders } });
  check(rejected.status === 403 && !rejected.websocket && !exposed(rejected) && !rejected.headers.some(([header]) => header === "set-cookie" || header === "x-echo-authorization") && JSON.parse(rejected.body).error.message === "Provider request failed", "A rejected handshake is projected without echoing upstream bodies or headers");
  const claude = await probe({ upgrade: true, dispatch: { provider: "claude", authority: { account_id: "default", session_id: probeSession }, headers: [] } });
  const plain = await probe({ upgrade: false, dispatch: { provider: "codex", authority: { account_id: "default", session_id: probeSession }, headers: [] } });
  check(claude.status === 400 && JSON.parse(claude.body).error.code === "unsupported_provider" && plain.status === 400 && !claude.websocket && !plain.websocket, "Only Codex WebSocket upgrades may use the socket route");
  await mock("/reject", { method: "POST", body: JSON.stringify([{ status: 401, code: "token_expired" }]) });
  const refreshed = await probe({ upgrade: true, dispatch: { provider: "codex", authority: { account_id: "default", session_id: probeSession }, headers: socketHeaders } });
  const refreshDiagnostic = JSON.parse(refreshed.headers.find(([header]) => header === "x-nanocode-model-diagnostic")?.[1] ?? "{}");
  const refreshHandshakes = ofSession((await stats()).handshakes, probeSession).slice(-2);
  check(refreshed.status === 101 && refreshed.websocket && refreshes === 1 && refreshDiagnostic.retried && refreshDiagnostic.rotated && refreshHandshakes[0].rejected === 401 && refreshHandshakes[0].original && !refreshHandshakes[1].original && refreshHandshakes[1].authorized, "A 401 handshake rotates the Codex credential inside egress once and retries the upgrade");

  const surfaces = { persisted: [], metrics: [] };
  for (const session of [name, fallbackName, forbiddenName]) {
    surfaces.persisted.push(await persisted(session));
    surfaces.metrics.push(await metrics(session));
  }
  const egressMetrics = await (await request("http://worker/metrics")).json();
  state = await stats();
  const frames = [...framesBeforeRestart, ...state.requests];
  check(!exposed(surfaces.persisted) && !exposed(surfaces.metrics) && !exposed(egressMetrics) && frames.every(entry => entry.credential_free), "No credential value appears in persisted SDK state, records, events, jobs, metrics, diagnostics or model frames");
  check(forbiddenFailures.length === 6 && forbiddenFailures.every(entry => entry.payload.error.includes("Provider request failed") && !exposed(entry.payload)), "Handshake rejection details visible to the WASM SDK are the safe projection");
  check(outbound === 0, "Private egress made no direct network requests");

  const report = {
    runtime: "local workerd (Miniflare): compiled WASM SDK in the session Durable Object, private egress, mock Codex Responses upstream bound as CODEX_TRANSPORT",
    assertions,
    socket: {
      turns: turns.length,
      model_calls: requests.length,
      handshakes: handshakes.length,
      incremental_calls: requests.filter(entry => entry.continued).length,
      first_call_input_items: requests[0].input_items,
      incremental_input_items: requests.slice(1).map(entry => entry.input_items),
      tool_loop: { message: turns[1].message, tool_output_on_socket: !!toolRequest },
      runtime: { model_sockets: socketMetrics.runtime.model_sockets, socket_sends: socketMetrics.runtime.socket_sends, socket_frames: socketMetrics.runtime.socket_frames, model_requests: socketMetrics.runtime.model_requests },
      diagnostic: socketMetrics.model_diagnostics[0],
      after_upstream_close: { full_replay: afterClose.previous === null, response_retries: closeRun.response_retries, milliseconds: round(reopened.milliseconds) },
      cancellation: { socket_closed_by_client: true, milliseconds: round(cancelMs), queued_turns_on_one_socket: queued[1].connection === queued[0].connection },
      idle_release: { sockets_closed_when_idle: turns.length, evictable_when_idle: evictable },
      stream_idle_deadline: { idle_ms: 1000, milliseconds: round(stalled.milliseconds) },
      after_eviction: { handshakes_total: handshakesTotal, runtime_initializations: evictedMetrics.runtime_initializations, milliseconds: round(evicted.milliseconds) },
      after_restart: { handshakes: 1, full_replay_input_items: restartRequest.input_items, milliseconds: round(restarted.milliseconds) },
    },
    fallback: {
      upgrade_required: { handshakes: fallbackHandshakes.map(entry => entry.rejected), https_calls: fallbackRequests.length, turns: fallbackTurns.length },
      forbidden: { handshakes: forbiddenHandshakes.map(entry => entry.rejected), https_calls: forbiddenRequests.length, connection_failures_reported: forbiddenFailures.length },
    },
    credentials: { model_frames_checked: frames.length, persisted_sessions_checked: 3, exposed: false, handshake_refreshes: refreshes, direct_network_requests: outbound },
    latency: {
      note: "Loopback mock upstream that adds a simulated connection setup delay to every WebSocket handshake and every HTTPS request; informational only. Durations are SDK model.call.completed times inside the session Durable Object; turns are client-observed submit-to-completion times including polling.",
      simulated_connection_setup_ms: CONNECT_DELAY_MS,
      socket_first_call_with_handshake_ms: round(firstCall[0]),
      socket_call_ms: { samples: socketCalls.length, median: round(median(socketCalls)), max: round(Math.max(...socketCalls)) },
      https_call_ms: { samples: httpsCalls.length, median: round(median(httpsCalls)), max: round(Math.max(...httpsCalls)) },
      socket_turn_ms: { median: round(median(turns.slice(2).map(entry => entry.milliseconds))) },
      https_turn_ms: { median: round(median(fallbackTurns.slice(1).map(entry => entry.milliseconds))) },
    },
  };
  if (exposed(report)) throw new Error("Report would contain a credential");
  writeFileSync("output/codex-socket-measurement.json", JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
} catch (error) {
  throw new Error(redact(error instanceof Error ? error.stack ?? error.message : error));
} finally { await mf?.dispose(); }

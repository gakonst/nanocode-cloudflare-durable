import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { randomBytes } from "node:crypto";
import { writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { localOptions } from "./local-runtime.mjs";

const CONNECT_DELAY_MS = 300;
const LINGER_MS = 1500;
const SAMPLES = 5;
const MOCK = `
const state = { handshakes: 0, open: 0, requests: [] };

function lastUser(body) {
  const items = Array.isArray(body.input) ? body.input : [];
  return JSON.stringify([...items].reverse().find(item => item.type === "message" && item.role === "user")?.content ?? "");
}

function reply(server, id) {
  const item = { type: "message", id: "msg_" + id, role: "assistant", status: "completed", content: [{ type: "output_text", text: "watch-ok" }] };
  server.send(JSON.stringify({ type: "response.created", response: { id, status: "in_progress" } }));
  server.send(JSON.stringify({ type: "response.output_item.added", output_index: 0, item: { ...item, status: "in_progress", content: [] } }));
  server.send(JSON.stringify({ type: "response.output_text.delta", output_index: 0, item_id: item.id, content_index: 0, delta: "watch-ok" }));
  server.send(JSON.stringify({ type: "response.output_item.done", output_index: 0, item }));
  server.send(JSON.stringify({ type: "response.completed", response: { id, status: "completed", model: "gpt-6.1-sol", output: [item], usage: { input_tokens: 8, output_tokens: 4, total_tokens: 12 } } }));
}

export default {
  async fetch(incoming, env) {
    if (new URL(incoming.url).hostname === "mock.control") return Response.json(state);
    if (incoming.headers.get("upgrade")?.toLowerCase() !== "websocket") return new Response("websocket only", { status: 400 });
    await new Promise(resolve => setTimeout(resolve, Number(env.CONNECT_DELAY_MS)));
    const connection = ++state.handshakes;
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    state.open++;
    let open = true;
    const closed = () => {
      if (open) { open = false; state.open--; }
      try { server.close(1000, "closed"); } catch {}
    };
    server.addEventListener("close", closed);
    server.addEventListener("error", closed);
    server.addEventListener("message", event => {
      const body = JSON.parse(String(event.data));
      const held = lastUser(body).includes("hold the response");
      state.requests.push({ connection, previous: body.previous_response_id ?? null, held });
      if (!held) reply(server, "resp_" + state.requests.length);
    });
    return new Response(null, { status: 101, webSocket: client, headers: { "x-request-id": "req_ws_" + connection, "openai-model": "gpt-6.1-sol" } });
  },
};
`;

const secret = () => randomBytes(32).toString("base64url");
const segment = value => Buffer.from(JSON.stringify(value)).toString("base64url");
const account = `account-${randomBytes(9).toString("base64url")}`;
const access = [segment({ alg: "RS256", typ: "JWT" }), segment({ exp: Math.floor(Date.now() / 1000) + 3600, "https://api.openai.com/auth": { chatgpt_account_id: account } }), secret()].join(".");
const options = localOptions({
  provider: async () => { throw new Error("Private egress attempted direct network access"); },
  agent: { MODEL_SOCKET_LINGER_MS: String(LINGER_MS) },
  vault: { CODEX_OAUTH_JSON: JSON.stringify({ auth_mode: "chatgpt", tokens: { access_token: access, refresh_token: secret(), account_id: account } }), VAULT_KEY: secret() },
  egressServices: { CODEX_TRANSPORT: "codex-mock" },
  workers: [{ name: "codex-mock", routes: ["mock.control/*"], modules: [{ type: "ESModule", path: "codex-mock.mjs", contents: MOCK }], compatibilityDate: "2026-10-16", bindings: { CONNECT_DELAY_MS: String(CONNECT_DELAY_MS) } }],
});
const authorization = "Bearer local-measurement";
const mf = new Miniflare(convertV4MiniflareOptions(options));
const assertions = [];
const check = (value, message) => { if (!value) throw new Error(message); assertions.push(message); };
const median = values => { const sorted = [...values].sort((a, b) => a - b); return sorted.length ? sorted[Math.floor((sorted.length - 1) / 2)] : null; };
const round = value => value === null || value === undefined ? value : Math.round(value * 100) / 100;
const sleep = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds));
const stats = async () => (await mf.dispatchFetch("https://mock.control/")).json();
const metrics = async name => (await mf.dispatchFetch(`http://worker/sessions/${name}/metrics`, { headers: { authorization } })).json();
const storage = name => mf.unsafeGetDurableObjectStorage("nanocode-worker", "AgentSession", { name });

async function until(condition, detail, timeout = 30000) {
  const started = performance.now();
  while (!await condition()) {
    if (performance.now() - started > timeout) throw new Error(detail);
    await sleep(5);
  }
}

async function attach(name, after) {
  const response = await mf.dispatchFetch(`http://worker/sessions/${name}/socket${after === undefined ? "" : `?after=${after}`}`, { headers: { authorization, upgrade: "websocket" } });
  const socket = response.webSocket;
  if (!socket) throw new Error(`Socket rejected with ${response.status}: ${await response.text()}`);
  const watcher = { socket, events: [], replies: [], pongs: 0, closed: null };
  socket.addEventListener("message", event => {
    const data = String(event.data);
    if (data === "pong") { watcher.pongs++; return; }
    const message = { ...JSON.parse(data), received: performance.now() };
    (message.type === "event" ? watcher.events : watcher.replies).push(message);
  });
  socket.addEventListener("close", event => { watcher.closed = { code: event.code, reason: event.reason }; });
  socket.accept();
  return watcher;
}

const result = (watcher, id) => watcher.events.find(entry => entry.request_id === id && entry.event?.type === "result");
const contiguous = watcher => watcher.events.every((entry, index) => index === 0 || entry.seq === watcher.events[index - 1].seq + 1);

async function reply(watcher, type, count) {
  await until(async () => watcher.replies.filter(entry => entry.type === type).length >= count, `No ${type} reply`);
  return watcher.replies.filter(entry => entry.type === type)[count - 1];
}

async function socketTurn(watcher, id, prompt, extra = {}) {
  const started = performance.now();
  const replies = watcher.replies.filter(entry => entry.type === "turn").length;
  watcher.socket.send(JSON.stringify({ type: "turn", request_id: id, prompt, provider: "codex", ...extra }));
  const accepted = await reply(watcher, "turn", replies + 1);
  if (accepted.status !== 202) throw new Error(`Turn ${id} rejected with ${accepted.status}: ${JSON.stringify(accepted.body)}`);
  await until(async () => result(watcher, id), `Turn ${id} never reached the watcher`);
  const done = result(watcher, id);
  return { milliseconds: done.received - started, status: done.event.status, message: done.event.ok?.message };
}

const idle = () => until(async () => (await stats()).open === 0, "The model socket outlived the linger", LINGER_MS + 10000);

try {
  await mf.ready;
  const name = `watch-${Date.now()}`;
  const upgradeRequired = await mf.dispatchFetch(`http://worker/sessions/${name}/socket`, { headers: { authorization } });
  const badCursor = await mf.dispatchFetch(`http://worker/sessions/${name}/socket?after=-1`, { headers: { authorization, upgrade: "websocket" } });
  const unauthorized = await mf.dispatchFetch(`http://worker/sessions/${name}/socket`, { headers: { upgrade: "websocket" } });
  check(upgradeRequired.status === 426 && badCursor.status === 400 && unauthorized.status === 401 && !unauthorized.webSocket, "The session socket requires an authorized WebSocket upgrade and a valid cursor");

  const primary = await attach(name);
  await sleep(100);
  check(primary.events.length === 0 && primary.closed === null, "A watcher on an empty session stays open with no events");
  const first = await socketTurn(primary, "first", "Say hello");
  check(first.status === "completed" && first.message === "watch-ok" && contiguous(primary) && primary.events[0].event.type === "queued", "A turn submitted over the socket is admitted and its events stream to the same socket in order");

  const backlog = await attach(name, 0);
  const middle = primary.events[Math.floor(primary.events.length / 2)].seq;
  const tail = await attach(name, middle);
  await until(async () => backlog.events.length === primary.events.length && tail.events.at(-1)?.seq === primary.events.at(-1).seq, "Backlog did not replay");
  check(JSON.stringify(backlog.events.map(entry => [entry.seq, entry.event])) === JSON.stringify(primary.events.map(entry => [entry.seq, entry.event])) && tail.events[0].seq === middle + 1 && contiguous(tail), "A new watcher replays the stored events from its cursor exactly once");

  const submitted = await mf.dispatchFetch(`http://worker/sessions/${name}/turns`, { method: "POST", headers: { authorization, "content-type": "application/json" }, body: JSON.stringify({ request_id: "http", prompt: "Say hello over HTTP", provider: "codex" }) });
  check(submitted.status === 202, "An HTTP turn is still accepted alongside watchers");
  await until(async () => [primary, backlog, tail].every(watcher => result(watcher, "http")), "HTTP turn events did not fan out");
  check([primary, backlog, tail].every(contiguous) && backlog.events.at(-1).seq === primary.events.at(-1).seq && tail.events.at(-1).seq === primary.events.at(-1).seq, "Every watcher receives live events from turns submitted elsewhere without gaps or duplicates");

  const cold = [];
  for (let index = 0; index < SAMPLES; index++) {
    await idle();
    const before = (await stats()).handshakes;
    cold.push(await socketTurn(primary, `cold-${index}`, "Say hello cold"));
    check((await stats()).handshakes === before + 1, `Cold turn ${index} opened one model socket`);
  }
  const warm = [];
  const warmHandshake = [];
  for (let index = 0; index < SAMPLES; index++) {
    await idle();
    const before = (await stats()).handshakes;
    const started = performance.now();
    const replies = primary.replies.filter(entry => entry.type === "warm").length;
    primary.socket.send(JSON.stringify({ type: "warm" }));
    check((await reply(primary, "warm", replies + 1)).warmed === true, `Warm ${index} reported a preconnect`);
    await until(async () => (await stats()).open === 1, "Warm did not open a model socket");
    warmHandshake.push(performance.now() - started);
    warm.push(await socketTurn(primary, `warm-${index}`, "Say hello warm"));
    check((await stats()).handshakes === before + 1, `Warm turn ${index} reused the preconnected socket`);
  }
  await idle();
  check((await stats()).open === 0, "A warmed socket closes after the linger");

  primary.socket.send(JSON.stringify({ type: "turn", request_id: "held", prompt: "Please hold the response", provider: "codex" }));
  await until(async () => (await stats()).requests.some(entry => entry.held), "Held request did not reach the model");
  primary.socket.send(JSON.stringify({ type: "cancel", request_id: "held" }));
  const cancelled = await reply(primary, "cancel", 1);
  await until(async () => result(primary, "held"), "Cancelled turn never finished");
  check(cancelled.status < 300 && result(primary, "held").event.status === "cancelled", "A cancel sent over the socket settles the turn as cancelled");

  await idle();
  const startsBefore = (await metrics(name)).direct_starts;
  const evicted = await mf.unsafeEvictDurableObject("nanocode-worker", "AgentSession", { name }).then(() => true, error => String(error));
  await sleep(200);
  check(evicted === true && [primary, backlog, tail].every(watcher => watcher.closed === null), "Watchers stay connected while the idle session object is evicted");
  primary.socket.send("ping");
  await until(async () => primary.pongs === 1, "No pong after eviction");
  check(true, "Ping is answered by the runtime auto-response");
  const seen = primary.events.at(-1).seq;
  const woken = await socketTurn(primary, "after-eviction", "Say hello after eviction");
  await until(async () => result(backlog, "after-eviction") && result(tail, "after-eviction"), "Restored watchers missed events");
  const restored = await metrics(name);
  check(woken.status === "completed" && startsBefore > 1 && restored.direct_starts === 1 && restored.runtime_initializations === 1 && [primary, backlog, tail].every(contiguous) && primary.events.find(entry => entry.seq > seen).seq === seen + 1, "A turn sent over a hibernated socket wakes the session and every watcher resumes at its cursor");

  const closing = performance.now();
  tail.socket.close(1000, "done");
  await until(async () => tail.closed, "The session did not answer the close handshake", 5000);
  const closeMs = performance.now() - closing;
  check(tail.closed.code === 1000, "The session answers a client close handshake");
  const resumed = await attach(name, seen);
  await until(async () => result(resumed, "after-eviction"), "Resumed watcher missed events");
  check(resumed.events[0].seq === seen + 1 && resumed.events.at(-1).seq === primary.events.at(-1).seq && contiguous(resumed), "Reconnecting with after resumes exactly after the last seen event");

  primary.socket.send("not json");
  check((await reply(primary, "error", 1)).error === "messages must be JSON", "Malformed messages get an error reply and keep the socket open");
  await until(async () => (await (await storage(name)).exec("SELECT COUNT(*) AS count FROM watch_sockets"))[0].count === 3, "Closed watcher registration was not removed");
  check(true, "Closing a watcher removes its registration");

  const report = {
    runtime: "local workerd (Miniflare): compiled WASM SDK in the session Durable Object, private egress, mock Codex Responses upstream bound as CODEX_TRANSPORT",
    assertions,
    events: { first_turn_ms: round(first.milliseconds), backlog_events: backlog.events.length },
    latency: {
      note: "Mock upstream adds a simulated handshake delay. Turn time is socket send to the result event on the same socket; warm turns are sent after the preconnected socket is open, as when the user is still typing.",
      simulated_handshake_ms: CONNECT_DELAY_MS,
      linger_ms: LINGER_MS,
      cold_turn_ms: { samples: cold.length, median: round(median(cold.map(entry => entry.milliseconds))), max: round(Math.max(...cold.map(entry => entry.milliseconds))) },
      warm_turn_ms: { samples: warm.length, median: round(median(warm.map(entry => entry.milliseconds))), max: round(Math.max(...warm.map(entry => entry.milliseconds))) },
      warm_to_open_ms: { median: round(median(warmHandshake)) },
      eviction_wake_turn_ms: round(woken.milliseconds),
      close_handshake_ms: round(closeMs),
    },
  };
  writeFileSync("output/watch-measurement.json", JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
} finally { await mf.dispose(); }

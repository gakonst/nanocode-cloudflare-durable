import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { GATEWAY_KEY, LOCAL_TOKEN, localOptions } from "./local-runtime.mjs";

const held = [];

function sse(model, text) {
  const events = [
    ["message_start", { type: "message_start", message: { id: "message-watch", type: "message", role: "assistant", model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 8, output_tokens: 0 } } }],
    ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }],
    ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text } }],
    ["content_block_stop", { type: "content_block_stop", index: 0 }],
    ["message_delta", { type: "message_delta", delta: { stop_reason: "end_turn", stop_sequence: null }, usage: { output_tokens: 4 } }],
    ["message_stop", { type: "message_stop" }],
  ];
  return new Response(events.map(([event, data]) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`).join(""), { headers: { "content-type": "text/event-stream" } });
}

const provider = async request => {
  if (new URL(request.url).hostname !== "provider.invalid" || request.headers.get("x-api-key") !== GATEWAY_KEY) throw new Error("Provider request escaped the gateway credential policy");
  const input = await request.json();
  const last = JSON.stringify(input.messages?.at(-1)?.content ?? "");
  if (last.includes("hold the response")) {
    held.push(Date.now());
    return new Response(new ReadableStream({ start() {} }), { headers: { "content-type": "text/event-stream" } });
  }
  return sse(input.model, "watch-ok");
};

const options = localOptions({ provider });
const authorization = `Bearer ${LOCAL_TOKEN}`;
const mf = new Miniflare(convertV4MiniflareOptions(options));
const assertions = [];
const check = (value, message) => { if (!value) throw new Error(message); assertions.push(message); };
const round = value => value === null || value === undefined ? value : Math.round(value * 100) / 100;
const sleep = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds));
const metrics = async name => (await mf.dispatchFetch(`http://worker/sessions/${name}/metrics`, { headers: { authorization } })).json();
const objectName = name => JSON.stringify(["owner", name]);
const storage = name => mf.unsafeGetDurableObjectStorage("nanocode", "AgentSession", { name: objectName(name) });

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
  watcher.socket.send(JSON.stringify({ type: "turn", request_id: id, prompt, ...extra }));
  const accepted = await reply(watcher, "turn", replies + 1);
  if (accepted.status !== 202) throw new Error(`Turn ${id} rejected with ${accepted.status}: ${JSON.stringify(accepted.body)}`);
  await until(async () => result(watcher, id), `Turn ${id} never reached the watcher`);
  const done = result(watcher, id);
  return { milliseconds: done.received - started, status: done.event.status, message: done.event.ok?.message };
}

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

  const submitted = await mf.dispatchFetch(`http://worker/sessions/${name}/turns`, { method: "POST", headers: { authorization, "content-type": "application/json" }, body: JSON.stringify({ request_id: "http", prompt: "Say hello over HTTP" }) });
  check(submitted.status === 202, "An HTTP turn is still accepted alongside watchers");
  await until(async () => [primary, backlog, tail].every(watcher => result(watcher, "http")), "HTTP turn events did not fan out");
  check([primary, backlog, tail].every(contiguous) && backlog.events.at(-1).seq === primary.events.at(-1).seq && tail.events.at(-1).seq === primary.events.at(-1).seq, "Every watcher receives live events from turns submitted elsewhere without gaps or duplicates");

  primary.socket.send(JSON.stringify({ type: "warm" }));
  check((await reply(primary, "warm", 1)).warmed === true, "Warm restores the resident runtime for the next turn");

  primary.socket.send(JSON.stringify({ type: "turn", request_id: "held", prompt: "Please hold the response" }));
  await until(async () => held.length === 1, "Held request did not reach the model");
  primary.socket.send(JSON.stringify({ type: "cancel", request_id: "held" }));
  const cancelled = await reply(primary, "cancel", 1);
  await until(async () => result(primary, "held"), "Cancelled turn never finished");
  check(cancelled.status < 300 && result(primary, "held").event.status === "cancelled", "A cancel sent over the socket settles the turn as cancelled");

  const startsBefore = (await metrics(name)).direct_starts;
  const evicted = await mf.unsafeEvictDurableObject("nanocode", "AgentSession", { name: objectName(name) }).then(() => true, error => String(error));
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
    runtime: "local workerd (Miniflare): compiled WASM SDK in the session Durable Object, mock AI Gateway binding and Claude Messages upstream",
    assertions,
    events: { first_turn_ms: round(first.milliseconds), backlog_events: backlog.events.length },
    latency: { note: "Loopback mock upstream; informational only.", eviction_wake_turn_ms: round(woken.milliseconds), close_handshake_ms: round(closeMs) },
  };
  writeFileSync("output/watch-measurement.json", JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
} finally { await mf.dispose(); }

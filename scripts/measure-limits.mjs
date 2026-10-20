import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { GATEWAY_KEY, LOCAL_TOKEN, localOptions } from "./local-runtime.mjs";

const STRING_LIMIT = 8 * 1024 * 1024;
const replyBytes = Number(process.env.LIMITS_REPLY_BYTES ?? STRING_LIMIT + 1_000_000);
const held = [];

function sse(model, texts) {
  const events = [
    ["message_start", { type: "message_start", message: { id: "message-limits", type: "message", role: "assistant", model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 8, output_tokens: 0 } } }],
    ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }],
    ...texts.map(text => ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text } }]),
    ["content_block_stop", { type: "content_block_stop", index: 0 }],
    ["message_delta", { type: "message_delta", delta: { stop_reason: "end_turn", stop_sequence: null }, usage: { output_tokens: 4 } }],
    ["message_stop", { type: "message_stop" }],
  ];
  return new Response(events.map(([event, data]) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`).join(""), { headers: { "content-type": "text/event-stream" } });
}

let calls = 0;
const provider = async request => {
  if (new URL(request.url).hostname !== "provider.invalid" || request.headers.get("x-api-key") !== GATEWAY_KEY) throw new Error("Provider request escaped the gateway credential policy");
  const input = await request.json();
  const last = JSON.stringify(input.messages?.at(-1)?.content ?? "");
  calls++;
  if (last.includes("giant reply")) return sse(input.model, ["x".repeat(replyBytes)]);
  if (last.includes("hold the response")) {
    held.push(Date.now());
    return new Response(new ReadableStream({ start() {} }), { headers: { "content-type": "text/event-stream" } });
  }
  return sse(input.model, ["limits-ok"]);
};

const mf = new Miniflare(convertV4MiniflareOptions(localOptions({ provider, agent: { RECOVERY_DELAY_MS: "500" } })));
const authorization = `Bearer ${LOCAL_TOKEN}`;
const assertions = [];
const failures = [];
const check = (value, message) => { (value ? assertions : failures).push(message); console.log(`${value ? "ok" : "FAILED"}: ${message}`); };
const sleep = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds));
const route = (path, init = {}) => mf.dispatchFetch(`http://worker/sessions/${path}`, { ...init, headers: { authorization, ...init.headers } });
const post = (name, body) => route(`${name}/turns`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
const status = async (name, id) => (await route(`${name}/turns/${id}`)).json();
const metrics = async name => (await route(`${name}/metrics`)).json();
const ids = text => [...text.matchAll(/^id: (\d+)$/gm)].map(match => Number(match[1]));

async function settle(name, id, timeout = 10000) {
  const started = performance.now();
  for (;;) {
    const report = await status(name, id);
    if (!["queued", "running"].includes(report.status) || performance.now() - started > timeout) return report;
    await sleep(50);
  }
}

async function until(condition, timeout = 10000) {
  const started = performance.now();
  while (!await condition()) {
    if (performance.now() - started > timeout) return false;
    await sleep(20);
  }
  return true;
}

async function firstChunk(response) {
  const reader = response.body.getReader();
  const { value } = await reader.read();
  await reader.cancel();
  return new TextDecoder().decode(value);
}

const report = { runtime: "local workerd (Miniflare) with its SQLite string and row limit of 8 MiB, which production enforces too", string_limit_bytes: STRING_LIMIT };
try {
  await mf.ready;

  const prompts = {};
  for (const bytes of [STRING_LIMIT - 10_000, STRING_LIMIT + 10_000]) {
    const name = `limits-prompt-${bytes}-${Date.now()}`;
    const response = await post(name, { request_id: "big", prompt: "a".repeat(bytes) });
    const body = await response.json();
    const final = response.status < 300 ? await settle(name, "big") : null;
    prompts[bytes] = { status: response.status, error: body.error, final: final?.status };
  }
  report.prompts = prompts;
  check(prompts[STRING_LIMIT - 10_000].status === 202 && prompts[STRING_LIMIT - 10_000].final === "completed", "A prompt just below the SQLite string limit is admitted and completes");
  check(prompts[STRING_LIMIT + 10_000].status === 413, "A prompt above the SQLite string limit is refused as too large instead of as a retryable 503");

  {
    const name = `limits-reply-${Date.now()}`;
    const admitted = await post(name, { request_id: "giant", prompt: "Send a giant reply" });
    const stuck = await settle(name, "giant", 3000);
    const cancel = await route(`${name}/turns/giant/cancel`, { method: "POST" });
    const cancelled = await settle(name, "giant", 5000);
    const follow = await post(name, { request_id: "after", prompt: "Say hello" });
    const after = await settle(name, "after", 5000);
    const quiet = await until(async () => (await metrics(name)).alarm_at === null, 5000);
    const m = await metrics(name);
    report.oversized_event = { reply_bytes: replyBytes, admitted: admitted.status, before_cancel: { status: stuck.status, note: stuck.error }, cancel: cancel.status, after_cancel: { status: cancelled.status, error: cancelled.error }, next_turn: after.status, alarm_at: m.alarm_at, recoveries: m.recoveries };
    check(cancel.status < 300 && cancelled.status === "cancelled", "Cancelling a turn whose events cannot be stored ends it instead of recovering it forever");
    check(after.status === "completed" && quiet, "The next turn runs after that cancellation and the session stops re-arming its alarm");
  }

  {
    const name = `limits-routes-${Date.now()}`;
    const admitted = await post(name, { request_id: "held", prompt: "Please hold the response" });
    await until(async () => held.length === 1);
    const misrouted = {};
    for (const path of ["hands/held/cancel", "events/held/cancel", "metrics/held/cancel", "socket/held/cancel"]) misrouted[path] = (await route(`${name}/${path}`, { method: "POST" })).status;
    const extra = {};
    for (const path of ["events/held", "metrics/held", "socket/held"]) extra[path] = (await route(`${name}/${path}`)).status;
    const alarms = (await metrics(name)).alarms;
    await sleep(2000);
    const watchdog = (await metrics(name)).alarms - alarms;
    const still = await status(name, "held");
    const cancel = await route(`${name}/turns/held/cancel`, { method: "POST" });
    const cancelled = await settle(name, "held");
    report.routes = { admitted: admitted.status, misrouted, extra, held_after_misrouted: still.status, watchdog_alarms_in_2s: watchdog, cancel: cancel.status, cancelled: cancelled.status };
    check(watchdog >= 2, "While a turn runs, its watchdog alarm keeps firing every RECOVERY_DELAY_MS and re-arms itself");
    check(Object.values(misrouted).every(code => code === 404) && still.status === "running", "Only /turns/<id>/cancel cancels a turn; cancel under another route is not found and leaves the turn running");
    check(Object.values(extra).every(code => code === 404), "Routes without an identifier segment reject one instead of ignoring it");
    check(cancelled.status === "cancelled", "The turn route still cancels the turn");
  }

  {
    const name = `limits-resume-${Date.now()}`;
    const first = await (await route(`${name}/turns`, { method: "POST", headers: { "content-type": "application/json", accept: "text/event-stream" }, body: JSON.stringify({ request_id: "one", prompt: "Say hello" }) })).text();
    const seqs = ids(first);
    const middle = seqs[Math.floor(seqs.length / 2)];
    const resumed = ids(await (await route(`${name}/turns`, { method: "POST", headers: { "content-type": "application/json", accept: "text/event-stream", "last-event-id": String(middle) }, body: JSON.stringify({ request_id: "one", prompt: "Say hello" }) })).text());
    const stream = ids(await firstChunk(await route(`${name}/events?after=0`, { headers: { accept: "text/event-stream", "last-event-id": String(middle) } })));
    const behind = ids(await firstChunk(await route(`${name}/events?after=${middle}`, { headers: { accept: "text/event-stream", "last-event-id": "1" } })));
    const invalid = (await route(`${name}/events`, { headers: { accept: "text/event-stream", "last-event-id": "x" } })).status;
    report.resume = { events: seqs, last_event_id: middle, turn_stream: resumed, session_stream: stream, after_beyond_last_event_id: behind, invalid_last_event_id: invalid };
    check(resumed[0] === middle + 1 && resumed.at(-1) === seqs.at(-1), "A turn stream reconnecting with Last-Event-ID resumes after that event");
    check(stream[0] === middle + 1, "The session event stream resumes after Last-Event-ID instead of replaying from after");
    check(behind[0] === middle + 1, "An after cursor beyond Last-Event-ID still wins");
    check(invalid === 400, "An invalid Last-Event-ID is refused like an invalid cursor");
  }

  report.model_calls = calls;
  report.assertions = assertions;
  report.failures = failures;
  writeFileSync("output/limits-measurement.json", JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
  if (failures.length) process.exitCode = 1;
} finally { await mf.dispose(); }

import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { mkdtempSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { GATEWAY_KEY, LOCAL_TOKEN, localOptions } from "./local-runtime.mjs";

const STRING_LIMIT = 8 * 1024 * 1024;
const UNFINISHED = new Set(["queued", "running"]);
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

const persistence = resolve(mkdtempSync("output/local-state-"));
const options = () => convertV4MiniflareOptions(localOptions({ provider, persistence, agent: { RECOVERY_DELAY_MS: "500" } }));
let mf = new Miniflare(options());
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

async function events(name) {
  const all = [];
  for (let after = 0; ;) {
    const page = await (await route(`${name}/events?after=${after}&limit=16`)).json();
    all.push(...page.events);
    if (!page.has_more) return all;
    after = page.next_cursor;
  }
}

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
    const boundary = {};
    for (const gap of [100, 200, 300, 400, 500, 600, 700, 800, 1000]) {
      const name = `limits-boundary-${gap}-${Date.now()}`;
      const bytes = STRING_LIMIT - gap;
      const response = await post(name, { request_id: "edge", prompt: "b".repeat(bytes) });
      const body = await response.json();
      const entry = boundary[gap] = { prompt_bytes: bytes, status: response.status, error: body.error, bytes: body.bytes, limit: body.limit };
      if (response.status >= 300) continue;
      const final = await settle(name, "edge", 4000);
      entry.final = final.status;
      entry.note = final.error;
      if (UNFINISHED.has(final.status)) {
        await route(`${name}/turns/edge/cancel`, { method: "POST" });
        entry.after_cancel = (await settle(name, "edge", 5000)).status;
        continue;
      }
      const accepted = (await events(name)).find(event => event.event?.event?.type === "input.accepted");
      entry.input_bytes = accepted?.event.event.payload.input.length ?? null;
    }
    report.admission_boundary = boundary;
    const admitted = Object.values(boundary).filter(entry => entry.status === 202);
    const refused = Object.values(boundary).filter(entry => entry.status === 413);
    check(admitted.length > 0 && refused.length > 0 && admitted.length + refused.length === Object.keys(boundary).length, "Prompts within a few hundred bytes of the limit are either admitted or refused with 413");
    check(admitted.every(entry => entry.final === "completed" && entry.input_bytes === entry.prompt_bytes), "Every admitted prompt completes and its input.accepted event is logged with the whole prompt");
    check(refused.every(entry => entry.limit === STRING_LIMIT && entry.bytes > STRING_LIMIT), "A refused prompt's 413 names the bytes its turn needs and the limit");
  }

  {
    const name = `limits-reply-${Date.now()}`;
    const admitted = await post(name, { request_id: "giant", prompt: "Send a giant reply" });
    const finished = await settle(name, "giant", 8000);
    if (UNFINISHED.has(finished.status)) await route(`${name}/turns/giant/cancel`, { method: "POST" });
    const omitted = (await events(name)).filter(event => event.event?.type === "omitted").map(event => event.event);
    const follow = await post(name, { request_id: "after", prompt: "Say hello" });
    const after = await settle(name, "after", 5000);
    const quiet = await until(async () => (await metrics(name)).alarm_at === null, 5000);
    const m = await metrics(name);
    report.oversized_event = { reply_bytes: replyBytes, admitted: admitted.status, finished: { status: finished.status, error: finished.error }, omitted, next_turn: { admitted: follow.status, status: after.status }, alarm_at: m.alarm_at, recoveries: m.recoveries };
    check(finished.status === "failed" && /result is \d+ bytes/.test(finished.error ?? ""), "A turn whose result exceeds the SQLite value limit fails by itself with an error naming its size instead of recovering until cancelled");
    check(omitted.some(event => event.event_type === "assistant.message" && event.bytes > STRING_LIMIT) && omitted.every(event => event.request_id === "giant"), "Its oversized events are logged as omitted markers naming their type and size");
    check(after.status === "completed" && quiet && m.recoveries === 0, "The next turn runs and the session stops re-arming its alarm without a recovery");
  }

  {
    const name = `limits-body-${Date.now()}`;
    const body = JSON.stringify({ request_id: "body", prompt: "c".repeat(10_000_000) });
    const sizedStarted = performance.now();
    const sized = await route(`${name}/turns`, { method: "POST", headers: { "content-type": "application/json" }, body });
    const sizedReply = { status: sized.status, body: await sized.json(), ms: performance.now() - sizedStarted };
    const encoded = new TextEncoder().encode(body);
    const stream = new ReadableStream({ start(controller) { for (let offset = 0; offset < encoded.length; offset += 65536) controller.enqueue(encoded.subarray(offset, offset + 65536)); controller.close(); } });
    const chunkedStarted = performance.now();
    const chunked = await route(`${name}/turns`, { method: "POST", headers: { "content-type": "application/json" }, body: stream, duplex: "half" });
    const chunkedReply = { status: chunked.status, body: await chunked.json(), ms: performance.now() - chunkedStarted };
    const m = await metrics(name);
    report.body_limit = { body_bytes: encoded.length, content_length: sizedReply, chunked: chunkedReply, submits: m.log?.operations?.submit ?? 0 };
    check(sized.status === 413 && /request bodies are limited to 8388608 bytes/.test(sizedReply.body.error), "A body whose Content-Length exceeds the SQLite value limit gets 413 before it is read");
    check(chunked.status === 413 && /request bodies are limited to 8388608 bytes/.test(chunkedReply.body.error) && !m.log?.operations?.submit, "A chunked body is cut off at the limit with 413 and never reaches the session log");
  }

  {
    const name = `limits-head-${Date.now()}`;
    await post(name, { request_id: "one", prompt: "Say hello" });
    await settle(name, "one");
    const head = {};
    for (const path of ["turns/one", "events?after=0", "metrics", "hands"]) {
      const get = await route(`${name}/${path}`);
      await get.arrayBuffer();
      const answered = await route(`${name}/${path}`, { method: "HEAD" });
      head[path] = { get: get.status, head: answered.status, body_bytes: (await answered.arrayBuffer()).byteLength, same_type: answered.headers.get("content-type") === get.headers.get("content-type") };
    }
    const stream = await route(`${name}/events?after=0`, { method: "HEAD", headers: { accept: "text/event-stream" } });
    head.stream = { head: stream.status, body_bytes: (await stream.arrayBuffer()).byteLength, type: stream.headers.get("content-type") };
    const health = await mf.dispatchFetch("http://worker/health", { method: "HEAD", headers: { authorization } });
    head.health = { head: health.status, body_bytes: (await health.arrayBuffer()).byteLength };
    report.head = head;
    check(Object.values(head).every(entry => entry.head === 200 && entry.body_bytes === 0 && (entry.get === undefined || (entry.get === 200 && entry.same_type))), "HEAD on GET routes answers like GET without a body");
  }

  {
    const name = `limits-legacy-${Date.now()}`;
    const storage = async () => mf.unsafeGetDurableObjectStorage("nanocode", "AgentSession", { name: JSON.stringify(["owner", name]) });
    await (await route(`${name}/metrics`)).json();
    const legacy = await storage();
    await legacy.exec("CREATE TABLE jobs (request_id TEXT PRIMARY KEY, prompt TEXT NOT NULL, status TEXT NOT NULL, result TEXT, scope TEXT, descriptor TEXT, cancel_requested INTEGER, first_seq INTEGER, last_seq INTEGER)");
    const descriptor = JSON.stringify({ account_id: "owner", session_id: "", session_name: name, subscription: false });
    const jobs = ["legacy-0", "legacy-1", "legacy-2"];
    for (const id of jobs) await legacy.exec("INSERT INTO jobs (request_id, prompt, status, scope, descriptor) VALUES (?, ?, 'queued', 'owner', ?)", id, `${id} ${"d".repeat(3_000_000)}`, descriptor);
    await mf.dispose();
    mf = new Miniflare(options());
    await mf.ready;
    const reply = await route(`${name}/turns/legacy-0`).then(async response => ({ status: response.status, body: (await response.text()).slice(0, 200) }), error => ({ error: String(error) }));
    const next = await post(name, { request_id: "after-legacy", prompt: "Say hello" }).then(response => response.status, error => String(error));
    const finals = {};
    for (const id of [...jobs, "after-legacy"]) finals[id] = (await settle(name, id, 15000).catch(error => ({ status: String(error) }))).status;
    const tables = await storage().then(legacy => legacy.exec("SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'jobs'")).then(rows => rows.length, error => String(error));
    report.legacy_migration = { jobs: jobs.length, prompt_bytes: 3_000_000, first_read: reply, next_turn: next, finals, jobs_table_left: tables > 0 };
    check(reply.status === 200 && Object.values(finals).every(final => final === "completed") && !tables, "A pre-inbox session whose unfinished prompts exceed the SQLite value limit migrates and runs its turns");
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

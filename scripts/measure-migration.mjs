import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { mkdtempSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { LOCAL_TOKEN, localOptions } from "./local-runtime.mjs";

const legacy = process.argv[2];
if (!legacy) throw new Error("usage: node scripts/measure-migration.mjs <directory of a bundle built before the SDK turn inbox>");
const held = [];
let calls = 0;

function sse(model, text) {
  const events = [
    ["message_start", { type: "message_start", message: { id: "message-" + calls, type: "message", role: "assistant", model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 8, output_tokens: 0 } } }],
    ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }],
    ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text } }],
    ["content_block_stop", { type: "content_block_stop", index: 0 }],
    ["message_delta", { type: "message_delta", delta: { stop_reason: "end_turn", stop_sequence: null }, usage: { output_tokens: 4 } }],
    ["message_stop", { type: "message_stop" }],
  ];
  return new Response(events.map(([event, data]) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`).join(""), { headers: { "content-type": "text/event-stream" } });
}

let holding = true;
const provider = async request => {
  const input = await request.json();
  calls++;
  const last = JSON.stringify(input.messages?.at(-1)?.content ?? "");
  if (holding && last.includes("hold the response")) {
    held.push(Date.now());
    return new Response(new ReadableStream({ start() {} }), { headers: { "content-type": "text/event-stream" } });
  }
  return sse(input.model, "migrated-ok");
};

const persistence = resolve(mkdtempSync("output/local-state-"));
const headers = { authorization: `Bearer ${LOCAL_TOKEN}`, "content-type": "application/json" };
const name = `migration-${Date.now()}`;
const base = `http://worker/sessions/${name}`;
const assertions = [];
const check = (value, message, evidence) => { if (!value) throw new Error(`${message}: ${JSON.stringify(evidence)}`); assertions.push(message); };
const sleep = milliseconds => new Promise(done => setTimeout(done, milliseconds));
let mf = new Miniflare(convertV4MiniflareOptions(localOptions({ provider, persistence, bundle: legacy })));
const request = (url, init = {}) => mf.dispatchFetch(url, { ...init, headers });
const storage = () => mf.unsafeGetDurableObjectStorage("nanocode", "AgentSession", { name: JSON.stringify(["owner", name]) });
const submit = (id, prompt) => request(`${base}/turns`, { method: "POST", body: JSON.stringify({ request_id: id, prompt }) });
const status = async id => (await request(`${base}/turns/${encodeURIComponent(id)}`)).json();
async function until(condition, detail, timeout = 30000) {
  const started = performance.now();
  while (!await condition()) {
    if (performance.now() - started > timeout) throw new Error(detail);
    await sleep(10);
  }
}
async function events() {
  const all = [];
  for (let after = 0; ;) {
    const page = await (await request(`${base}/events?after=${after}&limit=128`)).json();
    all.push(...page.events);
    if (!page.has_more) return all;
    after = page.next_cursor;
  }
}

try {
  await mf.ready;
  for (const id of ["done-a", "done-b"]) {
    await submit(id, `Say hello ${id}`);
    await until(async () => (await status(id)).status === "completed", `legacy turn ${id} did not complete`);
  }
  const completed = await status("done-a");
  await submit("held", "Please hold the response");
  await until(async () => held.length === 1, "held turn did not reach the model");
  await submit("queued", "Say hello after the held turn");
  await submit("dropped", "Say hello to be cancelled");
  check((await (await request(`${base}/turns/dropped/cancel`, { method: "POST" })).json()).status === "cancelled", "the legacy Worker cancels a queued turn");
  const legacyEvents = await events();
  const legacyRows = await (await storage()).exec("SELECT request_id, status FROM jobs ORDER BY rowid");
  check(legacyRows.length === 5 && legacyRows.some(row => row.request_id === "held" && row.status === "running") && legacyRows.some(row => row.request_id === "queued" && row.status === "queued"), "the legacy session holds completed, running, queued and cancelled jobs", legacyRows);
  await mf.dispose();
  holding = false;
  const upgraded = performance.now();
  mf = new Miniflare(convertV4MiniflareOptions(localOptions({ provider, persistence })));
  await mf.ready;
  check((await status("done-a")).ok?.message === completed.ok?.message && (await status("done-a")).status === "completed", "a completed turn keeps its result after the upgrade", await status("done-a"));
  const replay = await submit("done-a", "Say hello done-a");
  check(replay.status === 200 && (await replay.json()).status === "completed", "repeating a completed legacy submission returns its saved result");
  check((await submit("done-a", "Different input")).status === 409, "a legacy request ID with different input is rejected");
  const resubmitted = await submit("held", "Please hold the response");
  check([200, 202].includes(resubmitted.status), "repeating the interrupted legacy submission finds the same turn", resubmitted.status);
  await until(async () => (await status("held")).status === "completed" && (await status("queued")).status === "completed", "pending legacy turns did not finish after the upgrade", 60000);
  const recovered = performance.now() - upgraded;
  check((await status("dropped")).status === "cancelled", "a cancelled legacy turn stays cancelled");
  const upgradedEvents = await events();
  check(JSON.stringify(upgradedEvents.slice(0, legacyEvents.length)) === JSON.stringify(legacyEvents), "the event history recorded before the upgrade pages unchanged", { before: legacyEvents.length, after: upgradedEvents.length });
  check(upgradedEvents.every((event, index) => index === 0 || event.seq > upgradedEvents[index - 1].seq) && upgradedEvents.some(event => event.request_id === "held" && event.event.type === "result"), "events after the upgrade continue the same sequence", upgradedEvents.length);
  check(upgradedEvents.filter(event => event.request_id === "held" && event.event.type === "queued").length === 1 && upgradedEvents.filter(event => event.request_id === "queued" && event.event.type === "queued").length === 1, "pending legacy turns run once under their original admission", upgradedEvents.filter(event => event.event.type === "queued").map(event => event.request_id));
  const stream = await mf.dispatchFetch(`${base}/turns?after=${upgradedEvents.at(-1).seq}`, { method: "POST", headers: { ...headers, accept: "text/event-stream" }, body: JSON.stringify({ request_id: "done-b", prompt: "Say hello done-b" }) });
  check(stream.status === 200 && (await stream.text()) === "", "a finished legacy turn's stream past its result ends at once");
  const turnStream = await mf.dispatchFetch(`${base}/turns?after=0`, { method: "POST", headers: { ...headers, accept: "text/event-stream" }, body: JSON.stringify({ request_id: "done-b", prompt: "Say hello done-b" }) });
  const turnEvents = (await turnStream.text()).split("\n\n").filter(block => block.startsWith("id: ")).map(block => JSON.parse(block.slice(block.indexOf("data: ") + 6)));
  check(turnEvents.length > 2 && turnEvents.every(event => event.request_id === "done-b") && turnEvents.at(-1).event.type === "result", "a legacy turn's own events stream from its first event to its result", turnEvents.map(event => event.event.type ?? event.event.event?.type));
  const tables = (await (await storage()).exec("SELECT name FROM sqlite_schema WHERE type = 'table'")).map(row => row.name);
  check(!tables.includes("jobs") && !tables.includes("event_turns") && tables.includes("turns"), "the legacy jobs and event_turns tables are migrated and dropped", tables);
  await submit("fresh", "Say hello as a new turn");
  await until(async () => (await status("fresh")).status === "completed", "a new turn did not complete after the upgrade");
  const report = { runtime: "local workerd: legacy bundle, then the current bundle on the same Durable Object storage", legacy, assertions, legacy_events: legacyEvents.length, events: upgradedEvents.length, recovery_ms: Math.round(recovered) };
  writeFileSync("output/migration-measurement.json", JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
} finally { await mf.dispose(); }

import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { execFileSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { parseArgs } from "node:util";
import { LOCAL_TOKEN, localOptions } from "./local-runtime.mjs";

const { values } = parseArgs({ options: { turns: { type: "string", default: "40" }, deltas: { type: "string", default: "40" }, "codex-turns": { type: "string", default: "10" }, inspect: { type: "string" }, label: { type: "string", default: "run" } } });
const TURNS = Number(values.turns);
const DELTAS = Number(values.deltas);
const CODEX_TURNS = Number(values["codex-turns"]);
const ROUNDS = 3;
const KEYS = ["store_calls", "statements", "bridge_request_bytes", "bridge_reply_bytes", "rows_written", "rows_read"];
const STORE_PATHS = ["nanocode_worker::wasm::store", "nanocode_durability_core::sql", "nanocode_durability_core::store"];
const IDLE = new Set(["(idle)", "(program)", "(garbage collector)"]);
let calls = 0;

const sse = events => new Response(events.map(([event, data]) => (event ? "event: " + event + "\n" : "") + "data: " + JSON.stringify(data) + "\n\n").join(""), { headers: { "content-type": "text/event-stream" } });

function claude(input) {
  const turn = calls++;
  const deltas = Array.from({ length: DELTAS }, (_, index) => ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "turn " + turn + " delta " + index + " carries a sentence of reply text that grows the transcript. " } }]);
  return sse([
    ["message_start", { type: "message_start", message: { id: "message-" + turn, type: "message", role: "assistant", model: input.model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 8, output_tokens: 0 } } }],
    ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }],
    ...deltas,
    ["content_block_stop", { type: "content_block_stop", index: 0 }],
    ["message_delta", { type: "message_delta", delta: { stop_reason: "end_turn", stop_sequence: null }, usage: { output_tokens: DELTAS } }],
    ["message_stop", { type: "message_stop" }],
  ]);
}

function codex(input) {
  const items = Array.isArray(input.input) ? input.input : [];
  const user = items.findLastIndex(item => item.type === "message" && item.role === "user");
  const round = items.filter((item, index) => index > user && item.type === "function_call_output").length;
  const call = calls++;
  const id = "resp_" + call;
  const item = round < ROUNDS
    ? { type: "function_call", id: "fc_" + call, call_id: "call_" + call, name: "list_agents", arguments: "{}", status: "completed" }
    : { type: "message", id: "msg_" + call, role: "assistant", status: "completed", content: [{ type: "output_text", text: ("codex reply " + call + " ").repeat(DELTAS) }] };
  return sse([
    [null, { type: "response.created", response: { id, status: "in_progress" } }],
    [null, { type: "response.output_item.added", output_index: 0, item: item.type === "message" ? { ...item, status: "in_progress", content: [] } : item }],
    ...(item.type === "message" ? [[null, { type: "response.output_text.delta", output_index: 0, item_id: item.id, content_index: 0, delta: item.content[0].text }]] : []),
    [null, { type: "response.output_item.done", output_index: 0, item }],
    [null, { type: "response.completed", response: { id, status: "completed", model: "gpt-6.1-sol", output: [item], usage: { input_tokens: 8, output_tokens: 4, total_tokens: 12 } } }],
  ]);
}

const provider = async request => {
  const input = await request.json();
  return new URL(request.url).pathname === "/openai/v1/responses" ? codex(input) : claude(input);
};

const headers = { authorization: "Bearer " + LOCAL_TOKEN, "content-type": "application/json" };
const mf = new Miniflare({ ...convertV4MiniflareOptions(localOptions({ provider })), ...(values.inspect ? { inspectorPort: Number(values.inspect) } : {}) });
const request = (url, init = {}) => mf.dispatchFetch(url, { ...init, headers });
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function workerdCpu() {
  const pids = execFileSync("pgrep", ["-P", String(process.pid), "workerd"], { encoding: "utf8" }).split("\n").filter(Boolean);
  return pids.reduce((total, pid) => total + execFileSync("ps", ["-o", "time=", "-p", pid], { encoding: "utf8" }).trim().split(":").map(Number).reduce((sum, part) => sum * 60 + part, 0) * 1000, 0);
}

async function inspector(port) {
  const targets = await (await fetch("http://127.0.0.1:" + port + "/json")).json();
  const socket = new WebSocket(targets.find(target => target.title?.includes("core:user:nanocode")).webSocketDebuggerUrl);
  await new Promise((resolve, reject) => { socket.onopen = resolve; socket.onerror = reject; });
  const waiting = new Map();
  let next = 0;
  socket.onmessage = event => { const message = JSON.parse(event.data); waiting.get(message.id)?.(message.result); waiting.delete(message.id); };
  const send = (method, params = {}) => new Promise(resolve => { waiting.set(++next, resolve); socket.send(JSON.stringify({ id: next, method, params })); });
  await send("Profiler.enable");
  await send("Profiler.setSamplingInterval", { interval: 100 });
  return { start: () => send("Profiler.start"), stop: async () => (await send("Profiler.stop")).profile, close: () => socket.close() };
}

const BASE64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
function sourceOf(map) {
  const lines = [];
  let source = 0;
  for (const line of map.mappings.split(";")) {
    const segments = [];
    let column = 0;
    for (const segment of line.split(",").filter(Boolean)) {
      const fields = [];
      let value = 0, shift = 0;
      for (const char of segment) {
        const digit = BASE64.indexOf(char);
        value += (digit & 31) << shift;
        if (digit & 32) shift += 5;
        else { fields.push(value & 1 ? -(value >> 1) : value >> 1); value = 0; shift = 0; }
      }
      column += fields[0];
      if (fields.length > 1) source += fields[1];
      segments.push([column, source]);
    }
    lines.push(segments);
  }
  return (line, column) => map.sources[(lines[line] ?? []).findLast(segment => segment[0] <= column)?.[1]];
}

function rustPath(name) {
  const crate = /Cs[0-9A-Za-z]*_(?=\d)/.exec(name);
  if (!crate) return "";
  const segments = [];
  for (let at = crate.index + crate[0].length; segments.length < 3;) {
    const length = /^\d+/.exec(name.slice(at));
    if (!length) break;
    at += length[0].length;
    segments.push(name.slice(at, at + Number(length[0])));
    at += Number(length[0]);
  }
  return segments.join("::");
}

function attribute(profile, source, turns) {
  const nodes = new Map(profile.nodes.map(node => [node.id, node]));
  const parents = new Map(profile.nodes.flatMap(node => (node.children ?? []).map(child => [child, node.id])));
  const store = node => {
    const frame = node.callFrame;
    if (frame.url.startsWith("wasm://")) return STORE_PATHS.some(path => rustPath(frame.functionName).startsWith(path)) ? "wasm" : undefined;
    return frame.url.endsWith("index.js") && source(frame.lineNumber, frame.columnNumber)?.endsWith("src/store.ts") ? "js" : undefined;
  };
  const totals = { busy: 0, js: 0, wasm: 0 };
  profile.samples.forEach((id, index) => {
    const micros = profile.timeDeltas[index + 1] ?? 0;
    if (nodes.get(id).callFrame.functionName === "(idle)") return;
    totals.busy += micros;
    if (IDLE.has(nodes.get(id).callFrame.functionName)) return;
    let kind;
    for (let cursor = id; cursor !== undefined && !kind; cursor = parents.get(cursor)) kind = store(nodes.get(cursor));
    if (kind) totals[kind] += micros;
  });
  return { busy_ms_per_turn: totals.busy / turns / 1000, store_js_ms_per_turn: totals.js / turns / 1000, store_wasm_ms_per_turn: totals.wasm / turns / 1000 };
}

async function turn(session, provider, id) {
  const started = performance.now();
  const submitted = await request("http://worker/sessions/" + session + "/turns", { method: "POST", body: JSON.stringify({ request_id: id, prompt: "prompt " + id, provider }) });
  if (submitted.status !== 202) throw new Error("Submit rejected: " + await submitted.text());
  for (;;) {
    const result = await (await request("http://worker/sessions/" + session + "/turns/" + id)).json();
    if (result.status === "completed") return performance.now() - started;
    if (["failed", "cancelled"].includes(result.status)) throw new Error("Turn " + id + " " + result.status + ": " + JSON.stringify(result));
    await sleep(2);
  }
}

async function sample(session) {
  const value = await (await request("http://worker/sessions/" + session + "/metrics")).json();
  const runtime = value.runtime ?? {};
  return { store_calls: runtime.store_calls ?? 0, statements: value.checkpoints?.statements ?? 0, bridge_request_bytes: runtime.bridge_request_bytes ?? 0, bridge_reply_bytes: runtime.bridge_reply_bytes ?? 0, rows_written: value.checkpoints?.rows_written ?? 0, rows_read: value.checkpoints?.rows_read ?? 0, database_bytes: value.database_bytes, persisted: value.persisted, operations: value.checkpoints?.operations };
}

async function scenario(name, provider, count, profiler) {
  const session = "store-" + name + "-" + Date.now();
  const turns = [];
  await turn(session, provider, "open");
  let before = await sample(session);
  const cpu = workerdCpu();
  await profiler?.start();
  for (let index = 0; index < count; index++) {
    const milliseconds = await turn(session, provider, "turn-" + index);
    const after = await sample(session);
    turns.push({ milliseconds, ...Object.fromEntries(KEYS.map(key => [key, after[key] - before[key]])), database_bytes: after.database_bytes });
    before = after;
  }
  const profile = await profiler?.stop();
  const workerdMs = workerdCpu() - cpu;
  await mf.unsafeEvictDurableObject("nanocode", "AgentSession", { name: JSON.stringify(["owner", session]) });
  const restoreCpu = workerdCpu();
  const restoreMs = await turn(session, provider, "restored");
  const restored = await sample(session);
  const mean = key => turns.reduce((total, entry) => total + entry[key], 0) / count;
  return {
    name, provider, turns: count,
    per_turn: Object.fromEntries(["milliseconds", ...KEYS].map(key => [key, mean(key)])),
    workerd_cpu_ms_per_turn: workerdMs / count,
    ...(profile ? { profile: attribute(profile, sourceOf(JSON.parse(readFileSync("output/deploy/index.js.map", "utf8"))), count) } : {}),
    database_bytes: before.database_bytes,
    persisted: before.persisted,
    operations: before.operations,
    restored: { milliseconds: restoreMs, workerd_cpu_ms: workerdCpu() - restoreCpu, ...Object.fromEntries(KEYS.map(key => [key, restored[key]])) },
    series: turns,
  };
}

try {
  await mf.ready;
  const profiler = values.inspect ? await inspector(values.inspect) : undefined;
  const report = { label: values.label, turns: TURNS, deltas: DELTAS, codex_turns: CODEX_TURNS, scenarios: [await scenario("claude", "claude", TURNS, profiler), await scenario("codex", "codex", CODEX_TURNS, profiler)] };
  profiler?.close();
  mkdirSync("output", { recursive: true });
  const path = "output/store-" + values.label + "-" + Date.now() + ".json";
  writeFileSync(path, JSON.stringify(report, null, 2));
  for (const { series, ...entry } of report.scenarios) console.log(JSON.stringify(entry));
  console.log("report", path);
} finally { await mf.dispose(); }

import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { spawnSync } from "node:child_process";
import { mkdirSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { parseArgs } from "node:util";
import { localOptions } from "./local-runtime.mjs";

const { values } = parseArgs({ options: {
  agents: { type: "string", default: "10000" },
  batch: { type: "string", default: "50" },
  rate: { type: "string", default: "0" },
  latency: { type: "string", default: "0" },
  label: { type: "string", default: "hillclimb" },
  scenario: { type: "string" },
  scenarios: { type: "string", default: "score,spawn,turn,done,twice" },
  "gate-agents": { type: "string" },
  limit: { type: "string", default: "3600" },
} });

const RATE = Number(values.rate);
const LATENCY = Number(values.latency);
const LIMIT_MS = Number(values.limit) * 1000;
const prices = { request: 0.15e-6, gb_s: 12.5e-6, row_written: 1e-6, gb_month: 0.2, object_gb: 0.128 };

if (!values.scenario) {
  mkdirSync("output/hillclimb", { recursive: true });
  const runs = {};
  for (const scenario of values.scenarios.split(",")) {
    const agents = scenario === "score" ? values.agents : values["gate-agents"] ?? values.agents;
    const out = "output/hillclimb/" + values.label + "-" + scenario + ".json";
    const started = performance.now();
    const child = spawnSync(process.execPath, [process.argv[1], "--scenario", scenario, "--agents", agents, "--batch", values.batch, "--rate", values.rate, "--latency", values.latency, "--label", values.label, "--limit", values.limit], { stdio: ["ignore", "ignore", "inherit"], env: { ...process.env, HILLCLIMB_OUT: out } });
    runs[scenario] = (() => { try { return JSON.parse(readFileSync(out, "utf8")); } catch { return { failure: "scenario exited " + child.status + " without a report" }; } })();
    console.error(scenario + " finished in " + ((performance.now() - started) / 1000).toFixed(0) + " s");
  }
  const scored = score(runs);
  writeFileSync("output/hillclimb/" + values.label + ".json", JSON.stringify({ label: values.label, bundle: process.env.NANOCODE_BUNDLE_PATH ?? null, rate: RATE, latency_ms: LATENCY, score: scored, runs }, null, 1));
  console.log(JSON.stringify(scored, null, 1));
  process.exit(scored.gate.pass ? 0 : 1);
}

function score(runs) {
  const run = runs.score ?? {};
  const n = run.agents ?? 0;
  const gates = Object.entries(runs).filter(([name]) => name !== "score").map(([name, entry]) => ({ scenario: name, pass: entry.gate?.pass === true, ...entry.gate, failure: entry.failure }));
  const window = run.window_ms ? run.window_ms / 1000 : null;
  const duration = (run.child_active_ms ?? 0) / 1000 * prices.object_gb;
  const requests = (run.parent?.requests ?? 0) + (run.parent?.child_requests ?? 0) * 2;
  const cloudflare = requests * prices.request + duration * prices.gb_s + (run.parent?.rows_written ?? 0) * prices.row_written + (run.store_bytes ?? 0) / 1e9 * prices.gb_month;
  const restart = runs.turn?.restarts?.[0] ?? null;
  return {
    agents: n,
    gate: { pass: gates.length > 0 && gates.every(entry => entry.pass), scenarios: gates },
    saturation: RATE > 0 && window ? Number((run.provider.accepted / (RATE * window)).toFixed(3)) : null,
    infra_share: RATE > 0 && run.wall_ms ? Number(Math.max(0, 1 - run.provider.accepted / RATE / (run.wall_ms / 1000)).toFixed(3)) : null,
    wall_s: run.wall_ms ? Number((run.wall_ms / 1000).toFixed(1)) : null,
    parent_bytes_per_child: n && run.parent?.peak_wasm_bytes ? Math.round(run.parent.peak_wasm_bytes / n) : null,
    parent_peak_wasm_mb: run.parent?.peak_wasm_bytes ? Number((run.parent.peak_wasm_bytes / 1e6).toFixed(1)) : null,
    parent_requests_per_child: n ? Number(((run.parent?.requests ?? 0) / n).toFixed(2)) : null,
    parent_peak_requests_per_s: run.parent?.peak_requests_per_s ?? null,
    parent_outbound_per_child: n ? Number(((run.parent?.child_requests ?? 0) / n).toFixed(2)) : null,
    parent_rows_written_per_child: n ? Number(((run.parent?.rows_written ?? 0) / n).toFixed(2)) : null,
    store_bytes_per_child: n ? Math.round((run.store_bytes ?? 0) / n) : null,
    wasted_attempts_per_call: run.provider ? Number((run.provider.rejected / Math.max(1, run.provider.accepted)).toFixed(3)) : null,
    restart_ready_ms: restart?.ready_ms ?? null,
    restart_first_call_ms: restart?.first_call_ms ?? null,
    restart_list_ms: restart?.list_ms ?? null,
    root_bytes_per_child: n ? Math.round((run.provider?.root_bytes ?? 0) / n) : null,
    root_calls: run.provider?.root_calls ?? null,
    cloudflare_usd_estimate: Number(cloudflare.toFixed(5)),
    cloudflare_usd_per_10k: n ? Number((cloudflare / n * 10000).toFixed(4)) : null,
    peak_workerd_rss_mb: run.peak_workerd_rss_mb ?? null,
  };
}

const scenario = values.scenario;
const AGENTS = Number(values.agents);
const BATCH = Number(values.batch);
const provider_counts = { accepted: 0, rejected: 0, root_calls: 0, root_bytes: 0, child_calls: 0, after_restart: 0, child_after_restart: 0 };
const children = new Map();
const wrapped = new Map();
let firstCall = null, lastCall = null, restartedAt = null, firstAfterRestart = null, doneCalls = 0;
let tokens = RATE, refilled = performance.now();
let killedDone = new Set(), inFlightAtKill = new Set(), repeatedDone = 0;

const text = content => typeof content === "string" ? content : (content ?? []).filter(block => block.type === "text").map(block => block.text).join("\n");
const results = content => typeof content === "string" ? [] : (content ?? []).filter(block => block.type === "tool_result");
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function sse(input, blocks, stop) {
  const n = provider_counts.accepted;
  const events = [["message_start", { type: "message_start", message: { id: "message-" + n, type: "message", role: "assistant", model: input.model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 8, output_tokens: 0 } } }]];
  blocks.forEach((block, index) => {
    if (block.type === "text") {
      events.push(["content_block_start", { type: "content_block_start", index, content_block: { type: "text", text: "" } }]);
      events.push(["content_block_delta", { type: "content_block_delta", index, delta: { type: "text_delta", text: block.text } }]);
    } else {
      events.push(["content_block_start", { type: "content_block_start", index, content_block: { type: "tool_use", id: block.id, name: block.name, input: {} } }]);
      events.push(["content_block_delta", { type: "content_block_delta", index, delta: { type: "input_json_delta", partial_json: JSON.stringify(block.input) } }]);
    }
    events.push(["content_block_stop", { type: "content_block_stop", index }]);
  });
  events.push(["message_delta", { type: "message_delta", delta: { stop_reason: stop, stop_sequence: null }, usage: { output_tokens: 4 } }]);
  events.push(["message_stop", { type: "message_stop" }]);
  return new Response(events.map(([event, data]) => "event: " + event + "\ndata: " + JSON.stringify(data) + "\n\n").join(""), { headers: { "content-type": "text/event-stream" } });
}
const reply = (input, value) => sse(input, [{ type: "text", text: value }], "end_turn");
const use = (input, name, value) => sse(input, [{ type: "tool_use", id: "toolu_" + provider_counts.accepted + "_" + Math.random().toString(36).slice(2), name, input: value }], "tool_use");
function invoke(input, name, value) {
  const id = "toolu_" + provider_counts.accepted + "_" + Math.random().toString(36).slice(2);
  if ((input.tools ?? []).some(entry => entry.name === name)) return sse(input, [{ type: "tool_use", id, name, input: value }], "tool_use");
  wrapped.set(id, name);
  return sse(input, [{ type: "tool_use", id, name: "exec", input: { code: "text(JSON.stringify(await tools." + name + "(" + JSON.stringify(value) + ")));" } }], "tool_use");
}

function admit() {
  if (RATE <= 0) return true;
  const now = performance.now();
  tokens = Math.min(RATE, tokens + (now - refilled) / 1000 * RATE);
  refilled = now;
  if (tokens < 1) return false;
  tokens -= 1;
  return true;
}

const provider = async request => {
  const body = await request.text();
  if (!admit()) {
    provider_counts.rejected++;
    return new Response(JSON.stringify({ type: "error", error: { type: "rate_limit_error", message: "rate limited by the hill-climb mock" } }), { status: 429, headers: { "content-type": "application/json", "retry-after": "1" } });
  }
  if (LATENCY > 0) await sleep(LATENCY);
  const input = JSON.parse(body);
  provider_counts.accepted++;
  const now = performance.now();
  firstCall ??= now;
  lastCall = now;
  if (restartedAt !== null) { provider_counts.after_restart++; firstAfterRestart ??= now - restartedAt; }
  const messages = input.messages ?? [];
  const first = text(messages[0]?.content);
  const boundary = messages.findLastIndex(message => message.role === "assistant");
  const trailing = messages.slice(boundary + 1);
  const answered = trailing.flatMap(message => results(message.content));
  const uses = new Map(messages.flatMap(message => Array.isArray(message.content) ? message.content.filter(block => block.type === "tool_use").map(block => [block.id, wrapped.get(block.id) ?? block.name]) : []));
  const answeredText = answered.map(block => typeof block.content === "string" ? block.content : text(block.content)).join("\n");
  const running = /Script running with cell ID (\S+?)[.\s"\\]/.exec(answeredText + " ");
  const child = /Your agent ID is ([^\s.]+)\./.exec(first);
  if (child) {
    const id = child[1];
    provider_counts.child_calls++;
    const entry = children.get(id) ?? { first: now, done: null, calls: 0 };
    children.set(id, entry);
    entry.calls++;
    if (restartedAt !== null) {
      provider_counts.child_after_restart++;
      if (killedDone.has(id)) repeatedDone++;
    }
    if (running) return use(input, "wait", { cell_id: running[1], yield_time_ms: 30000 });
    if (answered.length) {
      if ((uses.get(answered[0].tool_use_id) ?? "").endsWith("submit_result") || answeredText.includes("accepted")) { entry.done ??= now; doneCalls++; }
      return reply(input, "done");
    }
    const name = (input.tools ?? []).map(entry => entry.name).find(name => name === "submit_result" || name?.endsWith("__submit_result")) ?? "submit_result";
    return invoke(input, name, { output: "ok" });
  }
  provider_counts.root_calls++;
  provider_counts.root_bytes += body.length;
  if (running) return use(input, "wait", { cell_id: running[1], yield_time_ms: 30000 });
  const prompt = trailing.map(message => text(message.content)).filter(Boolean).join("\n");
  if (answered.length) return reply(input, "spawned");
  if (prompt.includes("<subagent_completion")) return reply(input, "ack");
  if (prompt.includes("HILLCLIMB")) {
    const code = "const n = " + AGENTS + ", b = " + BATCH + "; for (let i = 0; i < n; i += b) { await Promise.all(Array.from({ length: Math.min(b, n - i) }, (_, j) => tools.spawn_agent({ role: 'worker', task: 'Return the string ok-' + (i + j) + '.', harness: null, model: null, thinking: null, reasoning_mode: null, output_contract: { kind: 'string' } }))); } text('spawned ' + n);";
    return use(input, "exec", { code });
  }
  return reply(input, "ack");
};

function workerdRss() {
  let total = 0;
  for (const pid of readdirSync("/proc").filter(entry => /^\d+$/.test(entry))) {
    try {
      if (!readFileSync("/proc/" + pid + "/cmdline", "utf8").includes("workerd")) continue;
      const rss = /VmRSS:\s+(\d+)/.exec(readFileSync("/proc/" + pid + "/status", "utf8"));
      if (rss) total += Number(rss[1]) * 1024;
    } catch {}
  }
  return total;
}
function diskBytes(path) {
  let total = 0;
  const walk = dir => { for (const entry of readdirSync(dir, { withFileTypes: true })) { const full = dir + "/" + entry.name; if (entry.isDirectory()) walk(full); else total += readFileSync(full).length; } };
  try { walk(path); } catch {}
  return total;
}

const persistence = "output/hillclimb-state-" + scenario + "-" + Date.now();
const options = localOptions({ provider, persistence });
let mf = new Miniflare(convertV4MiniflareOptions(options));
const base = "http://worker/sessions/" + values.label + "-" + scenario + "-" + Date.now();
const headers = { authorization: "Bearer local-measurement", "content-type": "application/json" };
const request = async (url, init = {}) => mf.dispatchFetch(url, { ...init, headers });
const metrics = async () => (await request(base + "/metrics")).json();
const completedIds = async () => {
  const listing = await (await request(base + "/subagents")).json();
  return { all: (listing.agents ?? []).length, ids: (listing.agents ?? []).map(agent => String(agent.agent_id)), completed: new Set((listing.agents ?? []).filter(agent => agent.status?.state === "completed").map(agent => String(agent.agent_id))) };
};
let pollErrors = 0;
async function until(condition, detail) {
  const start = performance.now();
  let failing = null;
  for (;;) {
    const met = await Promise.resolve().then(condition).then(value => { failing = null; return value; }, error => { pollErrors++; failing ??= performance.now(); if (performance.now() - failing > 60000) throw new Error(detail + ": the Worker stopped answering: " + error.message); return false; });
    if (met) return;
    if (performance.now() - start > LIMIT_MS) throw new Error(detail);
    await sleep(200);
  }
}

const report = { scenario, agents: AGENTS, batch: BATCH, rate: RATE, latency_ms: LATENCY, restarts: [] };
const parent = { peak_wasm_bytes: 0, peak_requests_per_s: 0, requests: 0, child_requests: 0, rows_written: 0 };
let peakRss = 0, lastRequests = null, lastSample = null, carried = { requests: 0, child_requests: 0, rows_written: 0 }, current = { requests: 0, child_requests: 0, rows_written: 0 };
let sampling = false;
const sampler = setInterval(async () => {
  if (sampling) return;
  sampling = true;
  peakRss = Math.max(peakRss, workerdRss());
  try {
    const m = await metrics();
    parent.peak_wasm_bytes = Math.max(parent.peak_wasm_bytes, m.runtime?.wasm_memory_bytes ?? 0);
    current = { requests: m.requests ?? 0, child_requests: m.child_requests ?? 0, rows_written: m.sql?.rows_written ?? 0 };
    const now = performance.now();
    if (lastRequests !== null && current.requests >= lastRequests) parent.peak_requests_per_s = Math.max(parent.peak_requests_per_s, Math.round((current.requests - lastRequests) / ((now - lastSample) / 1000)));
    lastRequests = current.requests;
    lastSample = now;
  } catch {}
  sampling = false;
}, 2000);

async function kill() {
  const listing = await completedIds();
  killedDone = listing.completed;
  for (const id of listing.ids.filter(id => !killedDone.has(id))) inFlightAtKill.add(id);
  const spawnedBefore = listing.all;
  const m = await metrics().catch(() => null);
  if (m) carried = { requests: carried.requests + (m.requests ?? 0), child_requests: carried.child_requests + (m.child_requests ?? 0), rows_written: carried.rows_written + (m.sql?.rows_written ?? 0) };
  current = { requests: 0, child_requests: 0, rows_written: 0 };
  lastRequests = null;
  await mf.dispose();
  mf = new Miniflare(convertV4MiniflareOptions(options));
  restartedAt = performance.now();
  firstAfterRestart = null;
  await mf.ready;
  const ready = performance.now() - restartedAt;
  const listStart = performance.now();
  const after = await completedIds();
  const restart = { ready_ms: Math.round(ready), list_ms: Math.round(performance.now() - listStart), listed: after.all, spawned_before: spawnedBefore, done_before: killedDone.size, in_flight_before: inFlightAtKill.size };
  report.restarts.push(restart);
  return restart;
}

const started = performance.now();
try {
  await mf.ready;
  const submitted = await request(base + "/turns", { method: "POST", body: JSON.stringify({ request_id: "hillclimb", prompt: "HILLCLIMB " + AGENTS }) });
  if (submitted.status !== 202) throw new Error("submit " + submitted.status + " " + await submitted.text());
  const doneCount = () => [...children.values()].filter(entry => entry.done !== null).length;
  const points = { spawn: [() => children.size >= AGENTS * 0.1], turn: [() => doneCount() >= AGENTS * 0.5], twice: [() => doneCount() >= AGENTS / 3, () => doneCount() >= AGENTS * 2 / 3], score: [], done: [] }[scenario];
  if (!points) throw new Error("unknown scenario " + scenario);
  for (const point of points) {
    await until(point, "kill point not reached");
    const restart = await kill();
    await until(() => firstAfterRestart !== null, "no model call after restart");
    restart.first_call_ms = Math.round(firstAfterRestart);
  }
  await until(async () => (await completedIds()).completed.size >= AGENTS, "children did not all finish");
  await until(async () => ["completed", "failed", "cancelled"].includes((await (await request(base + "/turns/hillclimb")).json()).status), "root turn did not settle");
  report.wall_ms = Math.round(performance.now() - started);
  if (scenario === "done") {
    await sleep(Number(process.env.SETTLE_MS ?? 5000));
    const restart = await kill();
    await sleep(Number(process.env.SETTLE_MS ?? 5000) * 2);
    restart.first_call_ms = firstAfterRestart === null ? null : Math.round(firstAfterRestart);
    restart.calls_after = provider_counts.child_after_restart;
  }
  const final = await completedIds();
  const m = await metrics();
  current = { requests: m.requests ?? 0, child_requests: m.child_requests ?? 0, rows_written: m.sql?.rows_written ?? 0 };
  parent.peak_wasm_bytes = Math.max(parent.peak_wasm_bytes, m.runtime?.wasm_memory_bytes ?? 0);
  report.final_wasm_bytes = m.runtime?.wasm_memory_bytes ?? null;
  report.completed = final.completed.size;
  report.listed = final.all;
  const repeats = [...children].filter(([id, entry]) => entry.calls > 2 && !inFlightAtKill.has(id)).length;
  report.gate = {
    pass: final.completed.size === AGENTS && final.all === AGENTS && repeatedDone === 0 && repeats === 0 && (scenario === "score" || report.restarts.every(restart => restart.listed >= restart.spawned_before)) && (scenario !== "done" || provider_counts.child_after_restart === 0),
    lost: AGENTS - final.completed.size,
    repeated_after_done: repeatedDone,
    repeated_outside_in_flight: repeats,
    child_calls_after_restart: provider_counts.child_after_restart,
  };
} catch (error) {
  report.failure = error instanceof Error ? error.message : String(error);
  report.gate = { pass: false, failure: report.failure };
} finally {
  clearInterval(sampler);
  parent.requests = carried.requests + current.requests;
  parent.child_requests = carried.child_requests + current.child_requests;
  parent.rows_written = carried.rows_written + current.rows_written;
  report.parent = parent;
  report.provider = provider_counts;
  report.window_ms = firstCall !== null && lastCall !== null ? Math.round(lastCall - firstCall) : null;
  report.children = children.size;
  report.child_active_ms = [...children.values()].reduce((total, entry) => total + ((entry.done ?? lastCall ?? entry.first) - entry.first + LATENCY), 0);
  report.peak_workerd_rss_mb = Math.round(peakRss / 1e6);
  report.poll_errors = pollErrors;
  report.store_bytes = diskBytes(persistence);
  await mf.dispose().catch(() => {});
}
writeFileSync(process.env.HILLCLIMB_OUT ?? "output/hillclimb/" + values.label + "-" + scenario + ".json", JSON.stringify(report, null, 1));

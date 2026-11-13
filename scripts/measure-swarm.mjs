import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { appendFileSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { parseArgs } from "node:util";
import { localOptions } from "./local-runtime.mjs";

const { values } = parseArgs({ options: { agents: { type: "string", default: "1000" }, batch: { type: "string", default: "50" }, kill: { type: "string", default: "0" }, label: { type: "string", default: "swarm" }, limit: { type: "string", default: "3600" } } });
const AGENTS = Number(values.agents);
const BATCH = Number(values.batch);
const KILL = Number(values.kill);
const LIMIT_MS = Number(values.limit) * 1000;
const counts = { calls: 0, root: 0, child_initial: 0, child_submitted: 0, child_done: 0, completions: 0, waits: 0, after_restart: 0 };
const childIds = new Set();
let firstAfterRestart = null;
let execAt = null;
const childFirst = [];
let restartedAt = null;
const wrapped = new Map();

const text = content => typeof content === "string" ? content : (content ?? []).filter(block => block.type === "text").map(block => block.text).join("\n");
const results = content => typeof content === "string" ? [] : (content ?? []).filter(block => block.type === "tool_result");

function sse(input, blocks, stop) {
  const n = counts.calls;
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
const use = (input, name, value) => sse(input, [{ type: "tool_use", id: "toolu_" + counts.calls, name, input: value }], "tool_use");
function invoke(input, name, value) {
  const id = "toolu_" + counts.calls;
  if ((input.tools ?? []).some(entry => entry.name === name)) return sse(input, [{ type: "tool_use", id, name, input: value }], "tool_use");
  wrapped.set(id, name);
  return sse(input, [{ type: "tool_use", id, name: "exec", input: { code: "text(JSON.stringify(await tools." + name + "(" + JSON.stringify(value) + ")));" } }], "tool_use");
}

const provider = async request => {
  const input = await request.json();
  counts.calls++;
  if (restartedAt !== null) { counts.after_restart++; if (firstAfterRestart === null) firstAfterRestart = performance.now() - restartedAt; }
  const messages = input.messages ?? [];
  const first = text(messages[0]?.content);
  const boundary = messages.findLastIndex(message => message.role === "assistant");
  const trailing = messages.slice(boundary + 1);
  const answered = trailing.flatMap(message => results(message.content));
  const uses = new Map(messages.flatMap(message => Array.isArray(message.content) ? message.content.filter(block => block.type === "tool_use").map(block => [block.id, wrapped.get(block.id) ?? block.name]) : []));
  const answeredText = answered.map(block => typeof block.content === "string" ? block.content : text(block.content)).join("\n");
  const running = /Script running with cell ID (\S+?)[.\s"\\]/.exec(answeredText + " ");
  if (running) { counts.waits++; return use(input, "wait", { cell_id: running[1], yield_time_ms: 30000 }); }
  const child = /Your agent ID is ([^\s.]+)\./.exec(first);
  if (child) {
    if (!childIds.has(child[1])) childFirst.push(performance.now());
    childIds.add(child[1]);
    if (answered.length) {
      if (process.env.TRACE_INITIAL && restartedAt !== null) appendFileSync(process.env.TRACE_INITIAL, JSON.stringify({ at: performance.now(), restarted: true, after: true, child: child[1], n: messages.length, roles: messages.map(m => m.role + ":" + (Array.isArray(m.content) ? m.content.map(b => b.type).join("+") : "text")).join(" "), last: answeredText.slice(0, 200) }) + String.fromCharCode(10));
      if ((uses.get(answered[0].tool_use_id) ?? "").endsWith("submit_result") || answeredText.includes("accepted")) counts.child_done++;
      return reply(input, "done");
    }
    counts.child_initial++;
    if (process.env.TRACE_INITIAL) appendFileSync(process.env.TRACE_INITIAL, JSON.stringify({ at: performance.now(), restarted: restartedAt !== null, child: child[1], n: messages.length, last: text(messages.at(-1)?.content).slice(0, 3000) }) + String.fromCharCode(10));
    const name = (input.tools ?? []).map(entry => entry.name).find(name => name === "submit_result" || name?.endsWith("__submit_result")) ?? "submit_result";
    counts.child_submitted++;
    return invoke(input, name, { output: "ok" });
  }
  counts.root++;
  const prompt = trailing.map(message => text(message.content)).filter(Boolean).join("\n");
  if (answered.length && process.env.RELEASE && !answeredText.includes("released")) return use(input, "exec", { code: "text('released')" });
  if (answered.length) return reply(input, "spawned");
  if (prompt.includes("<subagent_completion")) { counts.completions++; return reply(input, "ack"); }
  if (prompt.includes("SWARM-DO")) {
    if (execAt === null) execAt = performance.now();
    const code = "const n = " + AGENTS + ", b = " + BATCH + "; for (let i = 0; i < n; i += b) { await Promise.all(Array.from({ length: Math.min(b, n - i) }, (_, j) => tools.spawn_agent({ role: 'worker', task: 'Return the string ok-' + (i + j) + '.', harness: null, model: null, thinking: null, reasoning_mode: null, output_contract: { kind: 'string' } }))); } text('spawned ' + n);" + (process.env.ROOT_WAIT ? " const pending = new Set(Array.from({ length: n }, (_, i) => i + 1)); const terminal = new Set(['completed', 'failed', 'closed', 'interrupted']); while (pending.size) { const r = await tools.wait_agent({ agent_ids: [...pending].slice(0, " + Number(process.env.ROOT_WAIT) + "), timeout_ms: 60000 }); for (const a of r.agents) if (terminal.has(a.status.state)) pending.delete(a.agent_id); } text('waited');" : "");
    return use(input, "exec", { code });
  }
  return reply(input, "ack");
};

const persistence = "output/swarm-state-" + Date.now();
const options = localOptions({ provider, persistence });
let mf = new Miniflare(convertV4MiniflareOptions(options));
const name = values.label + "-" + Date.now();
const base = "http://worker/sessions/" + name;
const headers = { authorization: "Bearer local-measurement", "content-type": "application/json" };
const request = async (url, init = {}) => mf.dispatchFetch(url, { ...init, headers });
const metrics = async () => (await request(base + "/metrics")).json();

function workerdRss() {
  let total = 0;
  for (const pid of readdirSync("/proc").filter(entry => /^\d+$/.test(entry))) {
    try {
      const cmd = readFileSync("/proc/" + pid + "/cmdline", "utf8");
      if (!cmd.includes("workerd")) continue;
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
let peakRss = 0;
const sampler = setInterval(() => { peakRss = Math.max(peakRss, workerdRss()); }, 250);

async function until(condition, detail) {
  const start = performance.now();
  while (!await condition()) {
    if (performance.now() - start > LIMIT_MS) throw new Error(detail);
    await new Promise(resolve => setTimeout(resolve, 20));
  }
  return performance.now() - start;
}

const report = { agents: AGENTS, batch: BATCH, kill: KILL };
const started = performance.now();
let last = performance.now();
let lastCalls = -1, stalledSince = performance.now();
const progress = setInterval(async () => {
  if (counts.calls !== lastCalls) { lastCalls = counts.calls; stalledSince = performance.now(); }
  else if (performance.now() - stalledSince > Number(process.env.STALL_MS ?? 60000)) {
    try {
      const listing = await (await request(base + "/subagents")).json();
      const states = {};
      for (const agent of listing.agents ?? []) { const state = typeof agent.status === "string" ? agent.status : Object.keys(agent.status ?? {})[0] ?? agent.status?.state; states[JSON.stringify(agent.status?.state ?? state)] = (states[JSON.stringify(agent.status?.state ?? state)] ?? 0) + 1; }
      const m = await metrics();
      const turn = await (await request(base + "/turns/swarm")).json();
      console.error("STALLED " + JSON.stringify({ states, alarm_at: m.alarm_at, now: Date.now(), resident: m.resident_runtimes, turn: turn.status, counts, runtime: m.runtime }).slice(0, 3000));
    } catch (error) { console.error("STALLED inspect failed " + error); }
    process.exit(2);
  } let m = {}; try { m = await metrics(); } catch {} if (m.runtime?.heap?.sites) writeFileSync("output/heap-sites.json", JSON.stringify({ at: counts.child_done, live: m.runtime.heap.live, sites: m.runtime.heap.sites, fold: m.runtime.heap.fold }, null, 1)); console.error("wasm " + ((m.runtime?.wasm_memory_bytes ?? 0) / 1e6).toFixed(0) + " MB live " + ((m.runtime?.heap?.live ?? 0) / 1e6).toFixed(1) + " MB allocs " + m.runtime?.heap?.count + " buckets " + JSON.stringify((m.runtime?.heap?.buckets ?? []).map(b => Math.round(b / 1e5) / 10)) + " resident " + m.resident_runtimes + " db " + ((m.database_bytes ?? 0) / 1e6).toFixed(1) + " MB"); console.error(((performance.now() - started) / 1000).toFixed(1) + "s calls " + counts.calls + " children " + childIds.size + " done " + counts.child_done + " completions " + counts.completions + " rss " + (workerdRss() / 1e6).toFixed(0) + " MB"); }, 10000);
try {
  await mf.ready;
  const submitted = await request(base + "/turns", { method: "POST", body: JSON.stringify({ request_id: "swarm", prompt: "SWARM-DO " + AGENTS }) });
  if (submitted.status !== 202) throw new Error("submit " + submitted.status + " " + await submitted.text());
  if (KILL > 0) {
    report.until_kill_ms = await until(() => counts.child_initial >= KILL, "kill point not reached");
    const before = await metrics();
    report.before_kill = { child_done: counts.child_done, children: childIds.size, calls: counts.calls, sql: { statements: before.sql.statements, rows_written: before.sql.rows_written, rows_read: before.sql.rows_read }, rss_mb: workerdRss() / 1e6 };
    const doneAtKill = counts.child_done;
    if (process.env.KILL_DELAY_MS) await new Promise(resolve => setTimeout(resolve, Number(process.env.KILL_DELAY_MS)));
    await mf.dispose();
    mf = new Miniflare(convertV4MiniflareOptions(options));
    restartedAt = performance.now();
    await mf.ready;
    report.restart_ready_ms = performance.now() - restartedAt;
    const listStart = performance.now();
    const listed = await request(base + "/subagents");
    const listing = await listed.json();
    report.restart_list_ms = performance.now() - listStart;
    report.restart_listed = listing.agents?.length;
    await until(() => firstAfterRestart !== null, "no model call after restart");
    report.restart_first_model_call_ms = firstAfterRestart;
  }
  report.all_done_ms = await until(async () => { await new Promise(resolve => setTimeout(resolve, 1000)); const listing = await (await request(base + "/subagents")).json(); return (listing.agents ?? []).filter(agent => JSON.stringify(agent.status).includes("completed")).length >= AGENTS; }, "children did not all finish");
  await until(async () => { const turn = await (await request(base + "/turns/swarm")).json(); return ["completed", "failed", "cancelled"].includes(turn.status); }, "root turn did not settle");
  report.settled_ms = performance.now() - started;
  const after = await metrics();
  report.heap_sites = after.runtime?.heap?.sites;
  report.heap_live = after.runtime?.heap?.live;
  report.fold = after.runtime?.heap?.fold;
  report.wasm_memory_bytes = after.runtime?.wasm_memory_bytes;
  report.sql = { statements: after.sql.statements, rows_written: after.sql.rows_written, rows_read: after.sql.rows_read };
  report.rows_written_per_agent = after.sql.rows_written / AGENTS;
  report.statements_per_agent = after.sql.statements / AGENTS;
  const top = Object.entries(after.sql.by_statement).sort((a, b) => b[1].rows_written - a[1].rows_written).slice(0, 8).map(([query, entry]) => ({ query, ...entry }));
  report.top_writes = top;
  report.param_bytes_per_agent = Object.values(after.sql.by_statement).reduce((total, entry) => total + entry.param_bytes, 0) / AGENTS;
} catch (error) {
  report.failure = error instanceof Error ? error.message : String(error);
  process.exitCode = 1;
} finally {
  clearInterval(sampler);
  clearInterval(progress);
  report.counts = counts;
  if (execAt !== null && childFirst.length) { const at = q => Math.round(childFirst[Math.min(childFirst.length - 1, Math.floor(q * childFirst.length))] - execAt); report.child_first_call_ms = { first: at(0), p50: at(0.5), last: at(1) }; }
  report.children = childIds.size;
  report.peak_workerd_rss_mb = peakRss / 1e6;
  report.store_bytes = diskBytes(persistence);
  report.store_bytes_per_agent = report.store_bytes / AGENTS;
  await mf.dispose().catch(() => {});
}
writeFileSync("output/" + values.label + "-" + AGENTS + ".json", JSON.stringify(report, null, 2));
console.log(JSON.stringify(report, null, 2));

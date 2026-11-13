import { appendFileSync, writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { parseArgs } from "node:util";

const { values } = parseArgs({ options: { origin: { type: "string" }, session: { type: "string" }, agents: { type: "string", default: "100" }, batch: { type: "string", default: "100" }, model: { type: "string", default: "claude-haiku-5-5" }, thinking: { type: "string", default: "low" }, every: { type: "string", default: "10000" }, out: { type: "string" } } });
const token = process.env.WORKER_TOKEN;
if (!token) throw new Error("Provide WORKER_TOKEN through the environment");
if (!values.origin || !values.session || !values.out) throw new Error("Pass --origin, --session and --out");
const AGENTS = Number(values.agents);
const BATCH = Number(values.batch);
const base = new URL("/sessions/" + encodeURIComponent(values.session), values.origin).href;
const headers = { authorization: "Bearer " + token, "content-type": "application/json" };
const started = performance.now();
const elapsed = () => Math.round(performance.now() - started);
const log = entry => appendFileSync(values.out, JSON.stringify({ at_ms: elapsed(), ...entry }) + String.fromCharCode(10));

const code = "const n = " + AGENTS + ", b = " + BATCH + "; const pending = new Set(); let spawnedAt = Date.now(); for (let i = 0; i < n; i += b) { const batch = await Promise.all(Array.from({ length: Math.min(b, n - i) }, (_, j) => tools.spawn_agent({ role: 'worker', task: 'Call submit_result with the string ok-' + (i + j) + ' and nothing else.', harness: null, model: null, thinking: null, reasoning_mode: null, output_contract: { kind: 'string' } }))); for (const s of batch) pending.add(s.agent_id); } spawnedAt = Date.now() - spawnedAt; const terminal = new Set(['completed', 'failed', 'closed', 'interrupted']); const counts = {}; while (pending.size) { const r = await tools.wait_agent({ agent_ids: [...pending].slice(0, 8), timeout_ms: 60000 }); for (const a of r.agents) if (terminal.has(a.status.state)) { pending.delete(a.agent_id); counts[a.status.state] = (counts[a.status.state] ?? 0) + 1; } } text(JSON.stringify({ spawned_ms: spawnedAt, counts }));";
const prompt = "Run exactly this JavaScript in one exec cell without changing it. While it runs, keep calling wait on the cell until it completes. Then reply with its output only.\n\n" + code;

const request = async (path, init = {}) => {
  const response = await fetch(base + path, { ...init, headers });
  const text = await response.text();
  try { return { status: response.status, body: JSON.parse(text) }; } catch { return { status: response.status, body: text.slice(0, 500) }; }
};

const submitted = await request("/turns", { method: "POST", body: JSON.stringify({ request_id: "swarm", prompt, provider: "claude", model: values.model, thinking: values.thinking }) });
log({ submitted });
if (submitted.status >= 300) process.exit(1);

for (;;) {
  await new Promise(resolve => setTimeout(resolve, Number(values.every)));
  const t0 = performance.now();
  const list = await request("/subagents").catch(error => ({ status: 0, body: String(error) }));
  const list_ms = Math.round(performance.now() - t0);
  const agents = Array.isArray(list.body?.agents) ? list.body.agents : [];
  const states = {};
  let cost = 0, input = 0, cached = 0, output = 0, turns = 0;
  for (const agent of agents) {
    const state = agent.status?.state ?? String(agent.status);
    states[state] = (states[state] ?? 0) + 1;
    cost += Number(agent.usage?.cost_usd ?? 0);
    input += agent.usage?.input_tokens ?? 0;
    cached += agent.usage?.cached_input_tokens ?? 0;
    output += agent.usage?.output_tokens ?? 0;
    turns += agent.usage?.completed_turns ?? 0;
  }
  const metrics = await request("/metrics").catch(error => ({ status: 0, body: String(error) }));
  const runtime = metrics.body?.runtime ?? {};
  const turn = await request("/turns/swarm").catch(error => ({ status: 0, body: String(error) }));
  const entry = { list_status: list.status, list_ms, agents: agents.length, states, children_cost_usd: Number(cost.toFixed(4)), input_tokens: input, cached_input_tokens: cached, output_tokens: output, child_turns: turns, parent_wasm_bytes: runtime.wasm_memory_bytes ?? null, resident: metrics.body?.resident_runtimes ?? null, database_bytes: metrics.body?.database_bytes ?? null, sql: metrics.body?.sql ? { statements: metrics.body.sql.statements, rows_written: metrics.body.sql.rows_written, rows_read: metrics.body.sql.rows_read } : null, turn_status: turn.body?.status ?? turn.status, turn_usage: turn.body?.ok?.usage ?? null };
  log(entry);
  console.log(JSON.stringify(entry));
  if (turn.body?.status && !["queued", "running", "pending"].includes(turn.body.status)) { log({ final: turn.body }); writeFileSync(values.out + ".final.json", JSON.stringify(turn.body, null, 1)); break; }
}

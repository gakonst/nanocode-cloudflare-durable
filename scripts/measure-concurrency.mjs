import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { parseArgs } from "node:util";

const { values } = parseArgs({ options: { levels: { type: "string", default: "1,4,16,32" }, providers: { type: "string", default: "claude,codex" }, rounds: { type: "string", default: "2" }, scenario: { type: "string", default: "inference" }, origin: { type: "string", default: "https://nanocode-worker.gakonst.workers.dev" }, "timeout-seconds": { type: "string", default: "120" } } });
const levels = values.levels.split(",").map(Number);
const providers = values.providers.split(",");
const rounds = Number(values.rounds);
const timeout = Number(values["timeout-seconds"]);
const scenario = values.scenario;
const origin = new URL(values.origin);
if (!levels.length || levels.some(level => !Number.isSafeInteger(level) || level < 1 || level > 64) || new Set(levels).size !== levels.length) throw new Error("Use distinct concurrency levels between 1 and 64");
if (!providers.length || providers.some(provider => !["claude", "codex"].includes(provider)) || new Set(providers).size !== providers.length) throw new Error("Use claude,codex or one provider");
if (!Number.isSafeInteger(rounds) || rounds < 1 || rounds > 5 || !Number.isSafeInteger(timeout) || timeout < 10 || timeout > 600) throw new Error("Use 1–5 rounds and a 10–600 second timeout");
if (!["inference", "tools", "fanout", "queue"].includes(scenario)) throw new Error("Unknown scenario");
if (origin.username || origin.password || origin.search || origin.hash || origin.pathname !== "/" || origin.protocol !== "https:") throw new Error("Use an HTTPS origin");
const token = process.env.WORKER_TOKEN;
if (!token || /[\r\n]/.test(token)) throw new Error("Provide WORKER_TOKEN through the environment");
const run = randomUUID();
const report = { measured_at: new Date().toISOString(), origin: origin.origin, run, scenario, providers, rounds, requested_levels: levels, levels: [], deployment: undefined, limitations: ["Concurrency levels are per provider; mixed-provider request concurrency is their sum. Fanout levels count requested children per provider in one session.", "curl TTFT includes process startup, DNS, fresh TLS, network, durable admission, inference, and delivery. HTTP headers and tool arguments do not count as answer text.", "This is closed-loop burst load with synchronized rounds, not sustained open-loop capacity or a global maximum.", "A new session is not proof of a cold isolate. Independent sessions use independent Durable Objects; queued roots in one session execute serially.", "Model concurrency is observed from client-received events and can undercount actual overlap when events are buffered.", "Local-clock spans overlap. Cumulative storage and event waits must not be added to model durations.", "Reported server enqueue time precedes platform output gating and network delivery. Production clock resolution can hide CPU work.", "Private diagnostics are capped at the last 64 dispatches per runtime. Provider account limits and the shared remote Linux host can bound observed capacity."] };
try { report.deployment = JSON.parse(readFileSync("output/production-deployment.json", "utf8")); } catch {}
mkdirSync("output", { recursive: true });
const output = `output/concurrency-${scenario}-${Date.now()}.json`;
const save = () => writeFileSync(output, JSON.stringify(report, null, 2));
const numeric = value => Object.fromEntries(Object.entries(value ?? {}).flatMap(([key, item]) => typeof item === "number" || typeof item === "boolean" ? [[key, item]] : item && typeof item === "object" && !Array.isArray(item) ? [[key, numeric(item)]] : []));
const stats = values => {
  const sorted = values.filter(Number.isFinite).sort((a, b) => a - b);
  const quantile = p => sorted[Math.max(0, Math.ceil(sorted.length * p) - 1)];
  return sorted.length ? { count: sorted.length, p50: sorted.length % 2 ? sorted[(sorted.length - 1) / 2] : (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2, p95: quantile(0.95), p99: quantile(0.99), min: sorted[0], max: sorted.at(-1) } : { count: 0 };
};

function curl(route, { body, event, seconds = timeout } = {}) {
  const config = [`url = ${JSON.stringify(new URL(route, origin).href)}`, `max-time = ${seconds}`, "connect-timeout = 15", `header = ${JSON.stringify(`Authorization: Bearer ${token}`)}`, 'header = "Content-Type: application/json"', 'write-out = "%{stderr}%{json}\\n"', 'dump-header = "/dev/fd/3"'];
  if (event) config.push('header = "Accept: text/event-stream"');
  if (body !== undefined) config.push(`data = ${JSON.stringify(JSON.stringify(body))}`);
  const started = performance.now();
  const child = spawn("curl", ["--silent", "--show-error", "--no-buffer", "--config", "-"], { stdio: ["pipe", "pipe", "pipe", "pipe"] });
  let stdout = "", stderr = "", buffer = "", headers = "", parseErrors = 0;
  child.stdio[3].setEncoding("utf8");
  child.stdio[3].on("data", chunk => headers += chunk);
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", chunk => stderr += chunk);
  child.stdout.setEncoding("utf8");
  child.stdout.on("data", chunk => {
    if (!event) { stdout += chunk; return; }
    buffer += chunk.replace(/\r\n/g, "\n");
    let split;
    while ((split = buffer.indexOf("\n\n")) >= 0) {
      const frame = buffer.slice(0, split); buffer = buffer.slice(split + 2);
      const data = frame.split("\n").filter(line => line.startsWith("data:")).map(line => line.slice(5).trimStart()).join("\n");
      if (data) { try { event(JSON.parse(data), performance.now() - started); } catch { parseErrors++; } }
    }
  });
  child.stdin.on("error", () => {});
  child.stdin.end(config.join("\n") + "\n");
  return new Promise(resolve => {
    child.once("error", () => resolve({ exit_code: -1, error: "curl_spawn_failed" }));
    child.once("close", code => {
      let meta, value;
      try { meta = JSON.parse(stderr.trim().split("\n").at(-1)); } catch {}
      try { value = stdout ? JSON.parse(stdout) : undefined; } catch {}
      resolve({ exit_code: code, parse_errors: parseErrors, status: meta?.http_code, http_version: meta?.http_version, wall_ms: performance.now() - started, dns_ms: meta?.time_namelookup * 1000, tcp_ms: (meta?.time_connect - meta?.time_namelookup) * 1000, tls_ms: (meta?.time_appconnect - meta?.time_connect) * 1000, http_ttfb_ms: meta?.time_starttransfer * 1000, server_timing: Object.fromEntries([...headers.matchAll(/(?:^|[,\s])([a-z_]+);dur=([0-9.]+)/g)].map(([, key, value]) => [key, Number(value)])), value });
    });
  });
}

function prompt(session, index, level) {
  if (scenario === "tools") return `Use remote Code Mode for this synthetic benchmark. Do not delegate. In one exec, use tools.apply_patch to create ${session}-${index}.txt containing benchmark-ok; await Promise.all of three tools.exec_command calls running printf remote-tool-ok; call tools.list_agents({}); store("benchmark", "${session}"). In a separate exec, verify load("benchmark") equals "${session}" and read that file through tools.exec_command. Do not inspect secrets or unrelated files. Finish with exactly nanocode-worker-ok.`;
  if (scenario === "fanout") return `Synthetic concurrency benchmark. Start exactly ${level} child agents using spawn_agent, without waiting between spawns. Give each child output_contract {"kind":"string"}, task "Do not delegate, use workspace tools, or browse. Immediately submit_result with output nanocode-child-ok.", and inherit this provider/model. Keep your root turn alive until every child is completed, using wait_agent with all IDs and list_agents(include_completed=true). Check each structured output equals nanocode-child-ok. Do not spawn extra children. After all children complete, reply exactly nanocode-worker-ok.`;
  return "Reply exactly nanocode-worker-ok. Do not use tools or delegate.";
}

async function turn(session, round, index, wave) {
  const id = `r${round}-${index}`;
  const sample = { session: session.name, provider: session.provider, executor: session.executor, request_id: id, kind: round ? "warm-session" : "new-session", started_ms: performance.now() - wave.started, events: [], sdk_calls: [], tools: [], children: [] };
  wave.samples.push(sample);
  wave.active++; wave.peak_requests = Math.max(wave.peak_requests, wave.active);
  const transport = await curl(`/sessions/${session.name}/turns`, { body: { request_id: id, prompt: prompt(session.name, round, wave.level), provider: session.provider, ...(session.executor ? { executor: session.executor } : {}) }, event: (envelope, elapsed) => {
    const outer = envelope.event;
    const child = outer?.source === "subagent" ? outer.update : undefined;
    const sdk = outer?.source === "agent" ? outer.event : child?.type === "event" ? child.event : undefined;
    const type = sdk?.type ?? outer?.type;
    const actor = child ? `child:${child.id ?? child.agent?.id ?? "unknown"}` : "root";
    if (child && ["added", "status", "completion"].includes(child.type)) sample.children.push({ type: child.type, id: child.id ?? child.agent?.id, status: child.status, received_ms: elapsed });
    if (!type) return;
    const payload = sdk?.payload;
    const event = { type, actor, received_ms: elapsed, server_at: envelope.created_at, server_seq: envelope.seq, streamed_at: envelope.streamed_at, host: outer?.host_timing, sdk_timing: outer?.sdk_timing, seq: sdk?.seq, request_id: sdk?.request_id, call_index: payload?.model_call_index ?? payload?.call_index, model_elapsed_ns: payload?.model_elapsed_ns };
    sample.events.push(event);
    const modelKey = `${session.name}:${actor}:${sdk?.request_id}:${payload?.call_index}`;
    if (type === "model.call.started") { wave.models.add(modelKey); wave.peak_models = Math.max(wave.peak_models, wave.models.size); }
    if (["model.call.completed", "model.call.failed"].includes(type)) { wave.models.delete(modelKey); sample.sdk_calls.push({ actor, type, received_ms: elapsed, ...numeric(payload) }); }
    if (type === "tool.result") sample.tools.push({ actor, tool: payload?.tool, status: payload?.status, duration_ms: typeof payload?.duration_ns === "number" ? payload.duration_ns / 1e6 : undefined });
    if (actor === "root" && ["assistant.delta", "reasoning.summary.delta"].includes(type) && payload?.text) {
      sample.first_output_ms ??= elapsed; sample.first_output ??= event;
      if (type === "assistant.delta") sample.ttft_ms ??= elapsed;
      else sample.first_reasoning_ms ??= elapsed;
    }
    if (outer?.type === "result") { sample.status = outer.status; sample.completion_ms = elapsed; sample.response_matches = /^nanocode-worker-ok\.?$/.test(outer.ok?.message ?? ""); sample.worker_timing = outer.timing; }
  } });
  wave.active--; sample.finished_ms = performance.now() - wave.started;
  const { value, ...safeTransport } = transport; sample.curl = safeTransport;
  if (!sample.status) {
    sample.status = "stream_failed";
    const state = await curl(`/sessions/${session.name}/turns/${id}`, { seconds: 15 });
    sample.saved_status = state.value?.status;
    if (!["completed", "failed", "cancelled"].includes(state.value?.status)) {
      const cancelled = await curl(`/sessions/${session.name}/turns/${id}/cancel`, { body: {}, seconds: 30 });
      sample.cleanup = { http_status: cancelled.status, saved_status: cancelled.value?.status };
    }
  }
  const measured = await curl(`/sessions/${session.name}/metrics`, { seconds: 30 });
  sample.metrics = measured.value;
  sample.metrics_http_status = measured.status;
  if (scenario === "tools") {
    const count = name => sample.tools.filter(tool => tool.actor === "root" && tool.tool === name && tool.status === "completed").length;
    sample.workflow_valid = count("exec") >= 2 && count("exec_command") >= 4 && count("apply_patch") >= 1 && count("list_agents") >= 1;
  }
  if (scenario === "fanout") {
    const added = new Set(sample.children.filter(child => child.type === "added").map(child => child.id));
    const completed = new Set(sample.children.filter(child => child.status?.state === "completed" && child.status.output === "nanocode-child-ok").map(child => child.id));
    sample.child_count = added.size;
    sample.completed_children = completed.size;
    sample.workflow_valid = added.size === wave.level && completed.size === wave.level;
  }
  return sample;
}

function summarize(wave, provider) {
  const samples = wave.samples.filter(sample => !provider || sample.provider === provider);
  const ok = samples.filter(sample => sample.status === "completed" && sample.response_matches && sample.workflow_valid !== false && sample.curl.exit_code === 0 && sample.curl.parse_errors === 0);
  const elapsed = Math.max(...samples.map(sample => sample.finished_ms)) - Math.min(...samples.map(sample => sample.started_ms));
  return { provider: provider ?? "combined", attempted: samples.length, completed: ok.length, failed: samples.length - ok.length, resident_reused: samples.filter(sample => sample.worker_timing?.runtime_reused).length, elapsed_ms: elapsed, completed_per_second: ok.length / (elapsed / 1000), ttft_ms: stats(ok.map(sample => sample.ttft_ms)), first_output_ms: stats(ok.map(sample => sample.first_output_ms)), completion_ms: stats(ok.map(sample => sample.completion_ms)), admission_ms: stats(ok.map(sample => sample.worker_timing?.admission_ms)), pump_wait_ms: stats(ok.map(sample => sample.worker_timing?.pump_wait_ms)), runtime_open_ms: stats(ok.map(sample => sample.worker_timing?.runtime_open_ms)), sdk_to_first_output_ms: stats(ok.filter(sample => sample.first_output?.call_index === 1).map(sample => sample.first_output.model_elapsed_ns / 1e6)), outside_first_call_ms: stats(ok.filter(sample => sample.first_output?.call_index === 1).map(sample => sample.first_output_ms - sample.first_output.model_elapsed_ns / 1e6)), failed_tools: samples.flatMap(sample => sample.tools).filter(tool => tool.status !== "completed").length };
}

for (const level of levels) {
  const entry = { level, sessions: [], waves: [] };
  report.levels.push(entry);
  for (const provider of providers) {
    const total = ["fanout", "queue"].includes(scenario) ? 1 : level;
    for (let index = 0; index < total; index++) entry.sessions.push({ name: `load-${scenario}-${run}-${level}-${provider}-${index}`, provider, ...(scenario === "tools" ? { executor: index % 2 ? "paradigm-b" : "paradigm-a" } : {}) });
  }
  for (let round = 0; round < rounds; round++) {
    const wave = { level, round, kind: round ? "warm-session" : "new-session", started: performance.now(), active: 0, peak_requests: 0, models: new Set(), peak_models: 0, samples: [] };
    const pending = entry.sessions.flatMap(session => Array.from({ length: scenario === "queue" ? level : 1 }, (_, index) => turn(session, round, index, wave)));
    await Promise.all(pending);
    const { started, active, models, ...saved } = wave;
    saved.launch_spread_ms = Math.max(...saved.samples.map(sample => sample.started_ms)) - Math.min(...saved.samples.map(sample => sample.started_ms));
    saved.summaries = [summarize(wave), ...providers.map(provider => summarize(wave, provider))];
    entry.waves.push(saved); save();
    console.log(JSON.stringify({ scenario, level, round, peak_requests: saved.peak_requests, peak_models: saved.peak_models, launch_spread_ms: saved.launch_spread_ms, summaries: saved.summaries }));
    if (saved.summaries[0].failed) break;
  }
  for (let index = 0; index < entry.sessions.length; index += 8) await Promise.all(entry.sessions.slice(index, index + 8).map(async session => {
    const result = await curl(`/sessions/${session.name}/metrics`, { seconds: 30 });
    session.metrics = result.value;
    session.metrics_http_status = result.status;
  }));
  const models = [...new Map(entry.waves.flatMap(wave => wave.samples.flatMap(sample => sample.metrics?.runtime?.timing?.models ?? [])).concat(entry.sessions.flatMap(session => session.metrics?.runtime?.timing?.models ?? [])).map(model => [`${model.runtime_id}:${model.dispatch_id}`, model])).values()];
  entry.dispatches = { count: models.length, status: Object.fromEntries([...new Set(models.map(model => model.status))].map(status => [status, models.filter(model => model.status === status).length])), credentials: Object.fromEntries([...new Set(models.map(model => model.diagnostic?.credential))].map(value => [value, models.filter(model => model.diagnostic?.credential === value).length])), auth_lookup_ms: stats(models.map(model => model.diagnostic?.auth_lookup_ms)), headers_ms: stats(models.map(model => model.headers_ms)), unfinished: entry.sessions.reduce((count, session) => count + Number(session.metrics?.persisted?.unfinished ?? 0), 0), recoveries: entry.sessions.reduce((count, session) => count + Number(session.metrics?.recoveries ?? 0), 0) };
  save();
  if (entry.waves.some(wave => wave.summaries[0].failed)) { report.stopped_after_failed_level = level; break; }
}
console.log(JSON.stringify({ report: output, scenario, levels: report.levels.map(entry => ({ level: entry.level, dispatches: entry.dispatches, waves: entry.waves.map(wave => ({ round: wave.round, peak_requests: wave.peak_requests, peak_models: wave.peak_models, summaries: wave.summaries })) })) }));

import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { parseArgs } from "node:util";

const { values } = parseArgs({ options: { provider: { type: "string", default: "claude" }, executor: { type: "string" }, samples: { type: "string", default: "6" }, tools: { type: "boolean", default: false }, reasoning: { type: "boolean", default: false }, origin: { type: "string", default: "https://nanocode-worker.gakonst.workers.dev" } } });
const origin = new URL(values.origin);
if (origin.username || origin.password || origin.search || origin.hash || origin.pathname !== "/" || (origin.protocol !== "https:" && !["127.0.0.1", "localhost"].includes(origin.hostname))) throw new Error("Pass an HTTPS origin or loopback address");
if (!["claude", "codex"].includes(values.provider)) throw new Error("Invalid provider");
if (values.executor !== undefined && !/^[a-z0-9][a-z0-9_-]{0,47}$/.test(values.executor)) throw new Error("Invalid executor");
if (values.tools && !values.executor) throw new Error("Tool measurements require --executor");
if (values.tools && values.reasoning) throw new Error("Choose tools or reasoning measurements");
const count = Number(values.samples);
if (!Number.isSafeInteger(count) || count < 1 || count > 20) throw new Error("samples must be between 1 and 20");
const token = process.env.WORKER_TOKEN;
if (!token || /[\r\n]/.test(token)) throw new Error("Provide WORKER_TOKEN through the environment");
const session = `curl-${values.provider}-${randomUUID()}`;
const path = `/sessions/${session}/turns`;
const report = { measured_at: new Date().toISOString(), origin: origin.origin, session, provider: values.provider, executor: values.executor, tools: values.tools, reasoning: values.reasoning, transport: "one curl POST with finite durable SSE per turn; fresh TLS connection", samples: [], limitations: ["TTFT starts immediately before spawning curl and ends at the first nonempty assistant text delta; HTTP headers and keepalives do not count.", "Includes curl startup, DNS, TLS, network, durable admission and provider latency.", "A new session does not establish a cold isolate or credential cache.", "Server clocks measure local elapsed intervals; independent monotonic origins are never subtracted. Cloudflare clock resolution can hide synchronous CPU work.", "Spans can overlap, especially across subagents; their totals must not be added as an end-to-end latency.", "streamed_at is enqueue time before any platform output gate or network delay, not client receipt time."] };
const summarize = values => {
  const sorted = [...values].sort((a, b) => a - b);
  if (!sorted.length) return { count: 0 };
  return { count: sorted.length, median: sorted.length % 2 ? sorted[Math.floor(sorted.length / 2)] : (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2, min: sorted[0], max: sorted.at(-1) };
};
mkdirSync("output", { recursive: true });
const output = `output/curl-${values.provider}-${values.executor ?? "inference"}-${values.tools ? "tools" : values.reasoning ? "reasoning" : "text"}-${Date.now()}.json`;
const save = () => writeFileSync(output, JSON.stringify(report, null, 2));

function curl(route, { body, accept, event } = {}) {
  const config = [`url = ${JSON.stringify(new URL(route, origin).href)}`, "max-time = 300", "connect-timeout = 15", `header = ${JSON.stringify(`Authorization: Bearer ${token}`)}`, 'header = "Content-Type: application/json"', 'write-out = "%{stderr}%{json}\\n"', 'dump-header = "/dev/fd/3"'];
  if (accept) config.push(`header = ${JSON.stringify(`Accept: ${accept}`)}`);
  if (body !== undefined) config.push(`data = ${JSON.stringify(JSON.stringify(body))}`);
  const start = performance.now();
  const child = spawn("curl", ["--silent", "--show-error", "--no-buffer", "--config", "-"], { stdio: ["pipe", "pipe", "pipe", "pipe"] });
  let stdout = "";
  let stderr = "";
  let buffer = "";
  let parseError;
  let responseHeaders = "";
  child.stdio[3].setEncoding("utf8");
  child.stdio[3].on("data", chunk => responseHeaders += chunk);
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", chunk => stderr += chunk);
  child.stdout.on("data", chunk => {
    if (!event) { stdout += chunk; return; }
    buffer += chunk;
    let split;
    while ((split = buffer.indexOf("\n\n")) >= 0) {
      const frame = buffer.slice(0, split);
      buffer = buffer.slice(split + 2);
      const data = frame.split("\n").filter(line => line.startsWith("data:")).map(line => line.slice(5).trimStart()).join("\n");
      if (!data) continue;
      try { event(JSON.parse(data), performance.now() - start); } catch { parseError = true; }
    }
  });
  child.stdin.end(config.join("\n") + "\n");
  return new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("close", code => {
      let meta;
      try { meta = JSON.parse(stderr.trim().split("\n").at(-1)); } catch {}
      if (code !== 0 || !meta || parseError) { reject(new Error(`curl measurement failed (${code})`)); return; }
      const serverTiming = Object.fromEntries([...responseHeaders.matchAll(/(?:^|[,\s])([a-z_]+);dur=([0-9.]+)/g)].map(([, name, duration]) => [name, Number(duration)]));
      const timing = { status: meta.http_code, http_version: meta.http_version, dns_ms: meta.time_namelookup * 1000, tcp_ms: (meta.time_connect - meta.time_namelookup) * 1000, tls_ms: (meta.time_appconnect - meta.time_connect) * 1000, http_ttfb_ms: meta.time_starttransfer * 1000, total_ms: meta.time_total * 1000, wall_ms: performance.now() - start, server_timing: serverTiming };
      let value;
      if (stdout) { try { value = JSON.parse(stdout); } catch { reject(new Error("Invalid measurement response")); return; } }
      resolve({ ...timing, body: value });
    });
  });
}

function numericFields(value) {
  return Object.fromEntries(Object.entries(value ?? {}).flatMap(([key, item]) => typeof item === "number" || typeof item === "boolean" ? [[key, item]] : item && typeof item === "object" && !Array.isArray(item) ? [[key, numericFields(item)]] : []));
}

for (let index = 0; index < count; index++) {
  const id = `turn-${index}`;
  const file = `${session}-${index}.txt`;
  const toolPrompt = `Use remote Code Mode and workspace tools for this synthetic benchmark. Do not delegate. In one exec, use tools.apply_patch to create ${file} containing benchmark-ok, and await Promise.all of three tools.exec_command calls, each running printf remote-tool-ok. Also call tools.list_agents({}) to exercise a callback to the brain. Store a value using store("benchmark", ${index}). In a separate exec, print load("benchmark") and read ${file} using tools.exec_command. Do not inspect secrets or unrelated files. Finish with exactly nanocode-worker-ok.`;
  const input = { request_id: id, prompt: values.tools ? toolPrompt : values.reasoning ? "Find the smallest positive integer n such that n is 4 modulo 17, 9 modulo 19, and 7 modulo 23. Give n and a concise verification. Do not use tools." : "Reply exactly nanocode-worker-ok.", provider: values.provider, ...(values.executor ? { executor: values.executor } : {}) };
  const sample = { request_id: id, kind: index ? "warm-session" : "new-session", events: [], model_calls: [], sdk_model_calls: [], tool_calls: [] };
  report.samples.push(sample);
  const stages = new Map();
  const transport = await curl(path, { body: input, accept: "text/event-stream", event: (envelope, elapsed) => {
    const outer = envelope.event;
    const sdk = outer?.source === "agent" ? outer.event : undefined;
    const type = sdk?.type ?? outer?.type;
    if (!type) return;
    sample.events.push({ type, received_ms: elapsed, server_at: envelope.created_at, streamed_at: envelope.streamed_at, host: outer?.host_timing, sdk_timing: outer?.sdk_timing, sdk_request_id: sdk?.request_id, sdk_seq: sdk?.seq, model_call_index: sdk?.payload?.model_call_index ?? sdk?.payload?.call_index, model_elapsed_ns: sdk?.payload?.model_elapsed_ns });
    if (["queued", "running", "model.call.started", "assistant.delta"].includes(type) && !stages.has(type)) stages.set(type, envelope.created_at);
    if (type === "model.call.started") sample.model_calls.push(elapsed);
    if (["model.call.completed", "model.call.failed"].includes(type)) sample.sdk_model_calls.push({ type, received_ms: elapsed, ...numericFields(sdk?.payload) });
    if (type.includes("tool") && !type.includes("delta")) sample.tool_calls.push({ type, received_ms: elapsed, server_at: envelope.created_at, tool: sdk?.payload?.tool, call_id: sdk?.payload?.call_id, parent_call_id: sdk?.payload?.parent_call_id, status: sdk?.payload?.status, duration_ms: typeof sdk?.payload?.duration_ns === "number" ? sdk.payload.duration_ns / 1e6 : undefined });
    if (["assistant.delta", "reasoning.summary.delta"].includes(type) && sdk.payload?.text) {
      sample.first_output_ms ??= elapsed;
      sample.first_output ??= sample.events.at(-1);
      if (type === "assistant.delta") sample.ttft_ms ??= elapsed;
      else { sample.first_reasoning_ms ??= elapsed; sample.reasoning_deltas = (sample.reasoning_deltas ?? 0) + 1; }
    }
    if (type === "result") {
      sample.status = outer.status;
      sample.completion_ms = elapsed;
      sample.response_matches = values.reasoning ? /7229/.test(outer.ok?.message ?? "") : /^nanocode-worker-ok\.?$/.test(outer.ok?.message ?? "");
      sample.worker_timing = outer.timing;
      if (outer.error) sample.error = outer.error;
    }
  } });
  sample.curl = transport;
  if (stages.has("model.call.started")) sample.server_setup_ms = stages.get("model.call.started") - stages.get("queued");
  if (stages.has("assistant.delta")) sample.server_model_start_to_text_ms = stages.get("assistant.delta") - stages.get("model.call.started");
  if (!sample.status) throw new Error("Finite stream closed without a result");
  if (index === 0) {
    const replay = await curl(path, { body: input });
    sample.replay = { status: replay.status, wall_ms: replay.wall_ms, saved: replay.body?.status === sample.status };
    const conflict = await curl(path, { body: { ...input, prompt: `${input.prompt} changed` } });
    sample.conflict_status = conflict.status;
  }
  save();
  console.log(JSON.stringify({ provider: values.provider, executor: values.executor, tools: values.tools, index, status: sample.status, first_output_ms: sample.first_output_ms, first_reasoning_ms: sample.first_reasoning_ms, reasoning_deltas: sample.reasoning_deltas ?? 0, ttft_ms: sample.ttft_ms, completion_ms: sample.completion_ms, server_setup_ms: sample.server_setup_ms, model_calls: sample.model_calls.length }));
}
report.metrics = (await curl(`/sessions/${session}/metrics`)).body;
report.summary = { successes: report.samples.filter(sample => sample.status === "completed").length, failed_tool_results: report.samples.flatMap(sample => sample.tool_calls).filter(tool => tool.type === "tool.result" && tool.status === "failed").length, first_output_ms: summarize(report.samples.filter(sample => sample.first_output_ms !== undefined).map(sample => sample.first_output_ms)), first_reasoning_ms: summarize(report.samples.filter(sample => sample.first_reasoning_ms !== undefined).map(sample => sample.first_reasoning_ms)), ttft_ms: summarize(report.samples.filter(sample => sample.ttft_ms !== undefined).map(sample => sample.ttft_ms)), warm_ttft_ms: summarize(report.samples.slice(1).filter(sample => sample.ttft_ms !== undefined).map(sample => sample.ttft_ms)), setup_ms: summarize(report.samples.filter(sample => sample.server_setup_ms !== undefined).map(sample => sample.server_setup_ms)) };
save();
console.log(JSON.stringify({ report: output, summary: report.summary }));

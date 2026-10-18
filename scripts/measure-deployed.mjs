import { randomUUID } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { parseArgs } from "node:util";

const { values, positionals } = parseArgs({ allowPositionals: true, options: { "health-only": { type: "boolean", default: false }, samples: { type: "string", default: "6" }, provider: { type: "string", default: "claude" } } });
const origin = new URL(positionals[0] ?? process.env.WORKER_URL ?? "https://nanocode-worker.gakonst.workers.dev");
if (origin.protocol !== "https:" && origin.hostname !== "localhost" && origin.hostname !== "127.0.0.1") throw new Error("Use HTTPS for production measurements");
if (origin.username || origin.password || origin.search || origin.hash || origin.pathname !== "/") throw new Error("Pass only the Worker origin");
const token = process.env.WORKER_TOKEN;
if (!token) throw new Error("WORKER_TOKEN must be provided through the environment");
const provider = values.provider;
if (provider !== "claude" && provider !== "codex") throw new Error("provider must be claude or codex");
const count = Number(values.samples);
if (!Number.isSafeInteger(count) || count < 2 || count > 20) throw new Error("samples must be between 2 and 20");
const headers = { authorization: `Bearer ${token}`, "content-type": "application/json" };
const request = (path, init = {}) => fetch(new URL(path, origin), { ...init, headers: { ...headers, ...init.headers }, signal: init.signal ?? AbortSignal.timeout(120000) });
const summarize = numbers => {
  const sorted = [...numbers].sort((a, b) => a - b);
  return { count: sorted.length, median: sorted.length % 2 ? sorted[Math.floor(sorted.length / 2)] : (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2, min: sorted[0], max: sorted.at(-1) };
};
const report = { provider, measured_at: new Date().toISOString(), origin: origin.origin, prompt: "Reply exactly nanocode-worker-ok.", mode: values["health-only"] ? "health-only" : "live-inference", health: [], samples: [], limitations: ["Client timings include the network path and live provider latency.", "A new session does not establish whether its Worker isolate or private credential cache was cold.", "No remote execution host is exercised by this measurement."] };
const session = `production-measure-${randomUUID()}`;
report.session = session;
const base = `/sessions/${session}`;
let cursor = 0;

async function measureTurn(index) {
  const id = `turn-${index}`;
  const sample = { kind: index ? "warm-session" : "new-session", request_id: id };
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 120000);
  let reader;
  const consumption = (async () => {
    const response = await request(`${base}/events?after=${cursor}`, { headers: { accept: "text/event-stream" }, signal: controller.signal });
    if (response.status !== 200) throw new Error(`Event stream rejected: ${response.status}`);
    reader = response.body.getReader();
    const decoder = new TextDecoder();
    let pending = "";
    for (;;) {
      const next = await reader.read();
      if (next.done) throw new Error("Event stream ended before completion");
      pending += decoder.decode(next.value, { stream: true });
      let split;
      while ((split = pending.indexOf("\n\n")) >= 0) {
        const message = pending.slice(0, split);
        pending = pending.slice(split + 2);
        const data = message.split("\n").filter(line => line.startsWith("data:")).map(line => line.slice(5).trimStart()).join("\n");
        if (!data) continue;
        const event = JSON.parse(data);
        cursor = Math.max(cursor, event.seq);
        if (event.request_id !== id) continue;
        if (event.event?.source === "agent" && event.event.event?.type === "assistant.delta" && event.event.event.payload?.text && sample.ttft_ms === undefined) sample.ttft_ms = performance.now() - start;
        if (event.event?.type === "result") {
          sample.completion_ms = performance.now() - start;
          sample.status = event.event.status;
          sample.response = event.event.ok?.message;
          sample.error = event.event.error;
          return;
        }
      }
    }
  })();
  void consumption.catch(() => {});
  const start = performance.now();
  try {
    const response = await request(`${base}/turns`, { method: "POST", body: JSON.stringify({ request_id: id, prompt: report.prompt, provider }), signal: controller.signal });
    sample.admission_ms = performance.now() - start;
    sample.colo = response.headers.get("cf-ray")?.split("-").at(-1);
    if (response.status !== 202) throw new Error(`Turn admission rejected: ${response.status}`);
    await consumption;
    if (sample.status !== "completed" || sample.ttft_ms === undefined) throw new Error(`Live turn did not produce assistant text: ${sample.status}`);
    return sample;
  } finally {
    clearTimeout(timeout);
    controller.abort();
    await reader?.cancel().catch(() => {});
    await consumption.catch(() => {});
    report.samples.push(sample);
  }
}

try {
  for (let index = 0; index < 6; index++) {
    const start = performance.now();
    const response = await request("/health");
    const elapsed_ms = performance.now() - start;
    if (response.status !== 200) throw new Error(`Health check rejected: ${response.status}`);
    const health = await response.json();
    report.model = health.models?.[provider] ?? health.model;
    report.auth = health.auth;
    report.health.push({ kind: index ? "warm-client-connection" : "initial-client-connection", elapsed_ms, colo: response.headers.get("cf-ray")?.split("-").at(-1) });
  }
  const unauthenticated = await fetch(new URL("/health", origin), { signal: AbortSignal.timeout(15000) });
  report.unauthenticated_status = unauthenticated.status;
  if (unauthenticated.status !== 401) throw new Error("Unauthenticated health access was accepted");
  report.warm_health_ms = summarize(report.health.slice(1).map(item => item.elapsed_ms));
  if (!values["health-only"]) {
    for (let index = 0; index < count; index++) {
      const sample = await measureTurn(index);
      console.log(JSON.stringify({ kind: sample.kind, ttft_ms: sample.ttft_ms, completion_ms: sample.completion_ms }));
    }
    const before = await (await request(`${base}/metrics`)).json();
    const start = performance.now();
    const replay = await request(`${base}/turns`, { method: "POST", body: JSON.stringify({ request_id: "turn-0", prompt: report.prompt, provider }) });
    report.replay_ms = performance.now() - start;
    const replayed = await replay.json();
    if (replay.status !== 200 || replayed.status !== "completed") throw new Error("Completed request replay failed");
    const conflict = await request(`${base}/turns`, { method: "POST", body: JSON.stringify({ request_id: "turn-0", prompt: "Different measurement input" }) });
    report.conflict_status = conflict.status;
    if (conflict.status !== 409) throw new Error("Conflicting request identity was accepted");
    const after = await (await request(`${base}/metrics`)).json();
    if (before.model_dispatches !== after.model_dispatches || after.persisted.unfinished !== 0) throw new Error("Replay dispatched work or queue did not drain");
    report.response = replayed.ok?.message;
    report.session_id = replayed.ok?.session_id;
    report.metrics = after;
    report.warm_ttft_ms = summarize(report.samples.slice(1).map(item => item.ttft_ms));
  }
  report.result = "passed";
} catch (error) {
  report.result = "incomplete";
  report.error = error.message;
  process.exitCode = 1;
} finally {
  mkdirSync("output", { recursive: true });
  const path = values["health-only"] ? "output/production-health-measurement.json" : `output/production-${provider}-measurement.json`;
  writeFileSync(path, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ result: report.result, report: path, warm_health_ms: report.warm_health_ms, new_session: report.samples[0], warm_ttft_ms: report.warm_ttft_ms, replay_ms: report.replay_ms, error: report.error }, null, 2));
}

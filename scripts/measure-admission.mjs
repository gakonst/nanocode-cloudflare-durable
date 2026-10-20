import { mkdirSync, writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { parseArgs } from "node:util";

const { values, positionals } = parseArgs({ allowPositionals: true, options: { provider: { type: "string", default: "claude" }, sessions: { type: "string", default: "3" }, turns: { type: "string", default: "3" }, prefix: { type: "string", default: "admission" }, idle: { type: "string", default: "0" }, label: { type: "string", default: "run" } } });
const origin = new URL(positionals[0] ?? "https://nanocode.gakonst.workers.dev");
if (origin.protocol !== "https:" && !["127.0.0.1", "localhost"].includes(origin.hostname)) throw new Error("Use an HTTPS origin or a loopback address");
const token = process.env.WORKER_TOKEN;
if (!token || /[\r\n]/.test(token)) throw new Error("Provide WORKER_TOKEN through the environment");
if (!["claude", "codex"].includes(values.provider)) throw new Error("provider must be claude or codex");
const sessions = Number(values.sessions);
const turns = Number(values.turns);
const auth = { authorization: "Bearer " + token, "content-type": "application/json" };
const prompt = "Reply with only the word ok.";
const stamp = Date.now().toString(36);
const SPANS = /^(sdk\.open|wasm\.instantiate|host\.(store|log|http_start|socket_open|events|turn_finished))$/;
const round = value => typeof value === "number" ? Math.round(value * 10) / 10 : undefined;
const turnsPath = session => new URL("/sessions/" + encodeURIComponent(session) + "/turns", origin);

function serverTiming(response) {
  return Object.fromEntries([...(response.headers.get("server-timing") ?? "").matchAll(/([a-z_]+);dur=([0-9.]+)/g)].map(([, name, value]) => [name, round(Number(value))]));
}

async function health() {
  const started = performance.now();
  const response = await fetch(new URL("/health", origin), { headers: auth });
  await response.arrayBuffer();
  return round(performance.now() - started);
}

function timing(value = {}) {
  const spans = Object.fromEntries(Object.entries(value.runtime_spans ?? {}).filter(([name]) => SPANS.test(name)).map(([name, span]) => [name, { count: span.count, total_ms: round(span.total_ms) }]));
  return { admission_ms: round(value.admission_ms), pump_wait_ms: round(value.pump_wait_ms), runtime_open_ms: round(value.runtime_open_ms), runtime_reused: value.runtime_reused, run_ms: round(value.run_ms), observed_ms: round(value.observed_ms), runtime_spans: spans };
}

async function streamed(session, id) {
  const started = performance.now();
  const response = await fetch(turnsPath(session), { method: "POST", headers: { ...auth, accept: "text/event-stream" }, body: JSON.stringify({ request_id: id, prompt, provider: values.provider }) });
  const sample = { mode: "stream", request_id: id, status: response.status, headers_ms: round(performance.now() - started), server_timing: serverTiming(response) };
  if (response.status !== 200 || !response.headers.get("content-type")?.includes("text/event-stream")) throw new Error("turn " + id + " was rejected with HTTP " + response.status + ": " + (await response.text()).slice(0, 300));
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  const mark = (name, at) => { sample[name] ??= round(at); };
  for (;;) {
    const next = await reader.read();
    if (next.done) break;
    const at = performance.now() - started;
    buffer += decoder.decode(next.value, { stream: true });
    let split;
    while ((split = buffer.indexOf("\n\n")) >= 0) {
      const frame = buffer.slice(0, split);
      buffer = buffer.slice(split + 2);
      const data = frame.split("\n").filter(line => line.startsWith("data:")).map(line => line.slice(5).trimStart()).join("\n");
      if (!data) continue;
      const outer = JSON.parse(data).event;
      const type = outer?.source === "agent" ? outer.event?.type : outer?.type;
      mark("first_event_ms", at);
      if (type === "running") mark("running_ms", at);
      if (type === "model.call.started") mark("model_call_started_ms", at);
      if (type === "assistant.delta") mark("first_text_ms", at);
      if (type === "result") {
        mark("result_ms", at);
        sample.result = outer.status;
        sample.timing = timing(outer.timing);
      }
    }
  }
  if (sample.result !== "completed") throw new Error("turn " + id + " ended " + sample.result);
  return sample;
}

async function queued(session, id) {
  const body = JSON.stringify({ request_id: id, prompt, provider: values.provider });
  const started = performance.now();
  const response = await fetch(turnsPath(session), { method: "POST", headers: auth, body });
  const report = await response.json();
  const sample = { mode: "json", request_id: id, status: response.status, response_ms: round(performance.now() - started), server_timing: serverTiming(response), turn_status: report.status };
  for (;;) {
    const turn = await (await fetch(new URL(turnsPath(session).pathname + "/" + id, origin), { headers: auth })).json();
    if (["completed", "failed", "cancelled"].includes(turn.status)) { sample.result = turn.status; sample.completed_ms = round(performance.now() - started); break; }
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  const replayStarted = performance.now();
  const replay = await fetch(turnsPath(session), { method: "POST", headers: auth, body });
  await replay.arrayBuffer();
  sample.replay = { status: replay.status, response_ms: round(performance.now() - replayStarted), server_timing: serverTiming(replay) };
  return sample;
}

const median = list => {
  const sorted = list.filter(value => typeof value === "number").sort((a, b) => a - b);
  if (!sorted.length) return undefined;
  const middle = sorted.length >> 1;
  return round(sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2);
};
const report = { origin: origin.origin, provider: values.provider, label: values.label, idle_s: Number(values.idle), at: new Date().toISOString(), health_ms: [], sessions: [] };
for (let index = 0; index < 3; index++) report.health_ms.push(await health());
for (let index = 0; index < sessions; index++) {
  const session = values.prefix + "-" + values.provider + "-" + stamp + "-" + index;
  const samples = [];
  for (let turn = 0; turn < turns; turn++) samples.push({ ...(await streamed(session, "turn-" + turn)), kind: turn === 0 ? "new" : "warm" });
  samples.push({ ...(await queued(session, "queued")), kind: "warm" });
  if (Number(values.idle) > 0) {
    await new Promise(resolve => setTimeout(resolve, Number(values.idle) * 1000));
    samples.push({ ...(await streamed(session, "restored")), kind: "restored" });
  }
  report.sessions.push({ session, samples });
  console.error(JSON.stringify(samples.map(({ server_timing, timing, replay, ...rest }) => rest)));
}
const all = report.sessions.flatMap(entry => entry.samples);
const summary = { health_ms: median(report.health_ms) };
for (const kind of ["new", "warm", "restored"]) {
  const list = all.filter(sample => sample.kind === kind && sample.mode === "stream");
  summary[kind] = Object.fromEntries(["headers_ms", "first_event_ms", "running_ms", "model_call_started_ms", "first_text_ms", "result_ms"].map(key => [key, median(list.map(sample => sample[key]))]));
  summary[kind].session_response_ms = median(list.map(sample => sample.server_timing.session_response));
  for (const key of ["admission_ms", "pump_wait_ms", "runtime_open_ms", "run_ms"]) summary[kind][key] = median(list.map(sample => sample.timing?.[key]));
}
const json = all.filter(sample => sample.mode === "json");
summary.json = { response_ms: median(json.map(sample => sample.response_ms)), session_response_ms: median(json.map(sample => sample.server_timing.session_response)), completed_ms: median(json.map(sample => sample.completed_ms)), replay_ms: median(json.map(sample => sample.replay.response_ms)), replay_session_response_ms: median(json.map(sample => sample.replay.server_timing.session_response)) };
report.summary = summary;
mkdirSync("output", { recursive: true });
const file = "output/admission-" + values.label + "-" + values.provider + "-" + Date.now() + ".json";
writeFileSync(file, JSON.stringify(report, null, 2));
console.log(JSON.stringify({ file, summary }, null, 2));

import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { parseArgs } from "node:util";
import { LOCAL_TOKEN, localOptions } from "./local-runtime.mjs";

const { values } = parseArgs({ options: { providers: { type: "string", default: "claude,codex" }, rounds: { type: "string", default: "5" }, history: { type: "string", default: "10" }, "reply-bytes": { type: "string", default: "20000" }, label: { type: "string", default: "run" } } });
const replyBytes = Number(values["reply-bytes"]);
const round = value => typeof value === "number" ? Math.round(value * 10) / 10 : undefined;
let arrived;
let calls = 0;

function workerdCpu() {
  const pids = execFileSync("pgrep", ["-P", String(process.pid), "workerd"], { encoding: "utf8" }).split("\n").filter(Boolean);
  return pids.reduce((total, pid) => total + execFileSync("ps", ["-o", "time=", "-p", pid], { encoding: "utf8" }).trim().split(":").map(Number).reduce((sum, part) => sum * 60 + part, 0) * 1000, 0);
}

async function measured(step) {
  const cpu = workerdCpu();
  const sample = await step();
  return { ...(typeof sample === "number" ? { ms: sample } : sample), workerd_cpu_ms: round(workerdCpu() - cpu) };
}

function sse(events) {
  return new Response(events.map(([event, data]) => (event ? "event: " + event + "\n" : "") + "data: " + JSON.stringify(data) + "\n\n").join(""), { headers: { "content-type": "text/event-stream" } });
}

const provider = async request => {
  arrived?.();
  const url = new URL(request.url);
  const input = await request.json();
  const call = calls++;
  const text = "x".repeat(replyBytes);
  if (url.pathname === "/openai/v1/responses") {
    const id = "resp_" + call;
    const item = { type: "message", id: "msg_" + call, role: "assistant", status: "completed", content: [{ type: "output_text", text }] };
    return sse([[null, { type: "response.created", response: { id, status: "in_progress" } }], [null, { type: "response.output_item.added", output_index: 0, item: { ...item, status: "in_progress", content: [] } }], [null, { type: "response.output_text.delta", output_index: 0, item_id: item.id, content_index: 0, delta: text }], [null, { type: "response.output_item.done", output_index: 0, item }], [null, { type: "response.completed", response: { id, status: "completed", model: input.model, output: [item], usage: { input_tokens: 8, output_tokens: 4, total_tokens: 12 } } }]]);
  }
  return sse([
    ["message_start", { type: "message_start", message: { id: "message-" + call, type: "message", role: "assistant", model: input.model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 8, output_tokens: 0 } } }],
    ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }],
    ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text } }],
    ["content_block_stop", { type: "content_block_stop", index: 0 }],
    ["message_delta", { type: "message_delta", delta: { stop_reason: "end_turn", stop_sequence: null }, usage: { output_tokens: 4 } }],
    ["message_stop", { type: "message_stop" }],
  ]);
};

const persistence = resolve(mkdtempSync("output/local-state-"));
const options = localOptions({ provider, persistence });
const headers = { authorization: "Bearer " + LOCAL_TOKEN, "content-type": "application/json" };
let mf;
const start = async () => { mf = new Miniflare(convertV4MiniflareOptions(options)); await mf.ready; };

async function turn(name, id, providerName) {
  const started = performance.now();
  const sample = {};
  arrived = () => { sample.dispatch_ms ??= round(performance.now() - started); };
  const response = await mf.dispatchFetch("http://worker/sessions/" + name + "/turns", { method: "POST", headers: { ...headers, accept: "text/event-stream" }, body: JSON.stringify({ request_id: id, prompt: "Say hello for " + id, provider: providerName }) });
  sample.headers_ms = round(performance.now() - started);
  const text = await response.text();
  sample.result_ms = round(performance.now() - started);
  const result = text.split("\n").filter(line => line.startsWith("data:")).map(line => JSON.parse(line.slice(5)).event).find(event => event?.type === "result");
  if (result?.status !== "completed") throw new Error("turn " + id + " ended " + JSON.stringify(result)?.slice(0, 300));
  const timing = result.timing ?? {};
  const spans = timing.runtime_spans ?? {};
  sample.runtime_open_ms = round(timing.runtime_open_ms);
  sample.sdk_open_ms = round(spans["sdk.open"]?.total_ms);
  sample.instantiate_ms = round(spans["wasm.instantiate"]?.total_ms);
  sample.admission_ms = round(timing.admission_ms);
  return sample;
}

async function timed(url) {
  const started = performance.now();
  const response = await mf.dispatchFetch(url, { headers });
  await response.arrayBuffer();
  return round(performance.now() - started);
}

const median = list => { const sorted = list.filter(value => typeof value === "number").sort((a, b) => a - b); const middle = sorted.length >> 1; return sorted.length ? round(sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2) : undefined; };
const report = { label: values.label, bundle: process.env.NANOCODE_BUNDLE_PATH ?? "output/deploy", history: Number(values.history), reply_bytes: replyBytes, measured_at: new Date().toISOString(), providers: [] };
try {
  await start();
  for (const providerName of values.providers.split(",")) {
    const name = "cold-" + providerName + "-" + Date.now().toString(36);
    for (let index = 0; index < Number(values.history); index++) await turn(name, "history-" + index, providerName);
    const rounds = [];
    for (let index = 0; index < Number(values.rounds); index++) {
      await mf.dispose();
      await start();
      const sample = { restart: await measured(() => timed("http://worker/sessions/" + name + "/hands")) };
      sample.reader = await measured(() => timed("http://worker/sessions/" + name + "/turns/none"));
      sample.cold = await measured(() => turn(name, "cold-" + index, providerName));
      sample.warm = await measured(() => turn(name, "warm-" + index, providerName));
      rounds.push(sample);
      console.error(providerName, JSON.stringify(sample));
    }
    const summary = {
      restart_ms: median(rounds.map(sample => sample.restart.ms)),
      restart_cpu_ms: median(rounds.map(sample => sample.restart.workerd_cpu_ms)),
      reader_ms: median(rounds.map(sample => sample.reader.ms)),
      reader_cpu_ms: median(rounds.map(sample => sample.reader.workerd_cpu_ms)),
      ...Object.fromEntries(["headers_ms", "dispatch_ms", "result_ms", "runtime_open_ms", "sdk_open_ms", "workerd_cpu_ms"].flatMap(key => [["cold_" + key, median(rounds.map(sample => sample.cold[key]))], ["warm_" + key, median(rounds.map(sample => sample.warm[key]))]])),
    };
    report.providers.push({ provider: providerName, session: name, summary, rounds });
  }
} finally { await mf?.dispose(); }
mkdirSync("output", { recursive: true });
const file = "output/cold-local-" + values.label + "-" + Date.now() + ".json";
writeFileSync(file, JSON.stringify(report, null, 2));
console.log(JSON.stringify({ file, providers: report.providers.map(({ provider, summary }) => ({ provider, summary })) }, null, 2));

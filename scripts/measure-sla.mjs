import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { parseArgs } from "node:util";
import { LOCAL_TOKEN, localOptions } from "./local-runtime.mjs";

const { values } = parseArgs({ options: { providers: { type: "string", default: "claude,codex" }, rounds: { type: "string", default: "5" }, turns: { type: "string", default: "3" }, tokens: { type: "string", default: "200" }, rate: { type: "string", default: "0" }, coalesce: { type: "string" }, label: { type: "string", default: "run" } } });
const tokens = Number(values.tokens);
const rate = Number(values.rate);
const round = value => typeof value === "number" ? Math.round(value * 100) / 100 : undefined;
const sleep = ms => new Promise(done => setTimeout(done, ms));
let current;
let calls = 0;

function stream(frames, paced) {
  const encoder = new TextEncoder();
  return new ReadableStream({
    async start(controller) {
      const started = performance.now();
      for (let index = 0; index < frames.length; index++) {
        const [event, data, token] = frames[index];
        if (paced && token !== undefined && rate > 0) {
          const due = started + (token / rate) * 1000;
          const wait = due - performance.now();
          if (wait > 0) await sleep(wait);
        }
        if (token === 0) current.provider_first ??= performance.now();
        if (token === tokens - 1) current.provider_last = performance.now();
        controller.enqueue(encoder.encode((event ? "event: " + event + "\n" : "") + "data: " + JSON.stringify(data) + "\n\n"));
      }
      controller.close();
    },
  });
}

const words = Array.from({ length: tokens }, (_, index) => (index % 2 ? "ba" : "ab"));

const provider = async request => {
  current.arrived ??= performance.now();
  const url = new URL(request.url);
  const input = await request.json();
  current.request_bytes ??= JSON.stringify(input).length;
  const call = calls++;
  if (url.pathname === "/openai/v1/responses") {
    const id = "resp_" + call;
    const text = words.join("");
    const item = { type: "message", id: "msg_" + call, role: "assistant", status: "completed", content: [{ type: "output_text", text }] };
    return new Response(stream([
      [null, { type: "response.created", response: { id, status: "in_progress" } }],
      [null, { type: "response.output_item.added", output_index: 0, item: { ...item, status: "in_progress", content: [] } }],
      ...words.map((word, token) => [null, { type: "response.output_text.delta", output_index: 0, item_id: item.id, content_index: 0, delta: word }, token]),
      [null, { type: "response.output_item.done", output_index: 0, item }],
      [null, { type: "response.completed", response: { id, status: "completed", model: input.model, output: [item], usage: { input_tokens: 8, output_tokens: tokens, total_tokens: tokens + 8 } } }],
    ], true), { headers: { "content-type": "text/event-stream" } });
  }
  return new Response(stream([
    ["message_start", { type: "message_start", message: { id: "message-" + call, type: "message", role: "assistant", model: input.model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 8, output_tokens: 0 } } }],
    ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }],
    ...words.map((word, token) => ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: word } }, token]),
    ["content_block_stop", { type: "content_block_stop", index: 0 }],
    ["message_delta", { type: "message_delta", delta: { stop_reason: "end_turn", stop_sequence: null }, usage: { output_tokens: tokens } }],
    ["message_stop", { type: "message_stop" }],
  ], true), { headers: { "content-type": "text/event-stream" } });
};

function deltas(value) {
  if (!value || typeof value !== "object") return 0;
  if (value.type === "assistant.delta") return String(value.payload?.delta ?? value.payload?.text ?? "").length;
  return Object.values(value).reduce((total, inner) => total + deltas(inner), 0);
}

const finished = value => value && typeof value === "object" && (value.type === "result" || Object.values(value).some(finished));

const options = localOptions({ provider, persistence: resolve(mkdtempSync("output/local-state-")), agent: values.coalesce === undefined ? {} : { EVENT_COALESCE_MS: values.coalesce } });
const headers = { authorization: "Bearer " + LOCAL_TOKEN, "content-type": "application/json" };
let mf;

async function turn(session, id, providerName) {
  current = {};
  const started = performance.now();
  const response = await mf.dispatchFetch("http://worker/sessions/" + session + "/turns?live=1", { method: "POST", headers: { ...headers, accept: "text/event-stream" }, body: JSON.stringify({ request_id: id, prompt: "Reply " + id, provider: providerName }) });
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let chars = 0;
  let first;
  let last;
  let done = false;
  let timing;
  while (!done) {
    const { done: end, value } = await reader.read();
    if (end) break;
    buffer += decoder.decode(value, { stream: true });
    const cut = buffer.lastIndexOf("\n\n");
    if (cut < 0) continue;
    const at = performance.now();
    for (const block of buffer.slice(0, cut).split("\n\n")) {
      const data = block.split("\n").filter(line => line.startsWith("data:")).map(line => line.slice(5)).join("");
      if (!data) continue;
      const parsed = JSON.parse(data);
      const count = deltas(parsed);
      if (count) { first ??= at; last = at; chars += count; }
      if (finished(parsed)) { done = true; timing = parsed.event?.timing; }
    }
    buffer = buffer.slice(cut + 2);
  }
  reader.cancel().catch(() => {});
  const expected = tokens * 2;
  const sample = {
    dispatch: round(current.arrived - started),
    first: round(first - started),
    render: round(first - current.provider_first),
    overhead: round(current.arrived - started + (first - current.provider_first)),
    tail: round(last - current.provider_last),
    client_tps: round(tokens / ((last - first) / 1000)),
    provider_tps: round(tokens / ((current.provider_last - current.provider_first) / 1000)),
    chars: chars === expected ? undefined : chars,
    request_bytes: current.request_bytes,
    admission: round(timing?.admission_ms),
    runtime_open: round(timing?.runtime_open_ms),
    spans: Object.fromEntries(Object.entries(timing?.runtime_spans ?? {}).map(([name, span]) => [name, round(span.total_ms)])),
  };
  return sample;
}

const pct = (list, p) => { const sorted = list.filter(value => typeof value === "number" && Number.isFinite(value)).sort((a, b) => a - b); return sorted.length ? round(sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))]) : undefined; };
const report = { label: values.label, tokens, rate, coalesce: values.coalesce, measured_at: new Date().toISOString(), providers: [] };
try {
  for (const providerName of values.providers.split(",")) {
    const samples = [];
    for (let index = 0; index < Number(values.rounds); index++) {
      mf = new Miniflare(convertV4MiniflareOptions(options));
      await mf.ready;
      const session = "sla-" + providerName + "-" + index + "-" + Date.now().toString(36);
      for (let number = 1; number <= Number(values.turns); number++) {
        const sample = { turn: number, ...(await turn(session, "turn-" + index + "-" + number, providerName)) };
        samples.push(sample);
        console.error(providerName, index, JSON.stringify(sample));
      }
      await mf.dispose();
    }
    const keys = ["dispatch", "first", "render", "overhead", "tail", "client_tps", "provider_tps"];
    const summary = [...new Set(samples.map(sample => sample.turn))].map(number => ({ turn: number, ...Object.fromEntries(keys.map(key => [key, [pct(samples.filter(sample => sample.turn === number).map(sample => sample[key]), 0.5), pct(samples.filter(sample => sample.turn === number).map(sample => sample[key]), 0.99)]])) }));
    report.providers.push({ provider: providerName, summary, samples });
  }
} finally { await mf?.dispose(); }
mkdirSync("output", { recursive: true });
const file = "output/sla-" + values.label + "-" + Date.now() + ".json";
writeFileSync(file, JSON.stringify(report, null, 2));
console.log(JSON.stringify({ file, providers: report.providers.map(({ provider, summary }) => ({ provider, summary })) }));

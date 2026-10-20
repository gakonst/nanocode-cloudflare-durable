import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { mkdirSync, writeFileSync } from "node:fs";
import { parseArgs } from "node:util";
import { LOCAL_TOKEN, localOptions } from "./local-runtime.mjs";

const { values } = parseArgs({ options: { turns: { type: "string", default: "3" }, linger: { type: "string", default: "30000" }, recovery: { type: "string", default: "30000" }, label: { type: "string", default: "run" } } });
const TURNS = Number(values.turns);
let calls = 0;

const sse = events => new Response(events.map(([event, data]) => (event ? "event: " + event + "\n" : "") + "data: " + JSON.stringify(data) + "\n\n").join(""), { headers: { "content-type": "text/event-stream" } });

function claude(input) {
  const turn = calls++;
  return sse([
    ["message_start", { type: "message_start", message: { id: "message-" + turn, type: "message", role: "assistant", model: input.model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 8, output_tokens: 0 } } }],
    ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }],
    ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "reply " + turn } }],
    ["content_block_stop", { type: "content_block_stop", index: 0 }],
    ["message_delta", { type: "message_delta", delta: { stop_reason: "end_turn", stop_sequence: null }, usage: { output_tokens: 2 } }],
    ["message_stop", { type: "message_stop" }],
  ]);
}

function codex() {
  const call = calls++;
  const item = { type: "message", id: "msg_" + call, role: "assistant", status: "completed", content: [{ type: "output_text", text: "codex reply " + call }] };
  return sse([
    [null, { type: "response.created", response: { id: "resp_" + call, status: "in_progress" } }],
    [null, { type: "response.output_item.added", output_index: 0, item: { ...item, status: "in_progress", content: [] } }],
    [null, { type: "response.output_text.delta", output_index: 0, item_id: item.id, content_index: 0, delta: item.content[0].text }],
    [null, { type: "response.output_item.done", output_index: 0, item }],
    [null, { type: "response.completed", response: { id: "resp_" + call, status: "completed", model: "gpt-6.1-sol", output: [item], usage: { input_tokens: 8, output_tokens: 4, total_tokens: 12 } } }],
  ]);
}

const provider = async request => new URL(request.url).pathname === "/openai/v1/responses" ? (await request.json(), codex()) : claude(await request.json());
const headers = { authorization: "Bearer " + LOCAL_TOKEN, "content-type": "application/json" };
const mf = new Miniflare(convertV4MiniflareOptions(localOptions({ provider, agent: { MODEL_SOCKET_LINGER_MS: values.linger, RECOVERY_DELAY_MS: values.recovery } })));
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function idle(session) {
  for (let attempt = 0; ; attempt++) {
    const metrics = await (await mf.dispatchFetch("http://worker/sessions/" + session + "/metrics", { headers })).json();
    if (metrics.idle.wait_until === 0 || attempt === 200) return { timers_ms: metrics.idle.timers_ms, wait_until: metrics.idle.wait_until, alarm_at: metrics.alarm_at, resident_runtimes: metrics.resident_runtimes };
    await sleep(5);
  }
}

try {
  await mf.ready;
  const report = { label: values.label, linger_ms: Number(values.linger), scenarios: [] };
  for (const name of ["claude", "codex"]) {
    const session = "idle-" + name + "-" + Date.now();
    const turns = [];
    for (let index = 0; index < TURNS; index++) {
      const response = await mf.dispatchFetch("http://worker/sessions/" + session + "/turns", { method: "POST", headers: { ...headers, accept: "text/event-stream" }, body: JSON.stringify({ request_id: "turn-" + index, prompt: "prompt " + index, provider: name }) });
      const text = await response.text();
      if (!text.includes('"status":"completed"')) throw new Error("turn did not complete: " + text.slice(-400));
      const state = await idle(session);
      turns.push({ ...state, non_hibernateable_tail_ms: Math.max(0, ...state.timers_ms) });
    }
    const tail = turns.map(turn => turn.non_hibernateable_tail_ms);
    report.scenarios.push({ name, turns, max_tail_ms: Math.max(...tail), billed_idle_gb_s_per_turn: Math.max(...tail) / 1000 * 128 / 1024 });
  }
  mkdirSync("output", { recursive: true });
  const path = "output/idle-" + values.label + "-" + Date.now() + ".json";
  writeFileSync(path, JSON.stringify(report, null, 2));
  for (const { name, max_tail_ms, billed_idle_gb_s_per_turn, turns } of report.scenarios) console.log(JSON.stringify({ name, max_tail_ms, billed_idle_gb_s_per_turn, alarm_at: turns.at(-1).alarm_at, timers_ms: turns.at(-1).timers_ms }));
  console.log("report", path);
} finally { await mf.dispose(); }

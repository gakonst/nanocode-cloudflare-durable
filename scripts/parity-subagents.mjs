import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { writeFileSync } from "node:fs";
import { parseArgs } from "node:util";
import { localOptions } from "./local-runtime.mjs";

const { values } = parseArgs({ options: { label: { type: "string", default: "parity" } } });
const text = content => typeof content === "string" ? content : (content ?? []).filter(block => block.type === "text").map(block => block.text).join("\n");
const results = content => typeof content === "string" ? [] : (content ?? []).filter(block => block.type === "tool_result");
let calls = 0;
const wrapped = new Map();
const holds = new Map();
const log = [];
const hold = id => { if (!holds.has(id)) { let release; const promise = new Promise(resolve => { release = resolve; }); holds.set(id, { promise, release }); } return holds.get(id); };

function sse(input, blocks, stop) {
  const events = [["message_start", { type: "message_start", message: { id: "message-" + calls, type: "message", role: "assistant", model: input.model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 8, output_tokens: 0 } } }]];
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
function invoke(input, name, value) {
  const id = "toolu_" + calls;
  if ((input.tools ?? []).some(entry => entry.name === name)) return sse(input, [{ type: "tool_use", id, name, input: value }], "tool_use");
  wrapped.set(id, name);
  return sse(input, [{ type: "tool_use", id, name: "exec", input: { code: "text(JSON.stringify(await tools." + name + "(" + JSON.stringify(value) + ")));" } }], "tool_use");
}

const ids = {};
const spawn = task => ({ role: "worker", task, harness: null, model: null, thinking: null, reasoning_mode: null, output_contract: { kind: "string" } });
const steps = [
  () => ["spawn_agent", spawn("MODE:quick A")],
  () => ["spawn_agent", spawn("MODE:hold B")],
  () => ["spawn_agent", spawn("MODE:hold C")],
  () => ["wait_agent", { agent_ids: [ids.A], timeout_ms: 30000 }],
  () => ["list_agents", {}],
  () => ["send_agent_message", { agent_id: ids.B, message: "PING-URGENT", priority: "urgent" }],
  () => { hold("B").release(); return ["wait_agent", { agent_ids: [ids.B], timeout_ms: 30000 }]; },
  () => ["send_agent_message", { agent_id: ids.A, message: "PING-DEFERRED", priority: "deferred" }],
  () => ["wait_agent", { agent_ids: [ids.A], timeout_ms: 30000 }],
  () => ["interrupt_agent", { agent_id: ids.C }],
  () => ["list_agents", { include_completed: true }],
  () => ["close_agent", { agent_id: ids.C }],
  () => ["list_agents", { include_completed: true }],
];
let step = 0;
const childOf = first => /MODE:(\w+) (\w)/.exec(first);

const provider = async request => {
  const input = await request.json();
  calls++;
  const messages = input.messages ?? [];
  const first = text(messages[0]?.content);
  const boundary = messages.findLastIndex(message => message.role === "assistant");
  const trailing = messages.slice(boundary + 1);
  const answered = trailing.flatMap(message => results(message.content));
  const answeredText = answered.map(block => typeof block.content === "string" ? block.content : text(block.content)).join("\n");
  const trailingText = trailing.map(message => text(message.content)).filter(Boolean).join("\n");
  const running = /Script running with cell ID (\S+?)[.\s"\\]/.exec(answeredText + " ");
  if (running) return invoke(input, "wait", { cell_id: running[1], yield_time_ms: 30000 });
  const child = childOf(first);
  if (child) {
    const [, mode, name] = child;
    const agent = /Your agent ID is ([^\s.]+)\./.exec(first)?.[1];
    log.push({ who: name, call: { trailing: trailingText.replace(/[0-9a-f-]{36}/g, "<uuid>"), answered: answeredText.replace(/[0-9a-f-]{36}/g, "<uuid>"), tools: (input.tools ?? []).map(entry => entry.name).sort() } });
    if (trailingText.includes("PING-DEFERRED")) return reply(input, "pong-deferred " + name);
    if (answered.length) return reply(input, trailingText.includes("PING-URGENT") ? "saw-urgent " + name : "done " + name);
    if (mode === "hold") await hold(name).promise;
    return invoke(input, "submit_result", { output: "ok-" + name + "-" + agent });
  }
  if (trailingText.includes("<subagent_completion")) { log.push({ who: "root", completion: trailingText.replace(/[0-9a-f-]{36}/g, "<uuid>") }); }
  if (answered.length && step > 0) {
    const result = answeredText;
    log.push({ who: "root", step: step - 1, result });
    if (step <= 3) { const id = /"agent_id":\s*(\d+)/.exec(result)?.[1]; ids["ABC"[step - 1]] = Number(id); }
  }
  if (step < steps.length) { const [name, value] = steps[step++](); log.push({ who: "root", step: step - 1, call: name, input: value }); return invoke(input, name, value); }
  if (step === steps.length) { step++; return reply(input, "finished"); }
  return reply(input, "ack");
};

const persistence = "output/parity-state-" + Date.now();
const mf = new Miniflare(convertV4MiniflareOptions(localOptions({ provider, persistence })));
const base = "http://worker/sessions/" + values.label + "-" + Date.now();
const headers = { authorization: "Bearer local-measurement", "content-type": "application/json" };
const request = (url, init = {}) => mf.dispatchFetch(url, { ...init, headers });
let failure;
try {
  await mf.ready;
  const submitted = await request(base + "/turns", { method: "POST", body: JSON.stringify({ request_id: "parity", prompt: "PARITY" }) });
  if (submitted.status !== 202) throw new Error("submit " + submitted.status + " " + await submitted.text());
  const start = Date.now();
  for (;;) {
    await new Promise(resolve => setTimeout(resolve, 500));
    const turn = await (await request(base + "/turns/parity")).json();
    if (["completed", "failed", "cancelled"].includes(turn.status) && step > steps.length) break;
    if (Date.now() - start > 120000) throw new Error("timeout at step " + step + " turn " + turn.status);
  }
  await new Promise(resolve => setTimeout(resolve, 3000));
  log.push({ who: "host", subagents: await (await request(base + "/subagents")).json() });
} catch (error) {
  failure = String(error);
} finally {
  for (const entry of holds.values()) entry.release();
  await mf.dispose().catch(() => {});
}
writeFileSync("output/" + values.label + ".json", JSON.stringify({ failure, log }, null, 1));
console.log(JSON.stringify({ failure, steps: step, entries: log.length }));

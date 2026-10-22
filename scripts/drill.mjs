import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { mkdirSync, writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { GATEWAY_KEY, LOCAL_TOKEN, localOptions } from "./local-runtime.mjs";

const SESSION = "drill";
const PROMPT = "Please charge $42 for order 1001.";
const CRASH_AFTER_MS = 1500;
const requests = [];
const charges = [];

const text = content => typeof content === "string" ? content : Array.isArray(content) ? content.map(part => part.text ?? "").join("") : "";

function sse(events) {
  return new Response(events.map(([event, data]) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`).join(""), { headers: { "content-type": "text/event-stream" } });
}

const provider = async request => {
  if (new URL(request.url).hostname !== "provider.invalid" || request.headers.get("x-api-key") !== GATEWAY_KEY) throw new Error("the model request escaped the gateway credential policy");
  const body = await request.json();
  const last = body.messages.at(-1);
  const result = Array.isArray(last.content) ? last.content.find(block => block.type === "tool_result") : undefined;
  const tool = !result && text(last.content).includes("charge");
  const answer = result ? `The tool answered: ${text(result.content)}` : tool ? "tool_use charge" : "ok";
  requests.push({ messages: body.messages.length, tool_result: result ? text(result.content) : undefined, answer });
  const block = tool
    ? [{ type: "tool_use", id: `toolu_${requests.length}`, name: "exec", input: {} }, { type: "input_json_delta", partial_json: JSON.stringify({ code: 'text(await tools.charge({ amount: 42, memo: "order 1001" }));' }) }]
    : [{ type: "text", text: "" }, { type: "text_delta", text: answer }];
  return sse([
    ["message_start", { type: "message_start", message: { id: `message-${requests.length}`, type: "message", role: "assistant", model: body.model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 8, output_tokens: 0 } } }],
    ["content_block_start", { type: "content_block_start", index: 0, content_block: block[0] }],
    ["content_block_delta", { type: "content_block_delta", index: 0, delta: block[1] }],
    ["content_block_stop", { type: "content_block_stop", index: 0 }],
    ["message_delta", { type: "message_delta", delta: { stop_reason: tool ? "tool_use" : "end_turn", stop_sequence: null }, usage: { output_tokens: 4 } }],
    ["message_stop", { type: "message_stop" }],
  ]);
};

const options = localOptions({ provider, bundle: process.env.NANOCODE_BUNDLE_PATH ?? "output/deploy" });
options.handleStructuredLogs = log => {
  const line = String(log.message ?? "");
  if (line.includes('{"charge"')) charges.push(JSON.parse(line.slice(line.indexOf('{"charge"'))).charge);
};
const mf = new Miniflare(convertV4MiniflareOptions(options));
const headers = { authorization: `Bearer ${LOCAL_TOKEN}` };
const request = (path, init = {}) => mf.dispatchFetch(`https://nanocode.local${path}`, { ...init, headers: { ...headers, ...init.headers } });

async function events(response, until) {
  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
  const seen = [];
  let buffered = "";
  for (;;) {
    const read = await reader.read().catch(error => ({ error }));
    if (read.error || read.done) return { seen, ended: read.error ? String(read.error.message ?? read.error) : "done" };
    buffered += read.value;
    let end;
    while ((end = buffered.indexOf("\n\n")) >= 0) {
      const block = buffered.slice(0, end);
      buffered = buffered.slice(end + 2);
      const data = block.split("\n").filter(line => line.startsWith("data:")).map(line => line.slice(5).trim()).join("");
      if (!data) continue;
      const event = JSON.parse(data);
      seen.push(event);
      if (until(event)) {
        await reader.cancel().catch(() => undefined);
        return { seen, ended: "result" };
      }
    }
  }
}

const result = event => event.request_id === "turn-1" && event.event?.type === "result";
const started = performance.now();
try {
  const armed = await (await request(`/sessions/${SESSION}/crash`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ after_ms: CRASH_AFTER_MS }) })).json();
  const first = await events(await request(`/sessions/${SESSION}/turns`, { method: "POST", headers: { accept: "text/event-stream", "content-type": "application/json" }, body: JSON.stringify({ request_id: "turn-1", prompt: PROMPT }) }), result);
  const killed_ms = performance.now() - started;
  let after = first.seen.at(-1)?.seq ?? 0;
  const resumed = [];
  let finished = first.ended === "result" ? first.seen.find(result) : undefined;
  while (!finished && performance.now() - started < 60_000) {
    const response = await request(`/sessions/${SESSION}/events?after=${after}`, { headers: { accept: "text/event-stream" } }).catch(() => undefined);
    if (!response?.ok) {
      await new Promise(resolve => setTimeout(resolve, 200));
      continue;
    }
    const next = await events(response, result);
    resumed.push(...next.seen);
    after = next.seen.at(-1)?.seq ?? after;
    finished = next.seen.find(result);
  }
  const report = {
    measured_at: new Date().toISOString(),
    armed,
    first_stream_ended: first.ended,
    killed_ms,
    completed_ms: performance.now() - started,
    charges: charges.length,
    model_requests: requests,
    resumed_events: resumed.map(event => event.event?.event?.type ?? event.event?.type),
    result: finished?.event,
  };
  const failures = [
    first.ended === "result" && "the first stream finished the turn, so the crash never fired",
    charges.length !== 1 && `${charges.length} charges ran`,
    finished?.event?.status !== "completed" && "the turn did not complete",
    !requests.some(entry => entry.tool_result?.includes("outcome unknown")) && "the model never heard that the charge's outcome is unknown",
    requests.filter(entry => entry.answer === "tool_use charge").length !== 1 && "the model was asked for the charge more than once",
  ].filter(Boolean);
  mkdirSync("output", { recursive: true });
  writeFileSync("output/drill.json", `${JSON.stringify({ ...report, failures }, null, 2)}\n`);
  console.log(JSON.stringify({ ...report, failures }, null, 2));
  if (failures.length) process.exitCode = 1;
} finally {
  await mf.dispose();
}

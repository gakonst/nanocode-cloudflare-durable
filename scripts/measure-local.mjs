import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { GATEWAY_KEY, LOCAL_TOKEN, localOptions } from "./local-runtime.mjs";

const prompt = "Say hello";
let modelCalls = 0;
let interruptNext = false;
let failNext = false;

function stream(input, text = "durable wasm response") {
  const events = [
    ["message_start", { type: "message_start", message: { id: `message-${modelCalls}`, type: "message", role: "assistant", model: input.model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 8, output_tokens: 0 } } }],
    ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }],
    ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text } }],
    ["content_block_stop", { type: "content_block_stop", index: 0 }],
    ["message_delta", { type: "message_delta", delta: { stop_reason: "end_turn", stop_sequence: null }, usage: { output_tokens: 4 } }],
    ["message_stop", { type: "message_stop" }],
  ];
  return new Response(events.map(([event, data]) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`).join(""), { headers: { "content-type": "text/event-stream" } });
}

const provider = async request => {
  const url = new URL(request.url);
  if (url.hostname !== "provider.invalid" || request.headers.get("x-api-key") !== GATEWAY_KEY) throw new Error("Provider request escaped the gateway credential policy");
  const input = await request.json();
  if (input.model !== "claude-opus-5-5" || JSON.stringify(input).includes(GATEWAY_KEY)) throw new Error("Invalid model or credentials entered model input");
  modelCalls++;
  if (failNext) { failNext = false; return Response.json({ error: { type: "invalid_request_error", message: "provider rejected the request" } }, { status: 400 }); }
  if (interruptNext) { interruptNext = false; return new Response(new ReadableStream({ start() {} }), { headers: { "content-type": "text/event-stream" } }); }
  return stream(input);
};

const options = localOptions({ provider });
let mf = new Miniflare(convertV4MiniflareOptions(options));
const name = `measure-${Date.now()}`;
const objectName = JSON.stringify(["owner", name]);
const base = `http://worker/sessions/${name}/turns`;
const headers = { authorization: `Bearer ${LOCAL_TOKEN}`, "content-type": "application/json" };
const request = async (url, init = {}) => mf.dispatchFetch(url, { ...init, headers });
const measurements = [];
let session;

async function until(condition, detail, timeout = 20000) {
  const start = performance.now();
  while (!await condition()) {
    if (performance.now() - start > timeout) throw new Error(detail);
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}

async function submit(id) {
  const response = await request(base, { method: "POST", body: JSON.stringify({ request_id: id, prompt }) });
  if (response.status !== 202) throw new Error(`Submit rejected: ${await response.text()}`);
}

async function complete(id, start = performance.now(), expected = "completed") {
  let result;
  await until(async () => {
    const response = await request(`${base}/${encodeURIComponent(id)}`);
    if (response.status !== 200) throw new Error(`Status rejected: ${await response.text()}`);
    result = await response.json();
    if (["failed", "completed", "cancelled"].includes(result.status)) return true;
    return false;
  }, `Turn ${id} stalled`);
  if (!(Array.isArray(expected) ? expected : [expected]).includes(result.status)) throw new Error(`Unexpected result: ${JSON.stringify(result)}`);
  if (result.ok) {
    session ??= result.ok.session_id;
    if (session !== result.ok.session_id) throw new Error("Session identity changed after recovery");
  }
  measurements.push({ id, milliseconds: performance.now() - start, result });
  return result;
}

async function turn(id, expected) { const start = performance.now(); await submit(id); return complete(id, start, expected); }
const storage = () => mf.unsafeGetDurableObjectStorage("nanocode", "AgentSession", { name: objectName });
const metrics = async () => (await request(`http://worker/sessions/${name}/metrics`)).json();

try {
  await mf.ready;
  const bindings = await mf.getBindings("nanocode");
  if (Object.values(bindings).some(value => typeof value === "string" && value.includes(GATEWAY_KEY))) throw new Error("A provider credential reached the Worker");
  await turn("cold");
  await turn("warm");
  const warm = await metrics();
  if (warm.runtime_initializations !== 1) throw new Error("Warm turns reconstructed the SDK runtime");
  const before = modelCalls;
  const replayStart = performance.now();
  const replay = await request(base, { method: "POST", body: JSON.stringify({ request_id: "cold", prompt }) });
  if (replay.status !== 200 || modelCalls !== before) throw new Error("Duplicate dispatched model work");
  measurements.push({ id: "duplicate", milliseconds: performance.now() - replayStart });
  const conflict = await request(base, { method: "POST", body: JSON.stringify({ request_id: "cold", prompt: "Different prompt" }) });
  if (conflict.status !== 409) throw new Error("Conflicting request identity was accepted");
  await mf.unsafeEvictDurableObject("nanocode", "AgentSession", { name: objectName });
  await request(`${base}/cold`);
  const readOnly = await metrics();
  if (readOnly.runtime_initializations !== 0) throw new Error("Status polling initialized WASM");
  await turn("after-eviction");
  {
    const durable = await storage();
    await durable.exec("CREATE TRIGGER measurement_outage BEFORE UPDATE ON states BEGIN SELECT RAISE(ABORT, 'measurement checkpoint outage'); END");
    const callsBeforeOutage = modelCalls;
    const start = performance.now();
    await submit("checkpoint-recovery");
    await until(async () => {
      const state = await (await request(`${base}/checkpoint-recovery`)).json();
      if (state.status === "failed") throw new Error("Checkpoint outage became a terminal failure");
      return state.status === "running" && state.error;
    }, "Checkpoint failure did not persist recovery");
    if (modelCalls !== callsBeforeOutage) throw new Error("Model ran before checkpoint commit");
    await durable.exec("DROP TRIGGER measurement_outage");
    await complete("checkpoint-recovery", start);
    failNext = true;
    await turn("provider-rejection", "failed");
    const failedCalls = modelCalls;
    const failedReplay = await request(base, { method: "POST", body: JSON.stringify({ request_id: "provider-rejection", prompt }) });
    if (failedReplay.status !== 200 || modelCalls !== failedCalls) throw new Error("Saved provider rejection called the model");
    interruptNext = true;
    const calls = modelCalls;
    await submit("interrupted");
    await until(() => modelCalls > calls, "Interrupted model request did not start");
    await Promise.all([submit("interrupted"), submit("queued-a"), submit("queued-b")]);
    if (modelCalls !== calls + 1) throw new Error("Root turns executed concurrently");
    const restart = performance.now();
    await mf.dispose();
    mf = new Miniflare(convertV4MiniflareOptions(options));
    await mf.ready;
    await until(() => modelCalls > calls + 1, "Persisted alarm did not resume without a client");
    await complete("interrupted", restart);
    await complete("queued-a", restart);
    await complete("queued-b", restart);
    if (modelCalls !== calls + 4) throw new Error("Recovery duplicated queued requests");
    const restored = await storage();
    await restored.exec("UPDATE jobs SET status = 'running', result = NULL WHERE request_id = 'queued-b'");
    const receiptCalls = modelCalls;
    await turn("queued-b");
    if (modelCalls !== receiptCalls) throw new Error("Saved SDK receipt called the model");
    interruptNext = true;
    await submit("cancel-active");
    await until(() => modelCalls > receiptCalls, "Cancellation target did not start");
    await submit("cancel-queued");
    const cancelledQueued = await request(`${base}/cancel-queued/cancel`, { method: "POST" });
    if (cancelledQueued.status !== 200) throw new Error("Queued cancellation failed");
    await request(`${base}/cancel-active/cancel`, { method: "POST" });
    await complete("cancel-active", performance.now(), "cancelled");
    await complete("cancel-queued", performance.now(), "cancelled");
    await turn("after-cancellation");
    const savedState = await restored.exec("SELECT payload FROM states UNION ALL SELECT value AS payload FROM records UNION ALL SELECT body AS payload FROM events UNION ALL SELECT result AS payload FROM jobs");
    if (JSON.stringify(savedState).includes(GATEWAY_KEY)) throw new Error("Provider credential appeared in agent persistence");
  }
  const durable = await storage();
  const persisted = await durable.exec("SELECT (SELECT COUNT(*) FROM states) AS states, (SELECT COUNT(*) FROM records) AS records, (SELECT COUNT(*) FROM events) AS events, (SELECT COUNT(*) FROM jobs WHERE status = 'completed') AS completed, (SELECT COUNT(*) FROM jobs WHERE status IN ('queued', 'running')) AS unfinished");
  if (persisted[0].unfinished !== 0) throw new Error("Durable queue did not drain");
  const report = { runtime: "local workerd with a mock AI Gateway binding", model: "mock Opus 5.5 SSE", warm, measurements, modelCalls, persisted };
  writeFileSync("output/local-measurement.json", JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
} finally { await mf.dispose(); }

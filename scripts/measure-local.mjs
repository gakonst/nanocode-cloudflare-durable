import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { localOptions } from "./local-runtime.mjs";

const live = process.argv.includes("--live");
const transport = process.argv.includes("--transport");
const prompt = live ? "Reply exactly nanocode-worker-ok." : "Say hello";
let modelCalls = 0;
let interruptNext = false;
let failNext = false;
let transportMode;
let lateBodiesCancelled = 0;

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
  if (url.hostname !== "provider.invalid" || request.headers.get("x-api-key") !== "local-provider") throw new Error("Provider request escaped configured credential policy");
  const input = await request.json();
  if (input.model !== "claude-opus-5-5" || JSON.stringify(input).includes("local-provider")) throw new Error("Invalid model or credentials entered model input");
  modelCalls++;
  const currentTransport = transportMode;
  transportMode = undefined;
  if (currentTransport === "headers") {
    await new Promise(resolve => setTimeout(resolve, 800));
    return new Response(new ReadableStream({ start(sink) { sink.enqueue(new TextEncoder().encode("late response")); }, cancel() { lateBodiesCancelled++; } }));
  }
  if (currentTransport === "body" || currentTransport === "error-body") return new Response(new ReadableStream({ start(sink) { sink.enqueue(new TextEncoder().encode("initial chunk\n")); } }), { status: currentTransport === "body" ? 200 : 400, headers: { "content-type": "text/event-stream" } });
  if (currentTransport === "active") {
    let chunks = 0;
    return new Response(new ReadableStream({ async pull(sink) { await new Promise(resolve => setTimeout(resolve, 120)); if (++chunks > 8) sink.close(); else sink.enqueue(new TextEncoder().encode("active-stream\n")); } }));
  }
  if (failNext) { failNext = false; return Response.json({ error: { type: "invalid_request_error", message: "local-provider must never reach the agent" } }, { status: 400 }); }
  if (interruptNext) { interruptNext = false; return new Response(new ReadableStream({ start() {} }), { headers: { "content-type": "text/event-stream" } }); }
  return stream(input);
};

const options = localOptions({ live, provider: live ? undefined : provider, egress: transport && !live ? { MODEL_RESPONSE_HEADERS_TIMEOUT_MS: "200", MODEL_STREAM_IDLE_TIMEOUT_MS: "400" } : {} });
let mf = new Miniflare(convertV4MiniflareOptions(options));
const name = `measure-${Date.now()}`;
const sessionName = name;
const base = `http://worker/sessions/${name}/turns`;
const headers = { authorization: "Bearer local-measurement", "content-type": "application/json" };
const request = async (url, init = {}) => mf.dispatchFetch(url, { ...init, headers });
const measurements = [];
const transportMeasurements = [];
let session;

async function until(condition, detail, timeout = live ? 120000 : 20000) {
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
const storage = () => mf.unsafeGetDurableObjectStorage("nanocode-worker", "AgentSession", { name: sessionName });
const metrics = async () => (await request(`http://worker/sessions/${name}/metrics`)).json();

try {
  await mf.ready;
  const bindings = await mf.getBindings("nanocode-worker");
  for (const key of ["ANTHROPIC_API_KEY", "OAUTH_JSON", "ACCOUNTS", "VAULT_KEY", "RUNNER_TOKEN"]) if (key in bindings) throw new Error("Provider secret boundary is violated");
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
  await mf.unsafeEvictDurableObject("nanocode-worker", "AgentSession", { name: sessionName });
  await request(`${base}/cold`);
  const readOnly = await metrics();
  if (readOnly.runtime_initializations !== 0) throw new Error("Status polling initialized WASM");
  await turn("after-eviction");
  if (!live) {
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
    if (transport) {
      const bindings = await mf.getBindings("nanocode-worker");
      const dispatch = signal => bindings.EGRESS.fetch(new Request("https://egress.internal/model", { method: "POST", signal, headers: { "content-type": "application/json" }, body: JSON.stringify({ authority: { account_id: "default", session_id: "transport-measurement" }, subscription: false, headers: [], body: JSON.stringify({ model: "claude-opus-5-5" }) }) }));
      for (const fault of ["headers", "body", "error-body", "active"]) {
        transportMode = fault;
        const start = performance.now();
        const response = await dispatch();
        let failed = false;
        let bytes = 0;
        try { bytes = (await response.arrayBuffer()).byteLength; } catch { failed = true; }
        const milliseconds = performance.now() - start;
        console.log(JSON.stringify({ fault, status: response.status, failed, bytes, milliseconds }));
        if ((fault === "headers" || fault === "error-body") && response.status !== 504) throw new Error(`Unbounded provider ${fault}`);
        const counters = await (await bindings.EGRESS.fetch(new Request("https://egress.internal/metrics"))).json();
        if (fault === "body" && (response.status !== 200 || counters.metrics.model_timeouts !== 2 || (!failed && bytes !== 14))) throw new Error("Stalled provider body did not terminate at its idle deadline");
        if (fault === "active" && (response.status !== 200 || failed || bytes !== 112 || milliseconds < 900)) throw new Error("Active stream was cut off by its header deadline");
        if (milliseconds > 3000) throw new Error("Provider transport deadline did not bound the operation");
        transportMeasurements.push({ fault, status: response.status, failed, bytes, milliseconds });
      }
      transportMode = "headers";
      const controller = new AbortController();
      const start = performance.now();
      const pending = dispatch(controller.signal);
      setTimeout(() => controller.abort(), 50);
      await pending.then(response => response.body?.cancel(), () => {});
      if (performance.now() - start > 1000) throw new Error("Provider cancellation stalled");
      transportMeasurements.push({ fault: "cancelled", milliseconds: performance.now() - start });
      await new Promise(resolve => setTimeout(resolve, 900));
      transportMeasurements.push({ late_bodies_cancelled: lateBodiesCancelled });
      for (const fault of ["headers", "body"]) {
        transportMode = fault;
        const start = performance.now();
        await turn(`sdk-${fault}-timeout`, ["completed", "failed"]);
        if (performance.now() - start > 10000) throw new Error("Stalled provider blocked the SDK queue");
      }
      await turn("after-transport-timeouts");
    }
    const savedState = await restored.exec("SELECT payload FROM states UNION ALL SELECT value AS payload FROM records UNION ALL SELECT body AS payload FROM events UNION ALL SELECT result AS payload FROM jobs");
    if (JSON.stringify(savedState).includes("local-provider")) throw new Error("Provider credential appeared in agent persistence");
  }
  const durable = await storage();
  const persisted = await durable.exec("SELECT (SELECT COUNT(*) FROM states) AS states, (SELECT COUNT(*) FROM records) AS records, (SELECT COUNT(*) FROM events) AS events, (SELECT COUNT(*) FROM jobs WHERE status = 'completed') AS completed, (SELECT COUNT(*) FROM jobs WHERE status IN ('queued', 'running')) AS unfinished");
  if (persisted[0].unfinished !== 0) throw new Error("Durable queue did not drain");
  const report = { runtime: "local workerd", model: live ? "live Opus 5.5" : "mock Opus 5.5 SSE", warm, measurements, transportMeasurements, modelCalls: live ? undefined : modelCalls, persisted };
  writeFileSync(live ? "output/subscription-measurement.json" : "output/local-measurement.json", JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
} finally { await mf.dispose(); }

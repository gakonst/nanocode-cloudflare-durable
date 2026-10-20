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

const CODEX_TOOL_ROUNDS = 3;
const codexRequests = [];
const codexStored = new Set();
const codexRounds = new Map();
let codexTools = [];
let codexHang = false;

function responses(id, item) {
  const events = [
    { type: "response.created", response: { id, status: "in_progress" } },
    { type: "response.output_item.added", output_index: 0, item: item.type === "message" ? { ...item, status: "in_progress", content: [] } : item },
    ...(item.type === "message" ? [{ type: "response.output_text.delta", output_index: 0, item_id: item.id, content_index: 0, delta: item.content[0].text }] : []),
    { type: "response.output_item.done", output_index: 0, item },
    { type: "response.completed", response: { id, status: "completed", model: "gpt-6.1-sol", output: [item], usage: { input_tokens: 8, output_tokens: 4, total_tokens: 12 } } },
  ];
  return new Response(events.map(event => `data: ${JSON.stringify(event)}\n\n`).join(""), { headers: { "content-type": "text/event-stream", "x-request-id": `req_${id}` } });
}

function codex(text) {
  const input = JSON.parse(text);
  const items = Array.isArray(input.input) ? input.input : [];
  for (const item of items) if (item.type === "additional_tools" && Array.isArray(item.tools)) codexTools = item.tools.map(tool => tool.name);
  const entry = { model: input.model, store: input.store, stream: input.stream, input_items: items.length, body_bytes: Buffer.byteLength(text), previous: input.previous_response_id ?? null, credential_free: !text.includes(GATEWAY_KEY), status: 200 };
  codexRequests.push(entry);
  if (entry.previous && !codexStored.has(entry.previous)) {
    entry.status = 400;
    return Response.json({ error: { message: `Previous response with id '${entry.previous}' not found.`, type: "invalid_request_error", param: "previous_response_id", code: "previous_response_not_found" } }, { status: 400 });
  }
  const user = items.findLastIndex(item => item.type === "message" && item.role === "user");
  const round = (user >= 0 ? 0 : codexRounds.get(entry.previous) ?? 0) + items.filter((item, index) => index > user && item.type === "function_call_output").length;
  const id = `resp_${codexRequests.length}`;
  entry.id = id;
  codexRounds.set(id, round);
  if (input.store) codexStored.add(id);
  if (codexHang && round === 1) {
    codexHang = false;
    entry.held = true;
    return new Response(new ReadableStream({ start() {} }), { headers: { "content-type": "text/event-stream" } });
  }
  const call = codexRequests.length;
  return responses(id, round < CODEX_TOOL_ROUNDS
    ? { type: "function_call", id: `fc_${call}`, call_id: `call_${call}`, name: "list_agents", arguments: "{}", status: "completed" }
    : { type: "message", id: `msg_${call}`, role: "assistant", status: "completed", content: [{ type: "output_text", text: "codex gateway response" }] });
}

const provider = async request => {
  const url = new URL(request.url);
  if (url.hostname === "provider.invalid" && url.pathname === "/openai/v1/responses") {
    if (request.headers.get("authorization") !== `Bearer ${GATEWAY_KEY}`) throw new Error("Codex request escaped the gateway credential policy");
    return codex(await request.text());
  }
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
  const codexName = `codex-${Date.now()}`;
  const codexPath = `http://worker/sessions/${codexName}`;
  const codexTurn = async (id, cancel = false) => {
    const from = codexRequests.length;
    const started = performance.now();
    const submitted = await request(`${codexPath}/turns`, { method: "POST", body: JSON.stringify({ request_id: id, prompt, provider: "codex" }) });
    if (submitted.status !== 202) throw new Error(`Codex submit rejected: ${await submitted.text()}`);
    if (cancel) {
      await until(() => codexRequests.slice(from).some(entry => entry.held), `Codex turn ${id} did not reach its held call`);
      await request(`${codexPath}/turns/${id}/cancel`, { method: "POST" });
    }
    let result;
    await until(async () => {
      result = await (await request(`${codexPath}/turns/${id}`)).json();
      return ["failed", "completed", "cancelled"].includes(result.status);
    }, `Codex turn ${id} stalled`);
    const calls = codexRequests.slice(from);
    measurements.push({ id: `codex-${id}`, milliseconds: performance.now() - started, result, calls });
    return { result, calls, answered: calls.filter(entry => entry.status === 200) };
  };
  const replays = turn => turn.answered.slice(1).every((entry, index) => entry.input_items > turn.answered[index].input_items);
  const codexFirst = await codexTurn("first");
  const codexSecond = await codexTurn("second");
  await mf.unsafeEvictDurableObject("nanocode", "AgentSession", { name: JSON.stringify(["owner", codexName]) });
  const codexRestored = await codexTurn("after-eviction");
  codexHang = true;
  const codexCancelled = await codexTurn("cancelled", true);
  const codexResumed = await codexTurn("after-cancellation");
  const codexCompleted = [codexFirst, codexSecond, codexRestored, codexResumed];
  if (!codexTools.includes("list_agents")) throw new Error(`Codex tools did not include list_agents: ${codexTools}`);
  if (!codexCompleted.every(turn => turn.result.ok?.message === "codex gateway response" && turn.result.ok.model === "gpt-6.1-sol" && turn.answered.length === CODEX_TOOL_ROUNDS + 1) || codexCancelled.result.status !== "cancelled") throw new Error(`Codex turns did not complete through AI Gateway: ${JSON.stringify(codexCompleted.map(turn => turn.result))}`);
  if (!codexRequests.every(entry => entry.model === "gpt-6.1-sol" && entry.store === false && entry.previous === null && entry.stream && entry.credential_free)) throw new Error(`Codex gateway requests were not stateless: ${JSON.stringify(codexRequests)}`);
  if (!codexCompleted.every(replays) || codexSecond.answered[0].input_items <= codexFirst.answered.at(-1).input_items) throw new Error(`Codex model calls did not replay the full history: ${JSON.stringify(codexRequests)}`);
  const durable = await storage();
  const persisted = await durable.exec("SELECT (SELECT COUNT(*) FROM states) AS states, (SELECT COUNT(*) FROM records) AS records, (SELECT COUNT(*) FROM events) AS events, (SELECT COUNT(*) FROM jobs WHERE status = 'completed') AS completed, (SELECT COUNT(*) FROM jobs WHERE status IN ('queued', 'running')) AS unfinished");
  if (persisted[0].unfinished !== 0) throw new Error("Durable queue did not drain");
  const report = { runtime: "local workerd with a mock AI Gateway binding", model: "mock Opus 5.5 and Sol SSE", warm, measurements, modelCalls, codexRequests, persisted };
  writeFileSync("output/local-measurement.json", JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
} finally { await mf.dispose(); }

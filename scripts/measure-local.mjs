import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { mkdirSync, mkdtempSync, readdirSync, writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { resolve } from "node:path";
import { credentials } from "./credentials.mjs";

mkdirSync("output", { recursive: true });
let modelCalls = 0;
let interruptNext = false;
let failNext = false;
const live = process.argv.includes("--live");
const prompt = live ? "Reply exactly nanocode-worker-ok." : "Say hello";
const options = {
  name: "nanocode-worker",
  modules: [
    { type: "ESModule", path: "output/deploy/worker.js" },
    ...readdirSync("output/deploy").filter(file => file.endsWith(".wasm")).map(file => ({ type: "CompiledWasm", path: `output/deploy/${file}` })),
  ],
  compatibilityDate: "2026-10-16",
  unsafeInspectDurableObjects: true,
  ...(live ? {} : { resourcePersistencePath: resolve(mkdtempSync("output/local-state-")) }),
  durableObjects: { SESSIONS: { className: "AgentSession", useSQLite: true }, SUBSCRIPTION: { className: "SubscriptionAuth", useSQLite: true } },
  bindings: { WORKER_TOKEN: "local-measurement", ANTHROPIC_API_KEY: "local-provider", ANTHROPIC_ENDPOINT: "https://provider.invalid/v1/messages", RECOVERY_DELAY_MS: "1000" },
  outboundService: async request => {
    const input = await request.json();
    if (input.model !== "claude-opus-5-5") throw new Error("unexpected model");
    modelCalls++;
    if (failNext) {
      failNext = false;
      return Response.json({ type: "error", error: { type: "invalid_request_error", message: "measurement rejection" } }, { status: 400 });
    }
    if (interruptNext) {
      interruptNext = false;
      return new Response(new ReadableStream({ start() {} }), { headers: { "content-type": "text/event-stream" } });
    }
    const events = [
      ["message_start", { type: "message_start", message: { id: `message-${modelCalls}`, type: "message", role: "assistant", model: input.model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 8, output_tokens: 0 } } }],
      ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }],
      ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "durable wasm response" } }],
      ["content_block_stop", { type: "content_block_stop", index: 0 }],
      ["message_delta", { type: "message_delta", delta: { stop_reason: "end_turn", stop_sequence: null }, usage: { output_tokens: 4 } }],
      ["message_stop", { type: "message_stop" }],
    ];
    return new Response(events.map(([event, data]) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`).join(""), { headers: { "content-type": "text/event-stream" } });
  },
};
if (live) {
  options.bindings.OAUTH_JSON = JSON.stringify(credentials(process.env.OAUTH_FILE));
  delete options.bindings.ANTHROPIC_API_KEY;
  delete options.bindings.ANTHROPIC_ENDPOINT;
  delete options.outboundService;
}
let mf = new Miniflare(convertV4MiniflareOptions(options));
const name = `measure-${Date.now()}`;
const base = `http://worker/sessions/${name}/turns`;
const headers = { authorization: "Bearer local-measurement", "content-type": "application/json" };
const request = async (url, init = {}) => mf.dispatchFetch(url, { ...init, headers });
const measurements = [];
let session;
async function submit(id) {
  const accepted = await request(base, { method: "POST", body: JSON.stringify({ request_id: id, prompt }) });
  if (accepted.status !== 202) throw new Error(await accepted.text());
}
async function waitForTurn(id, start = performance.now(), expected = "completed") {
  for (;;) {
    const response = await request(`${base}/${encodeURIComponent(id)}`);
    if (response.status !== 200) throw new Error(await response.text());
    const result = await response.json();
    if (result.status === "failed" || result.status === "completed") {
      if (result.status !== expected) throw new Error(JSON.stringify(result));
      if (result.ok) {
        session ??= result.ok.session_id;
        if (result.ok.session_id !== session) throw new Error("session identity changed after restoration");
      }
      measurements.push({ id, milliseconds: performance.now() - start, result });
      return;
    }
    if (performance.now() - start > (live ? 60000 : 15000)) throw new Error(`turn stalled: ${JSON.stringify(result)}, model calls: ${modelCalls}`);
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}
async function turn(id, expected = "completed") {
  const start = performance.now();
  await submit(id);
  await waitForTurn(id, start, expected);
}
async function waitUntil(condition, detail) {
  const start = performance.now();
  while (!await condition()) {
    if (performance.now() - start > 15000) throw new Error(detail);
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}
const storage = () => mf.unsafeGetDurableObjectStorage("nanocode-worker", "AgentSession", { name });
try {
  await mf.ready;
  await turn("cold");
  const before = modelCalls;
  const replayStart = performance.now();
  const replay = await request(base, { method: "POST", body: JSON.stringify({ request_id: "cold", prompt }) });
  if (replay.status !== 200 || modelCalls !== before) throw new Error("duplicate request dispatched model work");
  measurements.push({ id: "duplicate", milliseconds: performance.now() - replayStart });
  const conflict = await request(base, { method: "POST", body: JSON.stringify({ request_id: "cold", prompt: "Different prompt" }) });
  if (conflict.status !== 409) throw new Error("request identity conflict was accepted");
  await mf.unsafeEvictDurableObject("nanocode-worker", "AgentSession", { name });
  await turn("after-eviction");
  if (!live) {
    const durable = await storage();
    await durable.exec("CREATE TRIGGER measurement_outage BEFORE UPDATE ON states BEGIN SELECT RAISE(ABORT, 'measurement checkpoint outage'); END");
    const checkpointStart = performance.now();
    const callsBeforeOutage = modelCalls;
    await submit("checkpoint-recovery");
    await waitUntil(async () => {
      const result = await (await request(`${base}/checkpoint-recovery`)).json();
      if (result.status === "failed") throw new Error("checkpoint outage was saved as a terminal failure");
      return result.status === "running" && result.error;
    }, "checkpoint outage did not schedule recovery");
    if (modelCalls !== callsBeforeOutage) throw new Error("model ran before checkpoint commit");
    await durable.exec("DROP TRIGGER measurement_outage");
    await waitForTurn("checkpoint-recovery", checkpointStart);
    failNext = true;
    await turn("provider-rejection", "failed");
    const failedCalls = modelCalls;
    const failedReplay = await request(base, { method: "POST", body: JSON.stringify({ request_id: "provider-rejection", prompt }) });
    if (failedReplay.status !== 200 || modelCalls !== failedCalls) throw new Error("terminal failure dispatched model work");
    interruptNext = true;
    const calls = modelCalls;
    await submit("interrupted");
    await waitUntil(() => modelCalls > calls, "interrupted model call did not start");
    await Promise.all([submit("interrupted"), submit("queued-a"), submit("queued-b")]);
    if (modelCalls !== calls + 1) throw new Error("queued turns ran concurrently in one session");
    const restartStart = performance.now();
    await mf.dispose();
    mf = new Miniflare(convertV4MiniflareOptions(options));
    await mf.ready;
    await waitUntil(() => modelCalls > calls + 1, "persisted alarm did not resume work without client requests");
    await waitForTurn("interrupted", restartStart);
    await waitForTurn("queued-a", restartStart);
    await waitForTurn("queued-b", restartStart);
    if (modelCalls !== calls + 4) throw new Error("restart duplicated queued model calls");
    const restored = await storage();
    await restored.exec("UPDATE jobs SET status = 'running', result = NULL WHERE request_id = 'queued-b'");
    const receiptCalls = modelCalls;
    await turn("queued-b");
    if (modelCalls !== receiptCalls) throw new Error("completed SDK receipt dispatched model work");
    delete options.bindings.ANTHROPIC_API_KEY;
    await mf.setOptions(convertV4MiniflareOptions(options));
    const saved = await request(base, { method: "POST", body: JSON.stringify({ request_id: "cold", prompt }) });
    if (saved.status !== 200) throw new Error("saved result requires provider credentials");
  }
  const durable = await storage();
  const persisted = await durable.exec("SELECT (SELECT COUNT(*) FROM states) AS states, (SELECT COUNT(*) FROM records) AS records, (SELECT COUNT(*) FROM jobs WHERE status = 'completed') AS completed, (SELECT COUNT(*) FROM jobs WHERE status IN ('queued', 'running')) AS unfinished");
  if (persisted[0].unfinished !== 0) throw new Error("durable queue did not drain");
  const report = { runtime: "local workerd", model: live ? "subscription Opus 5.5" : "mock Opus 5.5 SSE", measurements, modelCalls: live ? undefined : modelCalls, persisted };
  writeFileSync(live ? "output/subscription-measurement.json" : "output/local-measurement.json", JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
} finally {
  await mf.dispose();
}

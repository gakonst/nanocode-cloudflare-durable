import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { gunzipSync } from "node:zlib";
import { LOCAL_TOKEN, localOptions } from "./local-runtime.mjs";

const ENDPOINT = "https://api.anthropic.com/v1/messages";
const LARGE_BYTES = 20000;
const received = [];
let reject = null;
const assertions = [];
const check = (value, message) => { if (!value) throw new Error(message); assertions.push(message); };

function sse(model, text) {
  const events = [
    ["message_start", { type: "message_start", message: { id: "message-gzip", type: "message", role: "assistant", model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 8, output_tokens: 0 } } }],
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
  if (url.origin + url.pathname !== ENDPOINT) throw new Error(`Unexpected egress destination ${url.origin}`);
  const raw = Buffer.from(await request.arrayBuffer());
  const encoding = request.headers.get("content-encoding");
  const text = encoding === "gzip" ? gunzipSync(raw).toString() : raw.toString();
  received.push({ encoding, bytes: raw.length, text });
  if (encoding === "gzip" && reject === "proxy") return new Response("Unsupported Media Type", { status: 415 });
  if (encoding === "gzip" && reject === "json") return Response.json({ type: "error", error: { type: "invalid_request_error", message: "The request body is not valid JSON: expected value at line 1 column 1" } }, { status: 400, headers: { "request-id": "req_gzip" } });
  if (reject === "other") return Response.json({ type: "error", error: { type: "invalid_request_error", message: "max_tokens is too large" } }, { status: 400, headers: { "request-id": "req_gzip" } });
  const body = JSON.parse(text);
  if (body.stream) return sse(body.model, "gzip-ok");
  return Response.json({ ok: true });
};

const options = localOptions({ provider, egress: { ANTHROPIC_ENDPOINT: ENDPOINT } });
let mf;
const launch = async () => { mf = new Miniflare(convertV4MiniflareOptions(options)); await mf.ready; };
const payload = bytes => JSON.stringify({ model: "claude-opus-5-5", max_tokens: 16, messages: [{ role: "user", content: "x".repeat(Math.max(0, bytes - 80)) }] });
async function dispatch(body) {
  const bindings = await mf.getBindings("nanocode-worker");
  const response = await bindings.EGRESS.fetch(new Request("https://egress.internal/model", { method: "POST", headers: { "content-type": "application/octet-stream", "x-nanocode-model": JSON.stringify({ authority: { account_id: "default", session_id: "gzip-measurement" }, subscription: false, headers: [["content-type", "application/json"]] }) }, body }));
  await response.text();
  return response.status;
}
async function metrics() {
  const bindings = await mf.getBindings("nanocode-worker");
  return (await (await bindings.EGRESS.fetch(new Request("https://egress.internal/metrics"))).json()).metrics;
}
const padding = (sent, body) => sent.startsWith(body) && /^ [ \t]{0,256}$/.test(sent.slice(body.length));

try {
  await launch();
  const small = payload(1000);
  check(await dispatch(small) === 200 && received.at(-1).encoding === null && received.at(-1).text === small, "Bodies under 4096 bytes are sent as identity");
  const large = payload(LARGE_BYTES);
  check(await dispatch(large) === 200 && received.at(-1).encoding === "gzip" && padding(received.at(-1).text, large) && received.at(-1).bytes < large.length / 10, "Larger Messages bodies are padded with spaces or tabs and gzipped");
  const pads = new Set();
  for (let index = 0; index < 8; index++) { await dispatch(large); pads.add(received.at(-1).text.length - large.length); }
  check(pads.size > 1, "The padding length varies between requests");

  reject = "other";
  const before = received.length;
  check(await dispatch(large) === 400 && received.length === before + 1 && received.at(-1).encoding === "gzip", "An ordinary Anthropic 400 is returned without resending");
  reject = null;
  await dispatch(large);
  check(received.at(-1).encoding === "gzip", "Gzip stays on after an unrelated Anthropic error");

  reject = "json";
  const jsonBefore = received.length;
  check(await dispatch(large) === 200 && received.length === jsonBefore + 2 && received.at(-2).encoding === "gzip" && received.at(-1).encoding === null && received.at(-1).text === large, "An Anthropic invalid-JSON 400 resends the identity body");
  reject = null;
  await dispatch(large);
  check(received.at(-1).encoding === null, "Gzip stays off after a rejection");
  const afterJson = await metrics();
  await mf.dispose();

  await launch();
  reject = "proxy";
  const proxyBefore = received.length;
  check(await dispatch(large) === 200 && received.length === proxyBefore + 2 && received.at(-2).encoding === "gzip" && received.at(-1).encoding === null, "A 415 that did not come from Anthropic resends the identity body");
  reject = null;

  const sessions = `http://worker/sessions/gzip-${Date.now()}/turns`;
  const headers = { authorization: `Bearer ${LOCAL_TOKEN}`, "content-type": "application/json" };
  await mf.dispose();
  await launch();
  const turnBefore = received.length;
  const started = performance.now();
  const submitted = await mf.dispatchFetch(sessions, { method: "POST", headers, body: JSON.stringify({ request_id: "gzip-turn", prompt: "Reply with gzip-ok" }) });
  check(submitted.status === 202, "The worker accepts a Claude turn");
  let result;
  while (performance.now() - started < 30000) {
    result = await (await mf.dispatchFetch(`${sessions}/gzip-turn`, { headers })).json();
    if (["completed", "failed", "cancelled"].includes(result.status)) break;
    await new Promise(resolve => setTimeout(resolve, 20));
  }
  const turnRequests = received.slice(turnBefore);
  check(result.status === "completed" && JSON.stringify(result.ok?.message).includes("gzip-ok") && turnRequests.length > 0 && turnRequests.every(entry => entry.encoding === "gzip" && JSON.parse(entry.text).stream === true), "A WASM agent turn sends gzipped Messages bodies through egress and completes");
  const turnMetrics = await metrics();

  const report = {
    runtime: "local workerd (Miniflare): WASM SDK session, private egress and vault, mock Anthropic Messages endpoint at api.anthropic.com",
    assertions,
    large_body: { identity_bytes: large.length, gzip_bytes: received.find(entry => entry.encoding === "gzip")?.bytes },
    turn: { requests: turnRequests.length, identity_bytes: turnRequests.map(entry => entry.text.length), gzip_bytes: turnRequests.map(entry => entry.bytes) },
    metrics: { first_isolate: { requests: afterJson.model_gzip_requests, rejections: afterJson.model_gzip_rejections }, turn_isolate: { requests: turnMetrics.model_gzip_requests, input_bytes: turnMetrics.model_gzip_input_bytes, output_bytes: turnMetrics.model_gzip_output_bytes } },
  };
  writeFileSync("output/gzip-measurement.json", JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
} finally { await mf?.dispose(); }

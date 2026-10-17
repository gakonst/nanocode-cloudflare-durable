import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { randomBytes } from "node:crypto";
import { writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { localOptions } from "./local-runtime.mjs";

const credentials = { demo: randomBytes(32).toString("base64url"), vault: randomBytes(32).toString("base64url") };
const calls = { model: 0, demo: 0, vault: 0 };
const receipts = [];
function response(input, blocks, stop_reason) {
  const events = [["message_start", { type: "message_start", message: { id: `capabilities-${calls.model}`, type: "message", role: "assistant", model: input.model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 8, output_tokens: 0 } } }]];
  blocks.forEach((block, index) => {
    const delta = block.type === "tool_use" ? { type: "input_json_delta", partial_json: JSON.stringify(block.input) } : { type: "text_delta", text: block.text };
    events.push(["content_block_start", { type: "content_block_start", index, content_block: { ...block, input: block.input ? {} : undefined, text: block.text ? "" : undefined } }], ["content_block_delta", { type: "content_block_delta", index, delta }], ["content_block_stop", { type: "content_block_stop", index }]);
  });
  events.push(["message_delta", { type: "message_delta", delta: { stop_reason, stop_sequence: null }, usage: { output_tokens: 4 } }], ["message_stop", { type: "message_stop" }]);
  return new Response(events.map(([event, data]) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`).join(""), { headers: { "content-type": "text/event-stream" } });
}
const provider = async request => {
  const host = new URL(request.url).hostname;
  if (host !== "provider.invalid") {
    const id = host === "connector.invalid" ? "demo" : host === "vault.invalid" ? "vault" : undefined;
    if (!id || request.headers.get("authorization") !== `Bearer ${credentials[id]}`) throw new Error("Credential injection escaped connector policy");
    calls[id]++;
    return Response.json({ answer: 42, secret: credentials[id] });
  }
  calls.model++;
  const input = await request.json();
  for (const value of Object.values(credentials)) if (JSON.stringify(input).includes(value)) throw new Error("Credential reached the model");
  const results = input.messages.flatMap(message => Array.isArray(message.content) ? message.content.filter(block => block.type === "tool_result") : []);
  if (results.some(block => block.is_error)) throw new Error("Credential proxy tool failed");
  receipts.splice(0, Infinity, ...results.map(result => typeof result.content === "string" ? result.content : result.content.map(part => part.text ?? "").join("")));
  if (results.length === 0) return response(input, [{ type: "tool_use", id: "catalog", name: "list_connections", input: {} }], "tool_use");
  if (results.length === 1) return response(input, [
    { type: "tool_use", id: "connector", name: "connector_request", input: { connection_id: "demo", path: "/safe", headers: { authorization: "agent-selected-credential" } } },
    { type: "tool_use", id: "vault", name: "vault_request", input: { connection_id: "vault", path: "/safe" } },
  ], "tool_use");
  return response(input, [{ type: "text", text: "capabilities-ok" }], "end_turn");
};
const connections = Object.fromEntries(Object.entries(credentials).map(([id, value]) => [id, { kind: id === "vault" ? "vault" : "connector", base_url: id === "demo" ? "https://connector.invalid" : "https://vault.invalid", methods: ["GET"], paths: ["/safe"], response_fields: ["answer"], credential: { type: "static", value } }]));
const mf = new Miniflare(convertV4MiniflareOptions(localOptions({ provider, egress: { CONNECTIONS_JSON: JSON.stringify(connections) } })));
const headers = { authorization: "Bearer local-measurement", "content-type": "application/json" };
const base = "https://worker/sessions/capabilities/turns";
try {
  await mf.ready;
  const started = performance.now();
  const submit = await mf.dispatchFetch(base, { method: "POST", headers, body: JSON.stringify({ request_id: "capabilities", prompt: "Use approved connections" }) });
  if (submit.status !== 202) throw new Error("Capability request was not admitted");
  let result;
  do {
    result = await (await mf.dispatchFetch(`${base}/capabilities`, { headers })).json();
    if (performance.now() - started > 15000) throw new Error("Capability turn did not settle");
    if (result.status === "failed") throw new Error("Capability turn failed");
    await new Promise(resolve => setTimeout(resolve, 5));
  } while (result.status !== "completed");
  if (calls.model !== 3 || calls.demo !== 1 || calls.vault !== 1 || receipts.length !== 3) throw new Error("Unexpected capability dispatch count");
  if (!receipts[1].includes('"answer":42') || receipts[2].includes('"data"') || !receipts[2].includes('"status":200')) throw new Error("Connector or vault projection was incorrect");
  const durable = await mf.unsafeGetDurableObjectStorage("nanocode-worker", "AgentSession", { name: "capabilities" });
  const persistence = await durable.exec("SELECT payload FROM states UNION ALL SELECT value AS payload FROM records UNION ALL SELECT body AS payload FROM events");
  for (const value of Object.values(credentials)) if (JSON.stringify(persistence).includes(value)) throw new Error("Credential entered agent persistence");
  const report = { runtime: "local workerd, WASM SDK", calls, tool_receipts: receipts.length, credential_free_model_and_persistence: true, vault_status_only: true, milliseconds: performance.now() - started };
  writeFileSync("output/capabilities-measurement.json", JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
} finally { await mf.dispose(); }

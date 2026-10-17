import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { createServer as createSocketServer } from "node:net";
import { once } from "node:events";
import { mkdirSync, mkdtempSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { relative, resolve } from "node:path";
import { performance } from "node:perf_hooks";

mkdirSync("output", { recursive: true });
let modelCalls = 0;
let interrupt = false;
const provider = createServer(async (request, response) => {
  if (request.headers["x-api-key"] !== "local-provider") { response.writeHead(403).end(); return; }
  let body = "";
  for await (const chunk of request) body += chunk;
  const input = JSON.parse(body);
  if (JSON.stringify(input).includes("local-provider")) throw new Error("Provider credential entered agent input");
  modelCalls++;
  response.writeHead(200, { "content-type": "text/event-stream" });
  if (interrupt) { interrupt = false; response.flushHeaders(); return; }
  const events = [
    ["message_start", { type: "message_start", message: { id: `message-${modelCalls}`, type: "message", role: "assistant", model: input.model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 8, output_tokens: 0 } } }],
    ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }],
    ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "durable wasm response" } }],
    ["content_block_stop", { type: "content_block_stop", index: 0 }],
    ["message_delta", { type: "message_delta", delta: { stop_reason: "end_turn", stop_sequence: null }, usage: { output_tokens: 4 } }],
    ["message_stop", { type: "message_stop" }],
  ];
  response.end(events.map(([event, data]) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`).join(""));
});
provider.listen(0, "127.0.0.1");
await once(provider, "listening");
const listener = createSocketServer();
listener.listen(0, "127.0.0.1");
await once(listener, "listening");
const port = listener.address().port;
await new Promise(resolve => listener.close(resolve));
const publicConfig = JSON.parse(readFileSync("wrangler.jsonc", "utf8"));
publicConfig.main = resolve(publicConfig.main);
publicConfig.build.cwd = process.cwd();
publicConfig.build.watch_dir = publicConfig.build.watch_dir.map(path => relative(resolve("output"), resolve(path)));
publicConfig.vars = { ...publicConfig.vars, WORKER_TOKEN: "local-measurement", MODEL_AUTH: "api_key", RECOVERY_DELAY_MS: "1000" };
publicConfig.observability = { enabled: false };
const egressConfig = JSON.parse(readFileSync("wrangler.egress.jsonc", "utf8"));
egressConfig.main = resolve(egressConfig.main);
egressConfig.vars = { ...egressConfig.vars, ANTHROPIC_API_KEY: "local-provider", ANTHROPIC_ENDPOINT: `http://127.0.0.1:${provider.address().port}/v1/messages` };
egressConfig.observability = { enabled: false };
writeFileSync("output/wrangler-measurement.json", JSON.stringify(publicConfig));
writeFileSync("output/wrangler-egress-measurement.json", JSON.stringify(egressConfig));
const state = resolve(mkdtempSync("output/wrangler-state-"));
let child;
let logs = "";
const headers = { authorization: "Bearer local-measurement", "content-type": "application/json" };
const base = `http://127.0.0.1:${port}/sessions/wrangler-measurement/turns`;
const measurements = [];
const sourcePath = "src/worker.ts";
const originalSource = readFileSync(sourcePath, "utf8");
const until = async (condition, detail, timeout = 20000) => {
  const start = performance.now();
  while (!await condition()) {
    if (performance.now() - start > timeout) throw new Error(detail);
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};
const start = async () => {
  const offset = logs.length;
  child = spawn(process.execPath, ["node_modules/wrangler/bin/wrangler.js", "dev", "--local", "--config", "output/wrangler-measurement.json", "--config", "output/wrangler-egress-measurement.json", "--persist-to", state, "--ip", "127.0.0.1", "--port", String(port), "--inspector-port", "0", "--show-interactive-dev-session=false"], { detached: true, stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, WRANGLER_SEND_METRICS: "false" } });
  child.stdout.on("data", data => { logs += data; });
  child.stderr.on("data", data => { logs += data; });
  await until(() => {
    if (child.exitCode !== null) throw new Error("wrangler exited before readiness");
    return logs.slice(offset).includes("Ready on");
  }, "wrangler did not become ready", 60000);
};
const stop = async () => {
  if (!child || child.exitCode !== null) return;
  const exited = once(child, "exit");
  process.kill(-child.pid, "SIGKILL");
  await exited;
  child = undefined;
};
const submit = async id => {
  const response = await fetch(base, { method: "POST", headers, body: JSON.stringify({ request_id: id, prompt: "Say hello" }) });
  if (response.status !== 202) throw new Error(await response.text());
};
const complete = async id => {
  let result;
  await until(async () => {
    result = await (await fetch(`${base}/${id}`, { headers })).json();
    if (result.status === "failed") throw new Error(JSON.stringify(result));
    return result.status === "completed";
  }, "wrangler job stalled");
  return result;
};
try {
  const startup = performance.now();
  await start();
  measurements.push({ id: "startup", milliseconds: performance.now() - startup, compiled_wasm: /Compiling /.test(logs) });
  await submit("cold");
  const initial = await complete("cold");
  const unchangedWasm = statSync("dist/nanocode_worker.wasm").mtimeMs;
  const reload = performance.now();
  const changedSource = originalSource.replace('credential_boundary: "private-egress"', 'credential_boundary: "private-egress-reload"');
  if (changedSource === originalSource) throw new Error("Reload measurement anchor is absent");
  writeFileSync(sourcePath, changedSource);
  await until(async () => {
    try { return (await (await fetch(`http://127.0.0.1:${port}/health`, { headers })).json()).credential_boundary === "private-egress-reload"; } catch { return false; }
  }, "TypeScript source change did not become active");
  measurements.push({ id: "typescript-reload", milliseconds: performance.now() - reload });
  if (statSync("dist/nanocode_worker.wasm").mtimeMs !== unchangedWasm) throw new Error("TypeScript change rewrote identical WASM");
  writeFileSync(sourcePath, originalSource);
  await until(async () => {
    try { return (await (await fetch(`http://127.0.0.1:${port}/health`, { headers })).json()).credential_boundary === "private-egress"; } catch { return false; }
  }, "Source restoration did not become active");
  interrupt = true;
  await submit("interrupted");
  await until(() => modelCalls === 2, "interrupted inference did not start");
  await submit("queued");
  const recovery = performance.now();
  await stop();
  await start();
  await until(() => modelCalls > 2, "Persisted alarm needed a client to resume");
  const restored = await complete("interrupted");
  await complete("queued");
  if (initial.ok.session_id !== restored.ok.session_id || modelCalls !== 4) throw new Error("Wrangler recovery changed identity or duplicated requests");
  measurements.push({ id: "process-restart-and-drain", milliseconds: performance.now() - recovery });
  const report = { runtime: "wrangler dev, two local Workers", measurements, modelCalls, session_identity_preserved: true, typescript_reload_preserved_wasm: true };
  writeFileSync("output/wrangler-measurement-report.json", JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
} finally {
  writeFileSync(sourcePath, originalSource);
  await stop();
  provider.closeAllConnections();
  provider.close();
  writeFileSync("output/wrangler-measurement.log", logs);
}

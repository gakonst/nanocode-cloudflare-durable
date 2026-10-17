import { Miniflare, convertV4MiniflareOptions, fetch as upgrade } from "miniflare";
import { spawn, spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { localOptions } from "./local-runtime.mjs";

const sdk = resolve(process.env.NANOCODE_SDK_PATH ?? "../nanocodex");
const runnerBinary = resolve(process.env.CARGO_TARGET_DIR ?? resolve(sdk, "target"), "debug/examples/runner");
const policy = { max_frame_bytes: 1048576, timeout_ms: 30000 };
const latencySteps = 20;
const token = randomBytes(32).toString("base64url");
const workspace = resolve(mkdtempSync("output/remote-workspace-"));
const persistence = resolve(mkdtempSync("output/remote-state-"));
const groups = new Set();
let port = 0;
let runner;
let runnerConnections = 0;
let modelCalls = 0;
const observed = {};
const gaps = [];
let lastResponseAt;

let supervising = true;
let runnerStarts = 0;
let runnerReady;
let runnerFailure;

const stopRunners = () => {
  supervising = false;
  for (const group of groups) {
    try { process.kill(-group, "SIGKILL"); } catch (error) { if (error.code !== "ESRCH") throw error; }
  }
  groups.clear();
};
process.on("exit", stopRunners);
for (const signal of ["SIGINT", "SIGTERM"]) process.on(signal, () => { stopRunners(); process.exit(130); });

function buildRunner() {
  const start = performance.now();
  const result = spawnSync("cargo", ["build", "--locked", "-p", "nanocode", "--example", "runner"], { cwd: sdk, stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error("Runner build failed");
  return performance.now() - start;
}

function startRunner() {
  runnerStarts++;
  const child = spawn(runnerBinary, ["--listen", `127.0.0.1:${port}`, "--workspace", workspace, "--max-frame-bytes", String(policy.max_frame_bytes), "--timeout-ms", String(policy.timeout_ms)], { env: { ...process.env, NANOCODE_RUNNER_TOKEN: token }, stdio: ["ignore", "ignore", "pipe"], detached: true });
  groups.add(child.pid);
  let stderr = "";
  const exited = new Promise(resolve => child.once("exit", (code, signal) => {
    resolve({ code, signal });
    if (supervising && runner.child === child) startRunner().catch(error => { runnerFailure = error; });
  }));
  runnerReady = new Promise((resolve, reject) => {
    child.stderr.on("data", chunk => {
      stderr += chunk;
      const match = /runner listening ws:\/\/(\S+)/.exec(stderr);
      if (match) resolve(match[1]);
    });
    exited.then(() => reject(new Error(`Runner exited before listening: ${stderr}`)));
  }).then(address => { port = Number(address.slice(address.lastIndexOf(":") + 1)); });
  runner = { child, exited };
  return runnerReady;
}

function sse(events) {
  lastResponseAt = performance.now();
  return new Response(events.map(([event, data]) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`).join(""), { headers: { "content-type": "text/event-stream" } });
}

function start(input) {
  return ["message_start", { type: "message_start", message: { id: `message-${modelCalls}`, type: "message", role: "assistant", model: input.model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 8, output_tokens: 0 } } }];
}

function reply(input, text) {
  return sse([
    start(input),
    ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }],
    ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text } }],
    ["content_block_stop", { type: "content_block_stop", index: 0 }],
    ["message_delta", { type: "message_delta", delta: { stop_reason: "end_turn", stop_sequence: null }, usage: { output_tokens: 4 } }],
    ["message_stop", { type: "message_stop" }],
  ]);
}

function call(input, name, value) {
  return sse([
    start(input),
    ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "tool_use", id: `toolu_measure_${modelCalls}`, name, input: {} } }],
    ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "input_json_delta", partial_json: JSON.stringify(value) } }],
    ["content_block_stop", { type: "content_block_stop", index: 0 }],
    ["message_delta", { type: "message_delta", delta: { stop_reason: "tool_use", stop_sequence: null }, usage: { output_tokens: 4 } }],
    ["message_stop", { type: "message_stop" }],
  ]);
}

const exec = (input, code) => call(input, "exec", { code });
const blockText = block => typeof block.content === "string" ? block.content : (block.content ?? []).map(part => part.text ?? "").join("");

function prompt(message) {
  if (message.role !== "user") return undefined;
  if (typeof message.content === "string") return message.content;
  if (message.content.some(block => block.type === "tool_result")) return undefined;
  return message.content.filter(block => block.type === "text").map(block => block.text).join("");
}

function turn(input) {
  let index = input.messages.length - 1;
  while (index >= 0 && prompt(input.messages[index]) === undefined) index--;
  const results = input.messages.slice(index + 1).flatMap(message => message.role === "user" && Array.isArray(message.content) ? message.content.filter(block => block.type === "tool_result") : []);
  const scenario = /remote-[a-z-]+/.exec(prompt(input.messages[index]) ?? "")?.[0];
  if (!scenario) throw new Error("Measurement prompt did not reach the provider");
  (observed[scenario] ??= []).splice(0, Infinity, ...results.map(block => ({ error: !!block.is_error, text: blockText(block) })));
  return { scenario, step: results.length, last: results.length ? blockText(results.at(-1)) : "" };
}

const scripts = {
  "remote-workspace": [
    () => `const patch = await tools.apply_patch("*** Begin Patch\\n*** Add File: notes/remote.txt\\n+written by remote code mode\\n*** End Patch\\n");
const bash = await tools.exec_command({ cmd: "cat notes/remote.txt && echo bash-$((6*7)) > notes/bash.txt && (printenv NANOCODE_RUNNER_TOKEN >/dev/null && echo credential-visible || echo credential-hidden)" });
const agents = await tools.list_agents({});
store("marker", "persisted-across-exec");
text(JSON.stringify({ patch, bash, agents }));`,
    () => `// @exec: {"yield_time_ms": 100}
text(load("marker"));
yield_control();
const slow = await tools.exec_command({ cmd: "sleep 1; cat notes/bash.txt; printf wait-done > notes/wait.txt" });
store("after_wait", "stored-after-wait");
text(slow);`,
    last => {
      const cell = /cell ID (\w+)/.exec(last)?.[1];
      if (!cell) throw new Error(`Exec did not yield a running cell: ${last}`);
      return ["wait", { cell_id: cell, yield_time_ms: 10000 }];
    },
    () => `text(load("marker") + " " + load("after_wait"));`,
  ],
  "remote-detached": [
    () => `// @exec: {"yield_time_ms": 100}
yield_control();
text(await tools.exec_command({ cmd: "printf started > notes/detached-started.txt; sleep 3; printf done > notes/detached-finished.txt", yield_time_ms: 10000 }));`,
  ],
  "remote-cancel": [
    () => `// @exec: {"yield_time_ms": 20000}
await tools.exec_command({ cmd: "printf started > notes/cancel-started.txt; sleep 3; printf late > notes/cancel-late.txt", yield_time_ms: 10000 });
text("cancel target finished");`,
  ],
  "remote-disconnect": [
    () => `// @exec: {"yield_time_ms": 20000}
await tools.exec_command({ cmd: "printf started > notes/disconnect-started.txt; sleep 2; printf finished > notes/disconnect-finished.txt", yield_time_ms: 10000 });
text("disconnect target finished");`,
  ],
  "remote-reconnect": [
    () => `text(await tools.exec_command({ cmd: "cat notes/remote.txt" }));`,
  ],
  "remote-restart": [
    () => `// @exec: {"yield_time_ms": 20000}
await tools.exec_command({ cmd: "printf x >> notes/restart.log; sleep 2; printf done > notes/restart-finished.txt", yield_time_ms: 10000 });
text("restart target finished");`,
  ],
  "remote-latency": Array.from({ length: latencySteps }, (_, index) => () => `text(${index});`),
};

async function model(input) {
  const { scenario, step, last } = turn(input);
  if (scenario === "remote-latency" && step > 0) gaps.push(performance.now() - lastResponseAt);
  const steps = scripts[scenario];
  if (step >= steps.length) return reply(input, `${scenario}-done`);
  const next = steps[step](last);
  return Array.isArray(next) ? call(input, ...next) : exec(input, next);
}

const outbound = async request => {
  const url = new URL(request.url);
  if (url.host === `127.0.0.1:${port}`) {
    if (request.headers.get("authorization") !== `Bearer ${token}` || url.search) throw new Error("Runner request escaped egress policy");
    runnerConnections++;
    return upgrade(`http://${url.host}${url.pathname}`, { headers: { upgrade: "websocket", authorization: request.headers.get("authorization") } });
  }
  if (url.hostname !== "provider.invalid" || request.headers.get("x-api-key") !== "local-provider") throw new Error("Provider request escaped configured credential policy");
  const input = await request.json();
  if (input.model !== "claude-opus-5-5" || JSON.stringify(input).includes("local-provider") || JSON.stringify(input).includes(token)) throw new Error("Invalid model or credentials entered model input");
  modelCalls++;
  try {
    return await model(input);
  } catch (error) {
    console.error(`Mock provider rejected model call ${modelCalls}: ${error.message}`);
    throw error;
  }
};

const runnerBuildMs = buildRunner();
await startRunner();
const options = localOptions({
  persistence,
  agent: { RUNNER_ENABLED: "true", REMOTE_MAX_FRAME_BYTES: String(policy.max_frame_bytes), REMOTE_TIMEOUT_MS: String(policy.timeout_ms) },
  egress: { RUNNER_URL: `ws://127.0.0.1:${port}`, RUNNER_TOKEN: token },
});
options.workers[1].outboundService = outbound;
let mf = new Miniflare(convertV4MiniflareOptions(options));
const name = `remote-${Date.now()}`;
const sessionName = name;
const base = `http://worker/sessions/${name}/turns`;
const headers = { authorization: "Bearer local-measurement", "content-type": "application/json" };
const request = (url, init = {}) => mf.dispatchFetch(url, { ...init, headers });
const storage = () => mf.unsafeGetDurableObjectStorage("nanocode-worker", "AgentSession", { name: sessionName });
const file = path => resolve(workspace, path);
const read = path => existsSync(file(path)) ? readFileSync(file(path), "utf8") : null;
const measurements = [];

async function until(condition, detail, timeout = policy.timeout_ms * 2) {
  const begin = performance.now();
  while (!await condition()) {
    if (performance.now() - begin > timeout) throw new Error(typeof detail === "function" ? detail() : detail);
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}

async function submit(id, scenario) {
  const response = await request(base, { method: "POST", body: JSON.stringify({ request_id: id, prompt: `Run ${scenario}` }) });
  if (![200, 202].includes(response.status)) throw new Error(`Submit rejected: ${await response.text()}`);
  return response.status;
}

async function complete(id, begin, expected = "completed") {
  let result;
  await until(async () => {
    const response = await request(`${base}/${encodeURIComponent(id)}`);
    if (response.status !== 200) throw new Error(`Status rejected: ${await response.text()}`);
    result = await response.json();
    return ["failed", "completed", "cancelled"].includes(result.status);
  }, () => `Turn ${id} stalled: ${JSON.stringify(result)} ${JSON.stringify(observed)}`);
  measurements.push({ id, status: result.status, milliseconds: performance.now() - begin, error: result.error });
  if (expected && result.status !== expected) throw new Error(`Unexpected ${id} result: ${JSON.stringify(result)}`);
  return result;
}

async function started(path, id) {
  await until(async () => {
    if (read(path) !== null) return true;
    const state = await (await request(`${base}/${encodeURIComponent(id)}`)).json();
    if (["failed", "completed", "cancelled"].includes(state.status)) throw new Error(`Turn ${id} ended before its remote command started: ${JSON.stringify(state)} ${JSON.stringify(observed)}`);
    return false;
  }, `Turn ${id} did not start its remote command`);
}

async function run(id, scenario, expected) {
  const begin = performance.now();
  await submit(id, scenario);
  return complete(id, begin, expected);
}

try {
  await mf.ready;
  const bindings = await mf.getBindings("nanocode-worker");
  for (const key of ["RUNNER_TOKEN", "RUNNER_URL", "ANTHROPIC_API_KEY", "OAUTH_JSON"]) if (key in bindings) throw new Error("Runner or provider secret reached the public Worker");

  const workspaceTurn = await run("workspace", "remote-workspace");
  const workspaceResults = observed["remote-workspace"];
  const files = { remote: read("notes/remote.txt"), bash: read("notes/bash.txt"), wait: read("notes/wait.txt") };
  if (files.remote !== "written by remote code mode\n" || files.bash !== "bash-42\n" || files.wait !== "wait-done") throw new Error(`Remote workspace mismatch: ${JSON.stringify(files)}`);
  if (!workspaceResults[0].text.includes("credential-hidden")) throw new Error("Runner credential was visible to remote Bash");
  if (!workspaceResults[0].text.includes("agents")) throw new Error("Brain callback result missing");
  if (!/Script running with cell ID/.test(workspaceResults[1].text)) throw new Error("Exec did not yield");
  if (!workspaceResults[2].text.includes("bash-42")) throw new Error("Wait did not resume the remote cell");
  if (!workspaceResults[3].text.includes("persisted-across-exec stored-after-wait")) throw new Error("Code Mode store/load did not persist across exec and wait");

  const latencyStart = performance.now();
  await run("latency", "remote-latency");
  const latencyMs = performance.now() - latencyStart;

  const callsBeforeReplay = modelCalls;
  const connectionsBeforeReplay = runnerConnections;
  const replayStart = performance.now();
  if (await submit("workspace", "remote-workspace") !== 200) throw new Error("Completed remote turn was not replayed from its receipt");
  measurements.push({ id: "workspace-replay", milliseconds: performance.now() - replayStart });
  if (modelCalls !== callsBeforeReplay || runnerConnections !== connectionsBeforeReplay) throw new Error("Receipt replay dispatched model or runner work");

  const detachedTurn = await run("detached", "remote-detached");
  if (!observed["remote-detached"][0]?.text.includes("Script running with cell ID")) throw new Error("Detached cell did not yield");
  await started("notes/detached-started.txt", "detached");
  const detachedStorage = await storage();
  const markerCount = async () => Number((await detachedStorage.exec("SELECT COUNT(*) AS count FROM metadata WHERE key LIKE 'background:%'"))[0].count);
  if (read("notes/detached-finished.txt") !== null || await markerCount() !== 1) throw new Error("Background alarm cleared before detached work completed");
  await until(() => read("notes/detached-finished.txt") === "done", "Detached workspace command did not finish");
  await until(async () => await markerCount() === 0, "Detached work never became quiescent");
  const detached = { status: detachedTurn.status, marker_retained_until_completion: true, marker_cleared_after_completion: true };

  const cancelStart = performance.now();
  await submit("cancel", "remote-cancel");
  await started("notes/cancel-started.txt", "cancel");
  await request(`${base}/cancel/cancel`, { method: "POST" });
  const cancelled = await complete("cancel", cancelStart, null);
  await new Promise(resolve => setTimeout(resolve, 3500));
  const cancellation = { status: cancelled.status, late_side_effect: read("notes/cancel-late.txt") !== null };
  if (cancellation.status !== "cancelled" || cancellation.late_side_effect) throw new Error("Remote cancellation did not stop the command");

  const restartCalls = modelCalls;
  await submit("restart", "remote-restart");
  await started("notes/restart.log", "restart");
  const restartAt = performance.now();
  const interrupted = runner;
  await mf.dispose();
  await interrupted.exited;
  await runnerReady;
  mf = new Miniflare(convertV4MiniflareOptions(options));
  await mf.ready;
  const restarted = await complete("restart", restartAt, null);
  await new Promise(resolve => setTimeout(resolve, 2500));
  const restart = { status: restarted.status, error: restarted.error, model_calls: modelCalls - restartCalls, dispatches: (read("notes/restart.log") ?? "").length, tool_results: [...(observed["remote-restart"] ?? [])] };
  if (restart.dispatches !== 1) throw new Error(`Interrupted remote exec was replayed ${restart.dispatches} times`);

  const afterRestartStart = performance.now();
  await submit("after-restart", "remote-reconnect");
  const afterRestart = await complete("after-restart", afterRestartStart, null);

  const disconnectStart = performance.now();
  await submit("disconnect", "remote-disconnect");
  await started("notes/disconnect-started.txt", "disconnect");
  supervising = false;
  runner.child.kill("SIGKILL");
  await runner.exited;
  const disconnected = await complete("disconnect", disconnectStart, null);
  await new Promise(resolve => setTimeout(resolve, 2500));
  const disconnect = { status: disconnected.status, error: disconnected.error, tool_results: [...(observed["remote-disconnect"] ?? [])], finished_after_kill: read("notes/disconnect-finished.txt") !== null };

  supervising = true;
  await startRunner();
  const reconnectStart = performance.now();
  await submit("reconnect", "remote-reconnect");
  const reconnected = await complete("reconnect", reconnectStart, null);
  const reconnect = { status: reconnected.status, error: reconnected.error, tool_results: [...(observed["remote-reconnect"] ?? [])] };

  const durable = await storage();
  const persisted = (await durable.exec("SELECT (SELECT COUNT(*) FROM states) AS states, (SELECT COUNT(*) FROM records) AS records, (SELECT COUNT(*) FROM events) AS events, (SELECT COUNT(*) FROM jobs WHERE status = 'completed') AS completed, (SELECT COUNT(*) FROM jobs WHERE status IN ('queued', 'running')) AS unfinished"))[0];
  const saved = JSON.stringify(await durable.exec("SELECT payload FROM states UNION ALL SELECT value AS payload FROM records UNION ALL SELECT body AS payload FROM events UNION ALL SELECT result AS payload FROM jobs"));
  if (saved.includes(token) || saved.includes("local-provider")) throw new Error("Credential appeared in agent persistence");
  const receipts = { exec_command: saved.split("exec_command").length - 1, apply_patch: saved.split("apply_patch").length - 1, list_agents: saved.split("list_agents").length - 1 };
  if (!receipts.exec_command || !receipts.apply_patch || !receipts.list_agents) throw new Error(`Nested remote receipts were not persisted: ${JSON.stringify(receipts)}`);

  const sorted = [...gaps].sort((a, b) => a - b);
  const report = {
    runtime: "local workerd (Miniflare) public + egress Workers, SDK runner example over loopback WebSocket",
    note: "Local transport only; not production colo latency. Egress-to-runner traffic passes through Miniflare's Node outbound coupling in this harness.",
    policy,
    runner_build_ms: runnerBuildMs,
    workspace_turn: { status: workspaceTurn.status, files, tool_results: workspaceResults.map(result => ({ error: result.error, bytes: result.text.length })) },
    latency: { exec_steps: latencySteps, turn_ms: latencyMs, model_to_model_gap_ms: { median: sorted[Math.floor(sorted.length / 2)], min: sorted[0], max: sorted.at(-1) } },
    detached,
    cancellation,
    disconnect,
    reconnect,
    restart,
    after_restart: { status: afterRestart.status, error: afterRestart.error },
    measurements,
    model_calls: modelCalls,
    runner_connections: runnerConnections,
    runner_starts: runnerStarts,
    receipts,
    persisted,
  };
  if ([restarted, afterRestart, disconnected, reconnected].some(result => result.status !== "completed")) throw new Error("A recovered turn did not complete");
  if (!restart.tool_results.some(result => result.error && /outcome unknown/.test(result.text))) throw new Error("Interrupted dispatch did not retain its uncertain receipt");
  if (!disconnect.tool_results.some(result => result.error && /unknown outcome/.test(result.text))) throw new Error("Disconnect did not report an uncertain outcome");
  if (persisted.unfinished !== 0) throw new Error("Remote queue retained unfinished jobs");
  if (runnerFailure) throw runnerFailure;
  writeFileSync("output/remote-measurement.json", JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
} finally {
  supervising = false;
  await mf.dispose();
  stopRunners();
}

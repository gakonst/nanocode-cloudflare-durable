import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { spawn, spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { localOptions } from "./local-runtime.mjs";

const sdk = resolve(process.env.NANOCODE_SDK_PATH ?? "../nanocodex");
const binary = process.env.HANDS_BINARY ?? resolve(process.env.CARGO_TARGET_DIR ?? resolve(sdk, "target"), "debug/nanocode");
const policy = { max_frame_bytes: Number(process.env.FAULTS_MAX_FRAME_BYTES ?? "1048576"), timeout_ms: Number(process.env.FAULTS_TIMEOUT_MS ?? "5000") };
const token = "local-measurement";
const persistence = resolve(mkdtempSync("output/hand-faults-state-"));
const groups = new Set();
const observed = {};
const checks = [];
let modelCalls = 0;

const stopHands = () => {
  for (const group of groups) {
    try { process.kill(-group, "SIGKILL"); } catch (error) { if (!["ESRCH", "EPERM"].includes(error.code)) throw error; }
  }
  groups.clear();
};
process.on("exit", stopHands);
for (const signal of ["SIGINT", "SIGTERM"]) process.on(signal, () => { stopHands(); process.exit(130); });

const sse = events => new Response(events.map(([event, data]) => "event: " + event + "\ndata: " + JSON.stringify(data) + "\n\n").join(""), { headers: { "content-type": "text/event-stream" } });
const start = input => ["message_start", { type: "message_start", message: { id: "message-" + modelCalls, type: "message", role: "assistant", model: input.model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 8, output_tokens: 0 } } }];
const reply = (input, text) => sse([
  start(input),
  ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }],
  ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text } }],
  ["content_block_stop", { type: "content_block_stop", index: 0 }],
  ["message_delta", { type: "message_delta", delta: { stop_reason: "end_turn", stop_sequence: null }, usage: { output_tokens: 4 } }],
  ["message_stop", { type: "message_stop" }],
]);
const call = (input, name, value) => sse([
  start(input),
  ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "tool_use", id: "toolu_faults_" + modelCalls, name, input: {} } }],
  ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "input_json_delta", partial_json: JSON.stringify(value) } }],
  ["content_block_stop", { type: "content_block_stop", index: 0 }],
  ["message_delta", { type: "message_delta", delta: { stop_reason: "tool_use", stop_sequence: null }, usage: { output_tokens: 4 } }],
  ["message_stop", { type: "message_stop" }],
]);

const blockText = block => typeof block.content === "string" ? block.content : (block.content ?? []).map(part => part.text ?? "[" + part.type + "]").join("");

function prompt(message) {
  if (message.role !== "user") return undefined;
  if (typeof message.content === "string") return message.content;
  if (message.content.some(block => block.type === "tool_result")) return undefined;
  return message.content.filter(block => block.type === "text").map(block => block.text).join("");
}

const cellId = text => /cell ID (\S+)/.exec(text ?? "")?.[1];
const markers = { exec: 3101, bash: 3102, child: 3103, grandchild: 3104 };
const PNG = [
  "import os, struct, zlib",
  "w = h = 1100",
  "raw = b''.join(b'\\x00' + os.urandom(w * 3) for _ in range(h))",
  "chunk = lambda kind, data: struct.pack('>I', len(data)) + kind + data + struct.pack('>I', zlib.crc32(kind + data) & 0xffffffff)",
  "open('big.png', 'wb').write(b'\\x89PNG\\r\\n\\x1a\\n' + chunk(b'IHDR', struct.pack('>IIBBBBB', w, h, 8, 2, 0, 0, 0)) + chunk(b'IDAT', zlib.compress(raw, 1)) + chunk(b'IEND', b''))",
].join("\n");

const gates = {};
const gate = name => { let open; const promise = new Promise(resolve => { open = resolve; }); gates[name] = { promise, open }; return promise; };
const scripts = {
  "faults-ping": [() => ({ code: 'text("pong");' })],
  "faults-where": [() => ({ code: 'text((await tools.exec_command({ cmd: "pwd" })).output);' })],
  "faults-reconnect": [() => ({ code: '// @exec: {"yield_time_ms": 1000}' + "\n" + 'await tools.exec_command({ cmd: "sleep 3105", yield_time_ms: 600000 }); text("finished");' }), results => ({ wait: { cell_id: cellId(results[0]?.text), yield_time_ms: 20000 } }), async results => { await gates.reattached.promise; return { wait: { cell_id: cellId(results[0]?.text), yield_time_ms: 1000 } }; }],
  "faults-text": [() => ({ code: 'text("x".repeat(1500000));' }), () => ({ code: 'text("after");' })],
  "faults-image": [() => ({ code: "await tools.Write({ file_path: 'big.py', content: " + JSON.stringify(PNG) + " }); await tools.exec_command({ cmd: 'python3 big.py' }); const file = await tools.Read({ file_path: 'big.png' }); text('read ' + JSON.stringify(file).length);" }), () => ({ code: 'text("after");' })],
  "faults-copy": [() => ({ code: 'const written = await tools.Write({ file_path: "copy.txt", content: "y".repeat(1500000), hand: "beta" }); text("copied " + JSON.stringify(written).slice(0, 200));' }), () => ({ code: 'text("after");' })],
  "faults-displace": [() => ({ code: '// @exec: {"yield_time_ms": 1000}' + "\n" + 'await tools.exec_command({ cmd: "sleep 3106", yield_time_ms: 600000 }); text("finished");' }), results => ({ wait: { cell_id: cellId(results[0]?.text), yield_time_ms: 20000 } })],
  "faults-catch": [() => ({ code: 'try { await tools.Write({ file_path: "caught.txt", content: "z".repeat(1500000), hand: "beta" }); text("written"); } catch (error) { const message = String(error?.message ?? error); text(JSON.stringify({ length: message.length, head: message.slice(0, 300) })); }' })],
  "faults-missing": [() => ({ code: 'const settled = await Promise.allSettled(["nowhere", "alpha"].map(hand => tools.exec_command({ cmd: "pwd", hand }))); text(JSON.stringify(settled.map(entry => entry.status === "fulfilled" ? { ok: entry.value.output } : { error: String(entry.reason) })));' })],
  "faults-yield": [() => ({ code: '// @exec: {"yield_time_ms": ' + (policy.timeout_ms + 4000) + '}' + "\n" + 'await tools.exec_command({ cmd: "sleep ' + (Math.ceil(policy.timeout_ms / 1000) + 1) + '", yield_time_ms: ' + (policy.timeout_ms + 3000) + ' }); text("slept");' })],
  "faults-cancel": [() => ({ code: '// @exec: {"yield_time_ms": 1500}' + "\n" + 'await Promise.all([tools.exec_command({ cmd: "sleep ' + markers.exec + '", yield_time_ms: 600000 }), tools.Bash({ command: "sh -c ' + "'sleep " + markers.child + " & sleep " + markers.grandchild + "'" + '; sleep ' + markers.bash + '", timeout: 600000 })]); text("finished");' }), results => ({ wait: { cell_id: cellId(results[0]?.text), yield_time_ms: 120000 } })],
};

async function model(input) {
  let index = input.messages.length - 1;
  while (index >= 0 && prompt(input.messages[index]) === undefined) index--;
  const results = input.messages.slice(index + 1).flatMap(message => message.role === "user" && Array.isArray(message.content) ? message.content.filter(block => block.type === "tool_result") : []);
  const request = prompt(input.messages[index]) ?? "";
  const scenario = /faults-[a-z]+/.exec(request)?.[0];
  const key = /\[([^ \]]+)\]/.exec(request)?.[1];
  if (!scenario || !key) throw new Error("Measurement prompt did not reach the provider");
  observed[key] = results.map(block => ({ error: !!block.is_error, text: blockText(block) }));
  const steps = scripts[scenario];
  if (results.length >= steps.length) return reply(input, scenario + "-done");
  const step = await steps[results.length](observed[key]);
  return step.wait ? call(input, "wait", step.wait) : call(input, "exec", step);
}

const outbound = async request => {
  const url = new URL(request.url);
  if (url.hostname !== "provider.invalid" || request.headers.get("x-api-key") !== "local-provider") throw new Error("Provider request escaped configured credential policy");
  modelCalls++;
  return model(await request.json());
};

if (!process.env.HANDS_SKIP_BUILD) {
  const result = spawnSync("cargo", ["build", "--locked", "-p", "nanocode-cli", "--bin", "nanocode"], { cwd: sdk, stdio: "inherit" });
  if (result.status !== 0) throw new Error("nanocode build failed");
}
const options = localOptions({ persistence, agent: { RECOVERY_DELAY_MS: "1000", REMOTE_MAX_FRAME_BYTES: String(policy.max_frame_bytes), REMOTE_TIMEOUT_MS: String(policy.timeout_ms) } });
options.workers[1].outboundService = outbound;
const mf = new Miniflare(convertV4MiniflareOptions(options));
const name = "faults-" + Date.now();
const headers = { authorization: "Bearer " + token, "content-type": "application/json" };
const request = (path, init = {}) => mf.dispatchFetch("http://worker/sessions/" + name + path, { ...init, headers });
const workspaces = {};
const logs = {};
const hands = {};

async function until(condition, detail, timeout = 60000) {
  const begin = performance.now();
  while (!await condition()) {
    if (performance.now() - begin > timeout) throw new Error(typeof detail === "function" ? detail() : detail);
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  return performance.now() - begin;
}

const attached = async () => (await (await request("/hands")).json()).hands;
const connects = key => (logs[key].match(/hand: connected/g) ?? []).length;

function startHand(key, hand, base) {
  workspaces[key] ??= resolve(mkdtempSync("output/hand-faults-" + key + "-"));
  logs[key] = "";
  const child = spawn(binary, ["hand", base.replace("http", "ws") + "sessions/" + name + "/hands", "--name", hand, "-C", workspaces[key]], { env: { ...process.env, NANOCODE_RUNNER_TOKEN: token }, stdio: ["ignore", "ignore", "pipe"], detached: true });
  groups.add(child.pid);
  child.stderr.on("data", chunk => { logs[key] += chunk; });
  hands[key] = { child, done: false, exited: new Promise(resolve => child.once("exit", () => { groups.delete(child.pid); hands[key].done = true; resolve(); })) };
}

async function stopHand(key) {
  try { process.kill(-hands[key].child.pid, "SIGKILL"); } catch (error) { if (error.code !== "ESRCH") throw error; }
  await hands[key].exited;
}

async function submit(id, scenario, hand) {
  const response = await request("/turns", { method: "POST", body: JSON.stringify({ request_id: id, prompt: "Run " + scenario + " [" + id + "]", hand }) });
  if (![200, 202].includes(response.status)) throw new Error("Submit rejected: " + await response.text());
}

async function finished(id, timeout = 120000) {
  let result;
  await until(async () => {
    result = await (await request("/turns/" + encodeURIComponent(id))).json();
    return ["failed", "completed", "cancelled"].includes(result.status);
  }, () => "Turn " + id + " stalled: " + JSON.stringify(result), timeout);
  return result;
}

async function turn(id, scenario, hand) {
  const begin = performance.now();
  await submit(id, scenario, hand);
  const result = await finished(id);
  if (result.status !== "completed") console.log("turn", id, JSON.stringify(result).slice(0, 1500));
  return { status: result.status, ms: Math.round(performance.now() - begin), results: observed[id] ?? [] };
}

const check = (name, passed, detail) => { checks.push({ name, passed: !!passed, detail }); console.log((passed ? "PASS " : "FAIL ") + name + (passed ? "" : ": " + JSON.stringify(detail).slice(0, 2000))); };
const brief = results => results.map(result => ({ error: result.error, text: result.text.length > 400 ? result.text.slice(0, 400) + "... (" + result.text.length + " chars)" : result.text }));

function processes(list) {
  const listing = spawnSync("ps", ["-axo", "pid=,command="], { encoding: "utf8" }).stdout;
  return listing.split("\n").filter(line => list.some(marker => line.trim().split(/ +/).slice(1).join(" ").split(/[;&' ]+/).includes(String(marker))) && !line.includes(" ps ")).map(line => line.trim());
}

const report = {};
try {
  const base = String(await mf.ready);
  startHand("alpha", "alpha", base);
  startHand("beta", "beta", base);
  await until(async () => (await attached()).length === 2, () => "Hands did not attach: " + JSON.stringify(logs));
  const scenarios = (process.env.FAULTS ?? "text,image,copy,missing,yield,cancel,reconnect,duplicate").split(",");

  for (const scenario of ["text", "image", "copy"]) {
    if (!scenarios.includes(scenario)) continue;
    const before = { alpha: connects("alpha"), beta: connects("beta") };
    const result = await turn("large-" + scenario, "faults-" + scenario, "alpha");
    await new Promise(resolve => setTimeout(resolve, 500));
    const after = { alpha: connects("alpha"), beta: connects("beta") };
    report[scenario] = { ...result, results: brief(result.results), reconnects: { alpha: after.alpha - before.alpha, beta: after.beta - before.beta } };
    check(scenario + ": an oversized frame fails only its own operation and the hands stay connected", result.status === "completed" && after.alpha === before.alpha && after.beta === before.beta && result.results[1]?.text.includes("after"), report[scenario]);
    if (scenario === "image") check("image: a script whose nested calls exceed the frame limit still delivers its own output", /read [0-9]+/.test(result.results[0]?.text ?? ""), report[scenario].results);
    else check(scenario + ": the failure names the frame limit", /max_frame_bytes/.test(result.results[0]?.text ?? ""), report[scenario].results);
    check(scenario + ": the failure is not reported as an unknown outcome", !/unknown outcome/.test(result.results[0]?.text ?? ""), report[scenario].results);
  }

  if (scenarios.includes("catch")) {
    const result = await turn("catch", "faults-catch", "alpha");
    report.catch = { ...result, results: brief(result.results) };
    const text = result.results[0]?.text ?? "";
    check("catch: a script catches a cross-hand call refused for its size and continues", result.status === "completed" && !result.results[0]?.error && /max_frame_bytes/.test(text) && /not dispatched/.test(text), report.catch.results);
  }

  if (scenarios.includes("missing")) {
    const result = await turn("missing", "faults-missing", "alpha");
    report.missing = { ...result, results: brief(result.results) };
    const text = result.results[0]?.text ?? "";
    check("missing: a call naming an absent hand fails without dispatching and the other call runs", /hand .nowhere. is not attached.*nothing was dispatched/.test(text) && text.includes(workspaces.alpha), report.missing.results);
  }

  if (scenarios.includes("yield")) {
    const result = await turn("yield", "faults-yield", "alpha");
    report.yield = { ...result, results: brief(result.results), timeout_ms: policy.timeout_ms };
    check("yield: a cell whose requested yield exceeds REMOTE_TIMEOUT_MS completes instead of hitting the operation deadline", /slept/.test(result.results[0]?.text ?? "") && !/deadline/.test(result.results[0]?.text ?? ""), report.yield.results);
  }

  if (scenarios.includes("cancel")) {
    const list = Object.values(markers);
    await submit("cancel", "faults-cancel", "alpha");
    await until(async () => processes(list).length >= 4 && (observed.cancel?.length ?? 0) >= 1, () => "cell processes did not start: " + JSON.stringify({ processes: processes(list), observed: observed.cancel, logs }));
    await new Promise(resolve => setTimeout(resolve, 1000));
    const running = processes(list);
    const cancelStarted = performance.now();
    const cancelled = await (await request("/turns/cancel/cancel", { method: "POST" })).json();
    const settled = await finished("cancel");
    let remaining = processes(list);
    const deadline = performance.now() + 5000;
    while (remaining.length && performance.now() < deadline) { await new Promise(resolve => setTimeout(resolve, 50)); remaining = processes(list); }
    report.cancel = { cancel: cancelled.status, status: settled.status, settled_ms: Math.round(performance.now() - cancelStarted), running, remaining, results: brief(observed.cancel ?? []) };
    check("cancel: cancelling a turn with a running cell stops the cell's processes and their children", settled.status === "cancelled" && running.length >= 4 && remaining.length === 0, report.cancel);
    for (const line of remaining) { try { process.kill(Number(line.split(" ")[0]), "SIGKILL"); } catch {} }
  }

  if (scenarios.includes("reconnect")) {
    gate("reattached");
    await submit("reconnect", "faults-reconnect", "alpha");
    await until(async () => (observed.reconnect?.length ?? 0) >= 1, () => "reconnect cell did not yield: " + JSON.stringify(observed.reconnect));
    await new Promise(resolve => setTimeout(resolve, 1000));
    await stopHand("alpha");
    await until(async () => (observed.reconnect?.length ?? 0) >= 2, () => "wait on the departed hand did not return: " + JSON.stringify(observed.reconnect));
    startHand("alpha", "alpha", base);
    await until(async () => (await attached()).includes("alpha") && connects("alpha") >= 1, () => "alpha did not reattach: " + logs.alpha);
    gates.reattached.open();
    const settled = await finished("reconnect");
    const after = await turn("reconnect-after", "faults-ping", "alpha");
    const orphans = processes([3105]);
    report.reconnect = { status: settled.status, results: brief(observed.reconnect ?? []), after: after.status, orphans };
    check("reconnect: a wait in flight when its hand dies reports an unknown outcome", /unknown outcome/.test(observed.reconnect?.[1]?.text ?? ""), report.reconnect);
    check("reconnect: waiting on the cell after the hand reattached reports it gone instead of hanging", settled.status === "completed" && (observed.reconnect?.[2]?.error ?? false), report.reconnect);
    check("reconnect: the reattached hand runs the next turn", after.status === "completed" && after.results[0]?.text.includes("pong"), report.reconnect);
    for (const line of orphans) { try { process.kill(Number(line.split(" ")[0]), "SIGKILL"); } catch {} }
  }

  if (scenarios.includes("duplicate")) {
    const before = connects("alpha");
    await submit("displace", "faults-displace", "alpha");
    await until(async () => (observed.displace?.length ?? 0) >= 1 && processes([3106]).length === 1, () => "displaced cell did not start: " + JSON.stringify(observed.displace));
    await new Promise(resolve => setTimeout(resolve, 500));
    startHand("impostor", "alpha", base);
    const displaced = await finished("displace");
    let leftover = processes([3106]);
    const deadline = performance.now() + 5000;
    while (leftover.length && performance.now() < deadline) { await new Promise(resolve => setTimeout(resolve, 50)); leftover = processes([3106]); }
    report.displaced = { status: displaced.status, results: brief(observed.displace ?? []), leftover };
    check("duplicate: the displaced hand's running cell reports an unknown outcome and its processes stop", displaced.status === "completed" && /unknown outcome/.test(observed.displace?.[1]?.text ?? "") && leftover.length === 0, report.displaced);
    for (const line of leftover) { try { process.kill(Number(line.split(" ")[0]), "SIGKILL"); } catch {} }
    await new Promise(resolve => setTimeout(resolve, 8000));
    const result = await turn("duplicate", "faults-where", "alpha");
    const flaps = { alpha: connects("alpha") - before, impostor: connects("impostor") };
    report.duplicate = { flaps, status: result.status, results: brief(result.results), alpha_log: logs.alpha.split("\n").slice(-4), impostor_log: logs.impostor.split("\n").slice(-4), alpha_stopped: hands.alpha.done };
    check("duplicate: two hands claiming one name do not displace each other in a loop", flaps.alpha === 0 && flaps.impostor === 1, report.duplicate);
    check("duplicate: the displaced hand is told why and stops", /another hand attached under this name; stopping/.test(logs.alpha) && hands.alpha.done, report.duplicate);
    check("duplicate: the name runs on the newest hand", result.status === "completed" && result.results[0]?.text.includes(workspaces.impostor), report.duplicate);
    await stopHand("impostor");
  }

  const passed = checks.filter(entry => entry.passed).length;
  const output = { runtime: "local workerd (Miniflare) with nanocode hand processes over loopback", binary, policy, model_calls: modelCalls, passed, failed: checks.length - passed, checks, report, hand_logs: Object.fromEntries(Object.entries(logs).map(([key, value]) => [key, value.split("\n").slice(-40)])) };
  mkdirSync("output", { recursive: true });
  writeFileSync(process.env.FAULTS_REPORT ?? "output/hand-faults.json", JSON.stringify(output, null, 2));
  console.log(JSON.stringify({ passed, failed: checks.length - passed }));
  if (passed !== checks.length) process.exitCode = 1;
} catch (error) {
  console.error(error);
  console.error(JSON.stringify({ logs, observed: Object.fromEntries(Object.entries(observed).map(([key, value]) => [key, brief(value)])) }, null, 2).slice(0, 20000));
  process.exitCode = 1;
} finally {
  await mf.dispose();
  stopHands();
}

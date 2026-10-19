import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { spawn, spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { localOptions } from "./local-runtime.mjs";

const sdk = resolve(process.env.NANOCODE_SDK_PATH ?? "../nanocodex");
const binary = resolve(process.env.CARGO_TARGET_DIR ?? resolve(sdk, "target"), "debug/nanocodex");
const policy = { max_frame_bytes: 1048576, timeout_ms: 30000 };
const keepaliveMs = Number(process.env.HANDS_KEEPALIVE_MS ?? "45000");
const graceMs = 2000;
const latencyTurns = 10;
const token = "local-measurement";
const persistence = resolve(mkdtempSync("output/hands-state-"));
const groups = new Set();
const observed = {};
let modelCalls = 0;

const stopHands = () => {
  for (const group of groups) {
    try { process.kill(-group, "SIGKILL"); } catch (error) { if (!["ESRCH", "EPERM"].includes(error.code)) throw error; }
  }
  groups.clear();
};
process.on("exit", stopHands);
for (const signal of ["SIGINT", "SIGTERM"]) process.on(signal, () => { stopHands(); process.exit(130); });

function buildHand() {
  const start = performance.now();
  const result = spawnSync("cargo", ["build", "--locked", "-p", "nanocodex", "--bin", "nanocodex"], { cwd: sdk, stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error("nanocodex build failed");
  return performance.now() - start;
}

function sse(events) {
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

function exec(input, code) {
  return sse([
    start(input),
    ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "tool_use", id: `toolu_hands_${modelCalls}`, name: "exec", input: {} } }],
    ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "input_json_delta", partial_json: JSON.stringify({ code }) } }],
    ["content_block_stop", { type: "content_block_stop", index: 0 }],
    ["message_delta", { type: "message_delta", delta: { stop_reason: "tool_use", stop_sequence: null }, usage: { output_tokens: 4 } }],
    ["message_stop", { type: "message_stop" }],
  ]);
}

const blockText = block => typeof block.content === "string" ? block.content : (block.content ?? []).map(part => part.text ?? "").join("");

function prompt(message) {
  if (message.role !== "user") return undefined;
  if (typeof message.content === "string") return message.content;
  if (message.content.some(block => block.type === "tool_result")) return undefined;
  return message.content.filter(block => block.type === "text").map(block => block.text).join("");
}

const scripts = {
  "hands-where": [() => `text(JSON.stringify(await tools.exec_command({ cmd: "pwd" })));`],
  "hands-callback": [() => `text(JSON.stringify({ agents: await tools.list_agents({}) }));`],
  "hands-store": [() => `store("marker", "kept-on-hand"); text("stored");`, () => `text(load("marker"));`],
  "hands-remember": [() => `store("thread", "kept-on-hand"); text("stored");`],
  "hands-recall": [() => `text(String(load("thread")));`],
  "hands-together": [() => `const started = Date.now(); const where = await tools.exec_command({ cmd: "sleep 1; pwd" }); text(JSON.stringify({ started, ended: Date.now(), where }));`],
  "hands-latency": [() => `text("pong");`],
};

function model(input) {
  let index = input.messages.length - 1;
  while (index >= 0 && prompt(input.messages[index]) === undefined) index--;
  const results = input.messages.slice(index + 1).flatMap(message => message.role === "user" && Array.isArray(message.content) ? message.content.filter(block => block.type === "tool_result") : []);
  const request = prompt(input.messages[index]) ?? "";
  const scenario = /hands-[a-z]+/.exec(request)?.[0];
  const key = /\[(\S+)\]/.exec(request)?.[1];
  if (!scenario || !key) throw new Error("Measurement prompt did not reach the provider");
  observed[key] = results.map(block => ({ error: !!block.is_error, text: blockText(block) }));
  const steps = scripts[scenario];
  return results.length >= steps.length ? reply(input, `${scenario}-done`) : exec(input, steps[results.length]());
}

const outbound = async request => {
  const url = new URL(request.url);
  if (url.hostname !== "provider.invalid" || request.headers.get("x-api-key") !== "local-provider") throw new Error("Provider request escaped configured credential policy");
  const input = await request.json();
  modelCalls++;
  return model(input);
};

const buildMs = process.env.HANDS_SKIP_BUILD ? 0 : buildHand();
const options = localOptions({ persistence, agent: { HANDS_ATTACH_GRACE_MS: String(graceMs), REMOTE_MAX_FRAME_BYTES: String(policy.max_frame_bytes), REMOTE_TIMEOUT_MS: String(policy.timeout_ms) } });
options.workers[1].outboundService = outbound;
const mf = new Miniflare(convertV4MiniflareOptions(options));
const name = `hands-${Date.now()}`;
const headers = { authorization: `Bearer ${token}`, "content-type": "application/json" };
const request = (path, init = {}) => mf.dispatchFetch(`http://worker/sessions/${name}${path}`, { ...init, headers });
const workspaces = {};
const logs = {};
const hands = {};
const measurements = [];

async function until(condition, detail, timeout = policy.timeout_ms * 2) {
  const begin = performance.now();
  while (!await condition()) {
    if (performance.now() - begin > timeout) throw new Error(typeof detail === "function" ? detail() : detail);
    await new Promise(resolve => setTimeout(resolve, 5));
  }
  return performance.now() - begin;
}

async function attached() {
  const response = await request("/hands");
  if (response.status !== 200) throw new Error(`Hand listing rejected: ${await response.text()}`);
  return (await response.json()).hands;
}

function startHand(hand, base, named = true) {
  workspaces[hand] ??= resolve(mkdtempSync(`output/hands-${hand}-`));
  logs[hand] ??= "";
  const child = spawn(binary, ["hand", `${base.replace("http", "ws")}sessions/${name}/hands`, ...(named ? ["--name", hand] : []), "-C", workspaces[hand]], { env: { ...process.env, NANOCODE_RUNNER_TOKEN: token }, stdio: ["ignore", "ignore", "pipe"], detached: true });
  groups.add(child.pid);
  child.stderr.on("data", chunk => { logs[hand] += chunk; });
  const exited = new Promise(resolve => child.once("exit", () => { groups.delete(child.pid); resolve(); }));
  hands[hand] = { child, exited };
}

async function stopHand(hand) {
  try { process.kill(-hands[hand].child.pid, "SIGKILL"); } catch (error) { if (error.code !== "ESRCH") throw error; }
  await hands[hand].exited;
}

async function turn(id, scenario, hand) {
  const begin = performance.now();
  const response = await request("/turns", { method: "POST", body: JSON.stringify({ request_id: id, prompt: `Run ${scenario} [${id}]`, ...(hand === undefined ? {} : { hand }) }) });
  if (![200, 202].includes(response.status)) throw new Error(`Submit rejected: ${await response.text()}`);
  let result;
  await until(async () => {
    result = await (await request(`/turns/${encodeURIComponent(id)}`)).json();
    return ["failed", "completed", "cancelled"].includes(result.status);
  }, () => `Turn ${id} stalled: ${JSON.stringify(result)}`);
  const milliseconds = performance.now() - begin;
  measurements.push({ id, hand, status: result.status, milliseconds });
  if (result.status !== "completed") throw new Error(`Turn ${id} did not complete: ${JSON.stringify(result)}`);
  return { milliseconds, results: observed[id] ?? [], timing: result.timing };
}

const expect = (condition, detail) => { if (!condition) throw new Error(detail); };

function perHand(result) {
  const parts = (result?.text ?? "").split(/\[hand ([^\]\n]+)\]\n/);
  const sections = {};
  for (let index = 1; index < parts.length; index += 2) sections[parts[index]] = parts[index + 1];
  return sections;
}

try {
  const base = String(await mf.ready);
  const attachStart = performance.now();
  startHand("laptop", base, false);
  await until(async () => (await attached()).length === 1, () => `Machine-named hand did not attach: ${JSON.stringify(logs)}`);
  const laptopName = (await attached())[0];
  const attachMs = performance.now() - attachStart;

  const pinned = await turn("thread-pin", "hands-where", laptopName);
  expect(pinned.results[0]?.text.includes(workspaces.laptop), `pinned turn ran elsewhere: ${JSON.stringify(pinned.results)}`);
  const kept = await turn("thread-kept", "hands-remember");
  expect(kept.results[0]?.text.includes("stored") && !kept.results[0].error, `turn without hand left the pin: ${JSON.stringify(kept.results)}`);

  const deskStart = performance.now();
  startHand("desk", base);
  const switched = await turn("thread-desk", "hands-where", "desk");
  const deskJoinMs = performance.now() - deskStart;
  expect(switched.results[0]?.text.includes(workspaces.desk), `new hand turn ran elsewhere: ${JSON.stringify(switched.results)}`);
  const back = await turn("thread-back", "hands-recall", laptopName);
  expect(back.results[0]?.text.includes("kept-on-hand"), `returning to the first hand lost its state: ${JSON.stringify(back.results)}`);

  const together = await turn("thread-all", "hands-together", "*");
  const sections = perHand(together.results[0]);
  const spans = Object.fromEntries(Object.entries(sections).map(([hand, text]) => [hand, /"started":(\d+),"ended":(\d+)/.exec(text)?.slice(1).map(Number)]));
  expect(Object.keys(spans).sort().join() === [laptopName, "desk"].sort().join() && Object.values(spans).every(Boolean), `every-hand turn: ${JSON.stringify(together.results)}`);
  expect(sections[laptopName].includes(workspaces.laptop) && sections.desk.includes(workspaces.desk), `every-hand turn ran elsewhere: ${JSON.stringify(sections)}`);
  const allStarted = Math.min(...Object.values(spans).map(span => span[0]));
  const allEnded = Math.max(...Object.values(spans).map(span => span[1]));
  const overlapped = Math.max(...Object.values(spans).map(span => span[0])) < Math.min(...Object.values(spans).map(span => span[1]));
  expect(overlapped, `hands ran one after another: ${JSON.stringify(spans)}`);

  const listed = await turn("thread-list", "hands-where", [laptopName, "desk"]);
  const listedSections = perHand(listed.results[0]);
  expect(listedSections[laptopName]?.includes(workspaces.laptop) && listedSections.desk?.includes(workspaces.desk), `listed hands: ${JSON.stringify(listed.results)}`);

  await stopHand("desk");
  const deskLeftMs = await until(async () => !(await attached()).includes("desk"), "Killed hand stayed attached");
  const remaining = await turn("thread-all-departed", "hands-where", "*");
  const remainingSections = perHand(remaining.results[0]);
  expect(Object.keys(remainingSections).join() === laptopName && !remaining.results[0].error, `every-hand turn after a hand left: ${JSON.stringify(remaining.results)}`);
  const missing = await turn("thread-list-departed", "hands-where", [laptopName, "desk"]);
  const missingSections = perHand(missing.results[0]);
  expect(missingSections[laptopName]?.includes(workspaces.laptop) && /not connected; nothing was dispatched/.test(missingSections.desk ?? ""), `listed hands after one left: ${JSON.stringify(missing.results)}`);
  expect(missing.timing?.hand_wait_ms >= graceMs, `listed departed hand did not wait for its grace period: ${JSON.stringify(missing.timing)}`);

  const deskReturnStart = performance.now();
  logs.desk = "";
  startHand("desk", base);
  await until(async () => (await attached()).includes("desk"), () => `Hand did not reattach: ${logs.desk}`);
  const deskReturnMs = performance.now() - deskReturnStart;
  const rejoined = await turn("thread-all-back", "hands-where", "*");
  expect(Object.keys(perHand(rejoined.results[0])).length === 2, `every-hand turn after reattach: ${JSON.stringify(rejoined.results)}`);

  const unpinned = await turn("thread-unpinned", "hands-where", null);
  expect(/no hand is selected/.test(unpinned.results[0]?.text ?? ""), `unpinned turn: ${JSON.stringify(unpinned.results)}`);
  const stillUnpinned = await turn("thread-still-unpinned", "hands-where");
  expect(/no hand is selected/.test(stillUnpinned.results[0]?.text ?? ""), `turn after unpinning: ${JSON.stringify(stillUnpinned.results)}`);
  const invalid = await request("/turns", { method: "POST", body: JSON.stringify({ request_id: "invalid-hand", prompt: "Run hands-where [invalid-hand]", hand: ["desk", "desk"] }) });
  expect(invalid.status === 400, `duplicate hand list was accepted with ${invalid.status}`);

  const callback = await turn("callback", "hands-callback", laptopName);
  expect(callback.results[0]?.text.includes("agents"), `brain callback failed: ${JSON.stringify(callback.results)}`);
  const stored = await turn("store", "hands-store", laptopName);
  expect(stored.results[1]?.text.includes("kept-on-hand"), `store/load did not stay on the hand: ${JSON.stringify(stored.results)}`);

  await stopHand("laptop");
  const detachMs = await until(async () => !(await attached()).includes(laptopName), "Killed hand stayed attached");
  const departed = await turn("where-departed", "hands-where", laptopName);
  expect(/not connected; nothing was dispatched/.test(departed.results[0]?.text ?? ""), `departed hand: ${JSON.stringify(departed.results)}`);
  expect(departed.timing?.hand_wait_ms >= graceMs, `departed hand did not wait for its grace period: ${JSON.stringify(departed.timing)}`);

  const lateTurn = turn("where-late", "hands-where", "late");
  await new Promise(resolve => setTimeout(resolve, 300));
  const lateStart = performance.now();
  startHand("late", base);
  const late = await lateTurn;
  const lateAttachedMs = performance.now() - lateStart;
  expect(late.results[0]?.text.includes(workspaces.late), `late hand: ${JSON.stringify(late.results)}`);
  expect(late.timing?.hand_wait_ms > 0 && late.timing.hand_wait_ms < graceMs, `late hand wait: ${JSON.stringify(late.timing)}`);
  await stopHand("late");

  const reconnectStart = performance.now();
  logs.laptop = "";
  startHand("laptop", base, false);
  await until(async () => (await attached()).includes(laptopName), () => `Hand did not reattach: ${logs.laptop}`);
  const reconnectMs = performance.now() - reconnectStart;
  const returned = await turn("where-returned", "hands-where", laptopName);
  expect(returned.results[0]?.text.includes(workspaces.laptop), `returned hand: ${JSON.stringify(returned.results)}`);

  await new Promise(resolve => setTimeout(resolve, keepaliveMs));
  const reconnects = (logs.laptop.match(/hand: connected/g) ?? []).length;
  expect(reconnects === 1 && (await attached()).includes(laptopName), `Idle hand did not stay attached: ${logs.laptop}`);
  const idle = await turn("where-idle", "hands-where");
  expect(idle.results[0]?.text.includes(workspaces.laptop), `idle hand: ${JSON.stringify(idle.results)}`);

  const latencies = [];
  for (let index = 0; index < latencyTurns; index++) latencies.push((await turn(`latency-${index}`, "hands-latency", index % 2 ? "desk" : laptopName)).milliseconds);
  latencies.sort((a, b) => a - b);

  const report = {
    runtime: "local workerd (Miniflare) public + egress Workers, nanocodex hand processes dialing the session Durable Object over loopback",
    note: "Local transport and mock provider; not production colo latency.",
    hand_build_ms: buildMs,
    machine_named_hand: laptopName,
    attach_machine_named_hand_ms: attachMs,
    thread: {
      pinned_turn_ms: pinned.milliseconds,
      unchanged_pin_turn_ms: kept.milliseconds,
      new_hand_spawn_to_turn_complete_ms: deskJoinMs,
      back_to_first_hand_turn_ms: back.milliseconds,
      every_hand: { turn_ms: together.milliseconds, hands: Object.keys(spans), one_second_cells_wall_ms: allEnded - allStarted, overlapped },
      listed_hands_turn_ms: listed.milliseconds,
      killed_hand_detached_ms: deskLeftMs,
      every_hand_after_departure: Object.keys(remainingSections),
      listed_departed_hand_wait_ms: missing.timing?.hand_wait_ms,
      reattached_ms: deskReturnMs,
      unpinned_result: unpinned.results[0]?.text,
    },
    turn_ms: { callback: callback.milliseconds },
    killed_hand_detached_ms: detachMs,
    departed_hand_result: departed.results[0]?.text,
    departed_hand_wait_ms: departed.timing?.hand_wait_ms,
    late_hand: { submitted_before_spawn_ms: 300, spawn_to_turn_complete_ms: lateAttachedMs, hand_wait_ms: late.timing?.hand_wait_ms, turn_ms: late.milliseconds },
    reconnect_to_attached_ms: reconnectMs,
    idle_ms: keepaliveMs,
    idle_reconnects: reconnects - 1,
    single_exec_turn_ms: { median: latencies[Math.floor(latencies.length / 2)], min: latencies[0], max: latencies.at(-1) },
    model_calls: modelCalls,
    measurements,
  };
  mkdirSync("output", { recursive: true });
  writeFileSync("output/hands-measurement.json", JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
} catch (error) {
  console.error(error);
  console.error(JSON.stringify({ logs, observed }, null, 2));
  process.exitCode = 1;
} finally {
  await mf.dispose();
  stopHands();
}

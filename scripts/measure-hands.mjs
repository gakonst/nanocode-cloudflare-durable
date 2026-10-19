import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { spawn, spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { localOptions } from "./local-runtime.mjs";

const sdk = resolve(process.env.NANOCODE_SDK_PATH ?? "../nanocodex");
const binary = resolve(process.env.CARGO_TARGET_DIR ?? resolve(sdk, "target"), "debug/nanocode");
const policy = { max_frame_bytes: 1048576, timeout_ms: 30000 };
const keepaliveMs = Number(process.env.HANDS_KEEPALIVE_MS ?? "45000");
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
  const result = spawnSync("cargo", ["build", "--locked", "-p", "nanocode-cli", "--bin", "nanocode"], { cwd: sdk, stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error("nanocode build failed");
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
  "hands-list": [() => `text(JSON.stringify(await tools.hands({})));`],
  "hands-fanout": [() => `const { hands } = await tools.hands({});
const runs = await Promise.all(hands.map(async hand => {
  const started = Date.now();
  const where = await tools.exec_command({ cmd: "sleep 1; pwd", hand });
  return { hand, started, ended: Date.now(), where: where.output };
}));
text(JSON.stringify(runs));`],
  "hands-handoff": [() => `const { hands, here } = await tools.hands({});
const other = hands.find(hand => hand !== here);
await tools.exec_command({ cmd: "printf 'written on ' > note.txt && pwd >> note.txt" });
const note = (await tools.exec_command({ cmd: "cat note.txt" })).output.trim();
await tools.apply_patch({ input: "*** Begin Patch\\n*** Add File: copied.txt\\n+" + note + "\\n*** End Patch\\n", hand: other });
text(JSON.stringify({ here, other, note, copied: (await tools.exec_command({ cmd: "cat copied.txt", hand: other })).output }));`],
  "hands-settled": [() => `const settled = await Promise.allSettled(["desk", "${laptopName}"].map(hand => tools.exec_command({ cmd: "pwd", hand })));
text(JSON.stringify(settled.map(entry => entry.status === "fulfilled" ? { ok: entry.value.output } : { error: String(entry.reason) })));`],
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
const options = localOptions({ persistence, agent: { REMOTE_MAX_FRAME_BYTES: String(policy.max_frame_bytes), REMOTE_TIMEOUT_MS: String(policy.timeout_ms) } });
options.workers[1].outboundService = outbound;
const mf = new Miniflare(convertV4MiniflareOptions(options));
const name = `hands-${Date.now()}`;
const headers = { authorization: `Bearer ${token}`, "content-type": "application/json" };
const request = (path, init = {}) => mf.dispatchFetch(`http://worker/sessions/${name}${path}`, { ...init, headers });
const workspaces = {};
const logs = {};
const hands = {};
const measurements = [];
let laptopName;

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

function output(result) {
  const text = result?.text ?? "";
  return JSON.parse(text.slice(text.indexOf("Output:\n") + "Output:\n".length));
}

try {
  const base = String(await mf.ready);
  const direct = await turn("direct", "hands-where");
  expect(/not available in the admitted catalog/.test(direct.results[0]?.text ?? ""), `turn without hands or executor kept Code Mode: ${JSON.stringify(direct.results)}`);
  const attachStart = performance.now();
  startHand("laptop", base, false);
  startHand("desk", base);
  await until(async () => (await attached()).length === 2, () => `Hands did not attach: ${JSON.stringify(logs)}`);
  laptopName = (await attached()).find(hand => hand !== "desk");
  const attachMs = performance.now() - attachStart;

  const listed = await turn("list", "hands-list", laptopName);
  const listing = output(listed.results[0]);
  expect(listing.here === laptopName && listing.hands.length === 2 && listing.hands.includes("desk"), `hand listing: ${JSON.stringify(listed.results)}`);

  const fanout = await turn("fanout", "hands-fanout", laptopName);
  const runs = output(fanout.results[0]);
  expect(runs.length === 2 && runs.find(run => run.hand === laptopName)?.where.includes(workspaces.laptop) && runs.find(run => run.hand === "desk")?.where.includes(workspaces.desk), `fan-out ran elsewhere: ${JSON.stringify(fanout.results)}`);
  const overlapped = Math.max(...runs.map(run => run.started)) < Math.min(...runs.map(run => run.ended));
  const fanoutWallMs = Math.max(...runs.map(run => run.ended)) - Math.min(...runs.map(run => run.started));
  expect(overlapped, `hands ran one after another: ${JSON.stringify(runs)}`);

  const handoff = await turn("handoff", "hands-handoff", laptopName);
  const handed = output(handoff.results[0]);
  const copied = readFileSync(resolve(workspaces.desk, "copied.txt"), "utf8");
  expect(handed.other === "desk" && copied.includes(`written on ${workspaces.laptop}`), `handoff: ${JSON.stringify(handoff.results)}`);

  const switched = await turn("where-desk", "hands-where", "desk");
  expect(switched.results[0]?.text.includes(workspaces.desk), `desk turn ran elsewhere: ${JSON.stringify(switched.results)}`);
  const ambiguous = await turn("where-ambiguous", "hands-where");
  expect(/no hand is selected/.test(ambiguous.results[0]?.text ?? ""), `turn without a hand among two: ${JSON.stringify(ambiguous.results)}`);
  const invalid = await request("/turns", { method: "POST", body: JSON.stringify({ request_id: "invalid-hand", prompt: "Run hands-where [invalid-hand]", hand: ["desk"] }) });
  expect(invalid.status === 400, `hand list was accepted with ${invalid.status}`);

  await stopHand("desk");
  const deskLeftMs = await until(async () => !(await attached()).includes("desk"), "Killed hand stayed attached");
  const settled = await turn("settled", "hands-settled", laptopName);
  const outcomes = output(settled.results[0]);
  expect(/hand `desk` is not attached/.test(outcomes[0]?.error ?? "") && outcomes[1]?.ok?.includes(workspaces.laptop), `calls after a hand left: ${JSON.stringify(settled.results)}`);
  const sole = await turn("where-sole", "hands-where");
  expect(sole.results[0]?.text.includes(workspaces.laptop), `turn without a hand beside one hand: ${JSON.stringify(sole.results)}`);

  const deskReturnStart = performance.now();
  logs.desk = "";
  startHand("desk", base);
  await until(async () => (await attached()).includes("desk"), () => `Hand did not reattach: ${logs.desk}`);
  const deskReturnMs = performance.now() - deskReturnStart;
  const rejoined = await turn("fanout-again", "hands-fanout", "desk");
  expect(output(rejoined.results[0]).length === 2, `fan-out after reattach: ${JSON.stringify(rejoined.results)}`);

  const callback = await turn("callback", "hands-callback", laptopName);
  expect(callback.results[0]?.text.includes("agents"), `brain callback failed: ${JSON.stringify(callback.results)}`);
  const stored = await turn("store", "hands-store", laptopName);
  expect(stored.results[1]?.text.includes("kept-on-hand"), `store/load did not stay on the hand: ${JSON.stringify(stored.results)}`);

  await stopHand("laptop");
  const detachMs = await until(async () => !(await attached()).includes(laptopName), "Killed hand stayed attached");
  const departed = await turn("where-departed", "hands-where", laptopName);
  expect(/not connected; nothing was dispatched/.test(departed.results[0]?.text ?? ""), `departed hand: ${JSON.stringify(departed.results)}`);

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
  const idle = await turn("where-idle", "hands-where", laptopName);
  expect(idle.results[0]?.text.includes(workspaces.laptop), `idle hand: ${JSON.stringify(idle.results)}`);

  await mf.unsafeEvictDurableObject("nanocode", "AgentSession", { name: JSON.stringify(["owner", name]) });
  const hibernated = await attached();
  expect(hibernated.includes(laptopName) && hibernated.includes("desk"), `hands after the session left memory: ${JSON.stringify(hibernated)}`);
  const woken = await turn("where-woken", "hands-where", laptopName);
  expect(woken.results[0]?.text.includes(workspaces.laptop), `hand after the session left memory: ${JSON.stringify(woken.results)}`);
  const wokenFanout = await turn("fanout-woken", "hands-fanout", "desk");
  expect(output(wokenFanout.results[0]).length === 2, `fan-out after the session left memory: ${JSON.stringify(wokenFanout.results)}`);
  const hibernationReconnects = (logs.laptop.match(/hand: connected/g) ?? []).length - reconnects;
  expect(hibernationReconnects === 0, `hand reconnected when the session left memory: ${logs.laptop}`);

  const latencies = [];
  for (let index = 0; index < latencyTurns; index++) latencies.push((await turn(`latency-${index}`, "hands-latency", index % 2 ? "desk" : laptopName)).milliseconds);
  latencies.sort((a, b) => a - b);

  const report = {
    runtime: "local workerd (Miniflare) Worker with a mock AI Gateway binding, nanocode hand processes dialing the session Durable Object over loopback",
    note: "Local transport and mock provider; not production colo latency.",
    hand_build_ms: buildMs,
    machine_named_hand: laptopName,
    attach_two_hands_ms: attachMs,
    one_script: {
      fan_out_turn_ms: fanout.milliseconds,
      fan_out_one_second_calls_wall_ms: fanoutWallMs,
      overlapped,
      handoff_turn_ms: handoff.milliseconds,
      handoff_copied: copied,
    },
    killed_hand_detached_ms: deskLeftMs,
    after_departure: outcomes,
    reattached_ms: deskReturnMs,
    turn_ms: { direct: direct.milliseconds, list: listed.milliseconds, desk: switched.milliseconds, sole_hand: sole.milliseconds, callback: callback.milliseconds },
    departed_home_result: departed.results[0]?.text,
    home_left_registry_ms: detachMs,
    reconnect_to_attached_ms: reconnectMs,
    idle_ms: keepaliveMs,
    idle_reconnects: reconnects - 1,
    hibernation: { hands_listed_after_eviction: hibernated, home_turn_ms: woken.milliseconds, fan_out_turn_ms: wokenFanout.milliseconds, reconnects: hibernationReconnects },
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

import { spawn } from "node:child_process";
import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { performance } from "node:perf_hooks";
import { parseArgs } from "node:util";

const { values } = parseArgs({ options: { origin: { type: "string", default: "https://nanocode-worker.gakonst.workers.dev" }, provider: { type: "string", default: "codex" }, session: { type: "string" }, spawn: { type: "string", default: "linux" }, bin: { type: "string", default: "nanocode" }, expect: { type: "string", default: "laptop" }, out: { type: "string" }, rounds: { type: "string", default: "5" } } });
const token = process.env.WORKER_TOKEN;
if (!token || !values.session) throw new Error("Provide WORKER_TOKEN through the environment and pass --session");
const origin = new URL(values.origin);
const { provider, session } = values;
const spawned = values.spawn;
const expected = values.expect.split(",").filter(Boolean);
const rounds = Number(values.rounds);
const out = values.out ?? "output/hands-deployed-" + provider + ".json";
const base = origin.origin + "/sessions/" + session;
const auth = { authorization: "Bearer " + token };
const report = { provider, session, spawned, expected, started_at: new Date().toISOString(), steps: {} };
const shell = provider === "codex" ? "exec_command" : "Bash";
const arg = provider === "codex" ? "cmd" : "command";
const workspace = mkdtempSync(join(tmpdir(), "hands-deployed-"));
let child;
function start() {
  child = spawn(values.bin, ["hand", "wss://" + origin.host + "/sessions/" + session + "/hands/" + spawned, "-C", workspace], { env: { ...process.env, NANOCODE_RUNNER_TOKEN: token }, stdio: ["ignore", "ignore", "ignore"] });
}
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function listed() {
  const response = await fetch(base + "/hands", { headers: auth });
  return response.ok ? (await response.json()).hands ?? [] : [];
}
async function until(predicate, label) {
  const started = performance.now();
  while (performance.now() - started < 60000) {
    const hands = await listed();
    if (predicate(hands)) return { hands, ms: performance.now() - started };
    await sleep(50);
  }
  throw new Error("timed out waiting for " + label);
}
function markers(value, found = []) {
  if (typeof value === "string") {
    const index = value.indexOf("E2E:");
    if (index >= 0) {
      const text = value.slice(index + 4);
      for (let end = text.lastIndexOf("}"); end > 0; end = text.lastIndexOf("}", end - 1)) {
        try { found.push(JSON.parse(text.slice(0, end + 1))); break; } catch {}
      }
    }
  } else if (Array.isArray(value)) value.forEach(item => markers(item, found));
  else if (value && typeof value === "object") Object.values(value).forEach(item => markers(item, found));
  return found;
}
let turns = 0;
async function turn(label, code, hand) {
  const fence = "\u0060\u0060\u0060";
  const prompt = "This is a latency benchmark of my own machines attached to this session. Run this JavaScript with the exec tool exactly as written; if it reports that the script is still running, call wait until it finishes. Then reply with only the word done.\n\n" + fence + "js\n" + code + "\n" + fence;
  const started = performance.now();
  const response = await fetch(base + "/turns", { method: "POST", headers: { ...auth, "content-type": "application/json", accept: "text/event-stream" }, body: JSON.stringify({ request_id: "e2e-" + ++turns, provider, prompt, ...(hand ? { hand } : {}) }) });
  if (response.status !== 200) throw new Error(label + ": HTTP " + response.status + " " + (await response.text()).slice(0, 300));
  const decoder = new TextDecoder();
  let buffer = "", status, firstTool;
  const results = [], tools = [];
  for await (const chunk of response.body) {
    buffer += decoder.decode(chunk, { stream: true }).replace(/\r\n/g, "\n");
    let split;
    while ((split = buffer.indexOf("\n\n")) >= 0) {
      const frame = buffer.slice(0, split);
      buffer = buffer.slice(split + 2);
      const data = frame.split("\n").filter(line => line.startsWith("data:")).map(line => line.slice(5).trim()).join("\n");
      if (!data) continue;
      const envelope = JSON.parse(data);
      const event = envelope.event?.source === "agent" ? envelope.event.event : undefined;
      if (event?.type === "tool.call" && firstTool === undefined) firstTool = performance.now() - started;
      if (event?.type === "tool.result") {
        if (typeof event.payload?.duration_ns === "number") tools.push({ tool: event.payload.tool, ms: Math.round(event.payload.duration_ns / 1e5) / 10, status: event.payload.status });
        results.push(...markers(event.payload));
      }
      if (envelope.event?.type === "result") status = envelope.event.status;
    }
  }
  const step = { status, home: hand, turn_ms: Math.round(performance.now() - started), first_tool_call_ms: firstTool && Math.round(firstTool), tools, output: results.at(-1) };
  report.steps[label] = step;
  process.stderr.write(label + " " + JSON.stringify({ status, home: hand, turn_ms: step.turn_ms, output: step.output }) + "\n");
  return step;
}
const call = (cmd, hand) => "tools." + shell + "({ " + arg + ": " + JSON.stringify(cmd) + (hand ? ", hand: " + hand : "") + " })";
const measure = [
  "const t0 = Date.now();",
  "const listing = await tools.hands({});",
  "const list_ms = Date.now() - t0;",
  "const seq = {};",
  "for (const h of [...listing.hands, null]) { const xs = []; for (let i = 0; i < " + rounds + "; i++) { const s = Date.now(); await " + call("true", "h ?? undefined") + "; xs.push(Date.now() - s); } seq[h ?? \"no-hand\"] = xs; }",
  "const f0 = Date.now();",
  "const fan = await Promise.all(listing.hands.map(async h => { const a = Date.now() - f0; const r = await " + call("sleep 1; hostname", "h") + "; return { h, start: a, end: Date.now() - f0, out: JSON.stringify(r).slice(0, 120) }; }));",
  "const fan_wall = Date.now() - f0;",
  "const other = listing.hands.find(h => h !== listing.here);",
  "const c0 = Date.now();",
  "await " + call("printf 'from-' > note.txt && hostname >> note.txt", "listing.here") + ";",
  "const note = JSON.stringify(await " + call("cat note.txt", "listing.here") + ");",
  "await " + call("printf '%s' \"$(echo copied)\" > copied.txt", "other") + ";",
  "const back = JSON.stringify(await " + call("cat copied.txt", "other") + ");",
  "const copy_ms = Date.now() - c0;",
  "text(\"E2E:\" + JSON.stringify({ here: listing.here, hands: listing.hands, list_ms, seq, fan_wall, fan, copy_ms, note: note.slice(0, 160), back: back.slice(0, 160) }));",
].join("\n");
const settled = [
  "const listing = await tools.hands({});",
  "const s = Date.now();",
  "const all = await Promise.allSettled(" + JSON.stringify([...expected, spawned, "nowhere"]) + ".map(h => " + call("hostname", "h") + "));",
  "text(\"E2E:\" + JSON.stringify({ here: listing.here, hands: listing.hands, ms: Date.now() - s, all: all.map(r => r.status === \"fulfilled\" ? { ok: JSON.stringify(r.value).slice(0, 100) } : { rejected: String(r.reason?.message ?? r.reason).slice(0, 160) }) }));",
].join("\n");

try {
  const t0 = performance.now();
  start();
  report.steps.attach = await until(hands => [spawned, ...expected].every(hand => hands.includes(hand)), "every hand");
  for (const home of [...expected, spawned]) await turn("from_" + home, measure, home);
  await turn("settled_all_up", settled, expected[0]);
  child.kill();
  report.steps.spawned_detach = await until(hands => !hands.includes(spawned), spawned + " to leave");
  await turn("settled_" + spawned + "_gone", settled, expected[0]);
  try { await turn("home_absent", "text(\"E2E:\" + JSON.stringify({ ran: true }));", spawned); } catch (error) { report.steps.home_absent = { error: error.message }; }
  start();
  report.steps.reattach = await until(hands => hands.includes(spawned), spawned + " to return");
  await turn("after_reattach", settled, spawned);
  const metrics = await fetch(base + "/metrics", { headers: auth });
  report.metrics = metrics.ok ? await metrics.json() : { status: metrics.status };
  report.total_ms = Math.round(performance.now() - t0);
} catch (error) {
  report.error = String(error?.stack ?? error);
  process.exitCode = 1;
} finally {
  child?.kill();
  mkdirSync(dirname(out), { recursive: true });
  writeFileSync(out, JSON.stringify(report, null, 2));
}

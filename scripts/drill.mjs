import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { GATEWAY_KEY, LOCAL_TOKEN, localOptions } from "./local-runtime.mjs";

const CRASH_AFTER_MS = 1500;
const WAIT_MS = 3000;
const RESTART_INTO_WAIT_MS = 800;
const STEPS = 5;
const UNKNOWN = /outcome (is )?unknown|interrupted by a host crash/;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const text = content => typeof content === "string" ? content : Array.isArray(content) ? content.map(part => part.text ?? "").join("") : "";

let requests = [];
let charges = [];
let reply = () => ({ text: "ok" });
let holds = [];
let restarts = [];

function sse(events) {
  return new Response(events.map(([event, data]) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`).join(""), { headers: { "content-type": "text/event-stream" } });
}

function message(body, answer) {
  const block = answer.code === undefined
    ? [{ type: "text", text: "" }, { type: "text_delta", text: answer.text }]
    : [{ type: "tool_use", id: `toolu_${requests.length}`, name: "exec", input: {} }, { type: "input_json_delta", partial_json: JSON.stringify({ code: answer.code }) }];
  return sse([
    ["message_start", { type: "message_start", message: { id: `message-${requests.length}`, type: "message", role: "assistant", model: body.model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 8, output_tokens: 0 } } }],
    ["content_block_start", { type: "content_block_start", index: 0, content_block: block[0] }],
    ["content_block_delta", { type: "content_block_delta", index: 0, delta: block[1] }],
    ["content_block_stop", { type: "content_block_stop", index: 0 }],
    ["message_delta", { type: "message_delta", delta: { stop_reason: answer.code === undefined ? "end_turn" : "tool_use", stop_sequence: null }, usage: { output_tokens: 4 } }],
    ["message_stop", { type: "message_stop" }],
  ]);
}

const provider = async request => {
  if (new URL(request.url).hostname !== "provider.invalid" || request.headers.get("x-api-key") !== GATEWAY_KEY) throw new Error("the model request escaped the gateway credential policy");
  const body = await request.json();
  const results = body.messages.flatMap(entry => Array.isArray(entry.content) ? entry.content.filter(block => block.type === "tool_result") : []);
  const entry = { messages: body.messages.length, round: results.length, tool_result: results.length ? text(results.at(-1).content) : undefined };
  requests.push(entry);
  const answer = reply(entry);
  entry.answer = answer.code ?? answer.text;
  if (!answer.hold) return message(body, answer);
  return new Response(new ReadableStream({ start: controller => { holds.push(controller); } }), { headers: { "content-type": "text/event-stream" } });
};

let mf;
let persistence;
const headers = { authorization: `Bearer ${LOCAL_TOKEN}`, "content-type": "application/json" };

function boot() {
  const options = localOptions({ provider, agent: { RECOVERY_DELAY_MS: "200" }, persistence });
  options.handleStructuredLogs = log => {
    const line = String(log.message ?? "");
    if (line.includes('{"charge"')) charges.push(JSON.parse(line.slice(line.indexOf('{"charge"'))).charge);
  };
  mf = new Miniflare(convertV4MiniflareOptions(options));
}

async function restart(reason) {
  const started = performance.now();
  const previous = mf;
  mf = undefined;
  for (const controller of holds.splice(0)) try { controller.error(new Error("restarted")); } catch {}
  await previous.dispose();
  boot();
  await mf.ready;
  restarts.push({ reason, ms: Math.round(performance.now() - started) });
}

async function request(path, init = {}) {
  for (let attempt = 0; ; attempt++) {
    while (!mf) await sleep(5);
    try { return await mf.dispatchFetch(`https://nanocode.local${path}`, { ...init, headers: { ...headers, ...init.headers } }); }
    catch (error) { if (attempt > 100) throw error; await sleep(20); }
  }
}

async function finish(session, id, deadline) {
  for (;;) {
    const response = await request(`/sessions/${session}/turns/${id}`);
    const report = response.status === 200 ? await response.json() : { status: `http ${response.status}` };
    if (["completed", "failed", "cancelled"].includes(report.status) || performance.now() > deadline) return report;
    await sleep(100);
  }
}

async function events(response, until) {
  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
  const seen = [];
  let buffered = "";
  for (;;) {
    const read = await reader.read().catch(error => ({ error }));
    if (read.error || read.done) return { seen, ended: read.error ? String(read.error.message ?? read.error) : "done" };
    buffered += read.value;
    let end;
    while ((end = buffered.indexOf("\n\n")) >= 0) {
      const block = buffered.slice(0, end);
      buffered = buffered.slice(end + 2);
      const data = block.split("\n").filter(line => line.startsWith("data:")).map(line => line.slice(5).trim()).join("");
      if (!data) continue;
      const event = JSON.parse(data);
      seen.push(event);
      if (until(event)) {
        await reader.cancel().catch(() => undefined);
        return { seen, ended: "result" };
      }
    }
  }
}

const scenarios = {
  async charge() {
    reply = entry => entry.tool_result !== undefined ? { text: `The tool answered: ${entry.tool_result}` } : { code: 'text(await tools.charge({ amount: 42, memo: "order 1001" }));' };
    const result = event => event.request_id === "turn-1" && event.event?.type === "result";
    const started = performance.now();
    const armed = await (await request("/sessions/drill/crash", { method: "POST", body: JSON.stringify({ after_ms: CRASH_AFTER_MS }) })).json();
    const first = await events(await request("/sessions/drill/turns", { method: "POST", headers: { accept: "text/event-stream" }, body: JSON.stringify({ request_id: "turn-1", prompt: "Please charge $42 for order 1001." }) }), result);
    const killed_ms = performance.now() - started;
    let after = first.seen.at(-1)?.seq ?? 0;
    const resumed = [];
    let finished = first.ended === "result" ? first.seen.find(result) : undefined;
    while (!finished && performance.now() - started < 60_000) {
      const response = await request(`/sessions/drill/events?after=${after}`, { headers: { accept: "text/event-stream" } }).catch(() => undefined);
      if (!response?.ok) {
        await sleep(200);
        continue;
      }
      const next = await events(response, result);
      resumed.push(...next.seen);
      after = next.seen.at(-1)?.seq ?? after;
      finished = next.seen.find(result);
    }
    return {
      report: { armed, first_stream_ended: first.ended, killed_ms, completed_ms: performance.now() - started, resumed_events: resumed.map(event => event.event?.event?.type ?? event.event?.type), result: finished?.event },
      failures: [
        first.ended === "result" && "the first stream finished the turn, so the crash never fired",
        charges.length !== 1 && `${charges.length} charges ran`,
        finished?.event?.status !== "completed" && "the turn did not complete",
        !requests.some(entry => UNKNOWN.test(entry.tool_result ?? "")) && "the model never heard that the charge's outcome is unknown",
        requests.filter(entry => entry.answer?.includes("tools.charge")).length !== 1 && "the model was asked for the charge more than once",
      ],
    };
  },
  async waiting() {
    reply = entry => {
      if (entry.round > 0) return { text: `The tool answered: ${entry.tool_result}` };
      if (requests.length === 1) setTimeout(() => restart("cell waiting before its first tool call"), RESTART_INTO_WAIT_MS);
      return { code: `await new Promise(resolve => setTimeout(resolve, ${WAIT_MS})); text(await tools.charge({ amount: 7, memo: "after the wait" }));` };
    };
    const started = performance.now();
    await request("/sessions/waiting/turns", { method: "POST", body: JSON.stringify({ request_id: "turn-1", prompt: "Wait three seconds, then charge $7." }) });
    const result = await finish("waiting", "turn-1", started + 60_000);
    const answered = requests.find(entry => entry.round > 0)?.tool_result ?? "";
    return {
      report: { completed_ms: performance.now() - started, restarts, result },
      failures: [
        restarts.length !== 1 && `${restarts.length} restarts happened`,
        result.status !== "completed" && `the turn ended ${result.status}${result.error ? `: ${result.error}` : ""}`,
        charges.length !== 1 && `${charges.length} charges ran`,
        UNKNOWN.test(answered) && `the cell had made no tool call when the host restarted, yet the model was told: ${answered.slice(0, 200)}`,
        !answered.includes("charged $7.00") && "the model never got the charge's result",
      ],
    };
  },
  async deploys() {
    const reset = new Set();
    reply = entry => {
      if (entry.round >= STEPS) return { text: "all steps done" };
      if (!reset.has(entry.round)) {
        reset.add(entry.round);
        setTimeout(() => restart(`model call ${entry.round + 1} held`), 300);
        return { hold: true };
      }
      return { code: `text("step ${entry.round + 1}");` };
    };
    const started = performance.now();
    await request("/sessions/deploys/turns", { method: "POST", body: JSON.stringify({ request_id: "turn-1", prompt: `Run ${STEPS} steps, one cell each.` }) });
    const result = await finish("deploys", "turn-1", started + 90_000);
    return {
      report: { completed_ms: performance.now() - started, restarts, result },
      failures: [
        result.status === "completed" && restarts.length !== STEPS && `${restarts.length} of ${STEPS} restarts happened`,
        result.status !== "completed" && `the turn, reset ${restarts.length} times with a model call and a cell completed between resets, ended ${result.status}${result.kind ? ` (${result.kind})` : ""}${result.error ? `: ${result.error}` : ""}`,
      ],
    };
  },
};

const selected = process.argv.slice(2).length ? process.argv.slice(2) : Object.keys(scenarios);
const report = { measured_at: new Date().toISOString(), scenarios: {} };
mkdirSync("output", { recursive: true });
for (const name of selected) {
  requests = [];
  charges = [];
  holds = [];
  restarts = [];
  persistence = resolve(mkdtempSync(`output/drill-${name}-`));
  boot();
  try {
    const outcome = await scenarios[name]();
    report.scenarios[name] = { ...outcome.report, charges: charges.length, model_requests: requests, failures: outcome.failures.filter(Boolean) };
  } catch (error) {
    report.scenarios[name] = { failures: [`the drill threw: ${error instanceof Error ? error.message : String(error)}`], model_requests: requests };
  } finally {
    while (!mf) await sleep(5);
    await mf.dispose();
    rmSync(persistence, { recursive: true, force: true });
  }
}
const failures = Object.entries(report.scenarios).flatMap(([name, entry]) => entry.failures.map(failure => `${name}: ${failure}`));
writeFileSync("output/drill.json", `${JSON.stringify({ ...report, failures }, null, 2)}\n`);
console.log(JSON.stringify({ scenarios: Object.fromEntries(Object.entries(report.scenarios).map(([name, entry]) => [name, { completed_ms: entry.completed_ms && Math.round(entry.completed_ms), killed_ms: entry.killed_ms && Math.round(entry.killed_ms), restarts: entry.restarts, status: entry.result?.status, kind: entry.result?.kind, charges: entry.charges, failures: entry.failures }])), failures }, null, 2));
if (failures.length) process.exitCode = 1;

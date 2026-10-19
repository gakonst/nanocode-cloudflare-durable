import { mkdirSync, writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { parseArgs } from "node:util";

const { values, positionals } = parseArgs({ allowPositionals: true, options: { turns: { type: "string", default: "2" }, idle: { type: "string", default: "0" }, label: { type: "string" }, session: { type: "string" }, prefix: { type: "string", default: "history" } } });
const origin = new URL(positionals[0] ?? "https://nanocode.gakonst.workers.dev");
const token = process.env.WORKER_TOKEN;
if (!token || /[\r\n]/.test(token)) throw new Error("Provide WORKER_TOKEN through your secret manager");
const prompt = "Call the list_agents tool three times, one call at a time, waiting for each result before the next call. Then reply with exactly: done.";
const session = values.session ?? "codex-history-" + Date.now().toString(36);
const path = "/sessions/" + encodeURIComponent(session);
const send = (target, init = {}) => fetch(new URL(target, origin), { ...init, headers: { authorization: "Bearer " + token, "content-type": "application/json" } });
const diagnostics = new Map();
const timings = new Map();
const initializations = [];

async function observe() {
  const metrics = await (await send(path + "/metrics")).json();
  for (const entry of metrics.model_diagnostics ?? []) diagnostics.set(entry.runtime_id + ":" + entry.dispatch_id, entry);
  for (const entry of metrics.runtime?.timing?.models ?? []) timings.set(entry.runtime_id + ":" + entry.dispatch_id, entry);
  initializations.push(metrics.runtime_initializations);
}

async function turn(id) {
  const started = performance.now();
  const submitted = await send(path + "/turns", { method: "POST", body: JSON.stringify({ request_id: id, prompt, provider: "codex" }) });
  if (![200, 202].includes(submitted.status)) throw new Error("Codex turn " + id + " was rejected with HTTP " + submitted.status + ": " + (await submitted.text()).slice(0, 300));
  for (;;) {
    const job = await (await send(path + "/turns/" + id)).json();
    if (["completed", "failed", "cancelled"].includes(job.status)) {
      await observe();
      return { request_id: id, status: job.status, ms: Math.round(performance.now() - started), message: typeof job.ok?.message === "string" ? job.ok.message.slice(0, 200) : job.error?.slice?.(0, 300) ?? null };
    }
    if (performance.now() - started > 300000) throw new Error("Codex turn " + id + " stalled");
    await new Promise(resolve => setTimeout(resolve, 250));
  }
}

await observe();
const prior = new Set(diagnostics.keys());
const results = [];
const count = Number(values.turns);
for (let index = 1; index <= count; index++) results.push(await turn(values.prefix + "-" + index));
if (Number(values.idle) > 0) {
  await new Promise(resolve => setTimeout(resolve, Number(values.idle) * 1000));
  results.push({ ...(await turn(values.prefix + "-after-idle")), idle_s: Number(values.idle) });
}
const events = [];
for (let after = 0; ;) {
  const page = await (await send(path + "/events?after=" + after + "&limit=128")).json();
  events.push(...page.events);
  if (!page.has_more) break;
  after = page.next_cursor;
}
const sdk = events.filter(entry => entry.event?.source === "agent").map(entry => ({ request_id: entry.request_id, runtime_id: entry.event.host_timing?.runtime_id, type: entry.event.event?.type, payload: entry.event.event?.payload ?? {} }));
const dispatches = [...diagnostics].filter(([key]) => !prior.has(key)).map(([, entry]) => {
  const attempt = Array.isArray(entry.attempts) ? entry.attempts.at(-1) : entry;
  const timing = timings.get(entry.runtime_id + ":" + entry.dispatch_id);
  return { runtime_id: entry.runtime_id, dispatch_id: entry.dispatch_id, transport: entry.transport, websocket: entry.websocket === true, status: entry.status, request_bytes: attempt?.body_bytes ?? null, input_items: attempt?.input_items ?? null, previous_response_id: attempt?.continued ?? null, headers_ms: timing?.headers_ms ?? null, last_byte_ms: timing?.last_byte_ms ?? null, response_bytes: timing?.bytes ?? null };
});
let next = 0;
const turns = results.map(result => {
  const own = sdk.filter(entry => entry.request_id === result.request_id);
  const attempts = own.filter(entry => entry.type === "model.attempt.started").length;
  const sent = dispatches.slice(next, next + attempts);
  next += attempts;
  const replays = new Map(own.filter(entry => entry.type === "model.attempt.started").map(entry => [entry.payload.model_call_index ?? entry.payload.call_index, entry.payload.replay_mode]));
  const calls = own.filter(entry => entry.type === "model.call.completed").map((entry, index) => {
    const usage = entry.payload.usage ?? {};
    const dispatch = sent.filter(value => value.status === 200)[index];
    const replay_mode = replays.get(entry.payload.call_index) ?? null;
    return { call_index: entry.payload.call_index, attempt: entry.payload.attempt, replay_mode, previous_response_id: dispatch?.previous_response_id ?? (replay_mode === null ? null : replay_mode === "incremental"), request_bytes: dispatch?.request_bytes ?? null, input_items: dispatch?.input_items ?? null, model_call_ms: Math.round((entry.payload.duration_ns ?? 0) / 1e6), first_output_ms: entry.payload.time_to_first_output_ns == null ? null : Math.round(entry.payload.time_to_first_output_ns / 1e6), input_tokens: usage.input_tokens ?? null, cached_tokens: usage.input_tokens_details?.cached_tokens ?? null, output_tokens: usage.output_tokens ?? null };
  });
  const retries = own.filter(entry => entry.type === "model.attempt.retrying").map(entry => ({ call_index: entry.payload.model_call_index, error_class: entry.payload.error_class, replay_mode: entry.payload.replay_mode }));
  const sum = key => calls.reduce((total, call) => total + (call[key] ?? 0), 0);
  return { ...result, runtimes: [...new Set(own.map(entry => entry.runtime_id).filter(Boolean))], dispatches: sent, retries, calls, summary: { model_calls: calls.length, dispatches: sent.length, request_bytes: sent.reduce((total, value) => total + (value.request_bytes ?? 0), 0), max_input_items: Math.max(0, ...calls.map(call => call.input_items ?? 0)), continued_calls: calls.filter(call => call.previous_response_id).length, model_call_ms: sum("model_call_ms"), input_tokens: sum("input_tokens"), cached_tokens: sum("cached_tokens"), output_tokens: sum("output_tokens") } };
});
const report = { origin: origin.origin, session, at: new Date().toISOString(), runtime_initializations: initializations, turns };
const runtimes = turns.map(value => value.runtimes.join(","));
mkdirSync("output", { recursive: true });
const file = "output/codex-history-" + (values.label ?? origin.hostname) + "-" + Date.now() + ".json";
writeFileSync(file, JSON.stringify(report, null, 2));
console.log(JSON.stringify({ file, session, runtimes, turns: turns.map(({ request_id, status, ms, summary, retries, calls }) => ({ request_id, status, ms, summary, retries, calls: calls.map(call => [call.request_bytes, call.input_items, call.replay_mode, call.model_call_ms, call.cached_tokens + "/" + call.input_tokens]) })) }, null, 2));

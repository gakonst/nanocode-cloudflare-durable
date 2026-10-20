import { spawn } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { parseArgs } from "node:util";

const { values, positionals } = parseArgs({ allowPositionals: true, options: { provider: { type: "string", default: "claude" }, turns: { type: "string", default: "3" }, "prompt-bytes": { type: "string", default: "20000" }, idle: { type: "string", default: "150" }, session: { type: "string" }, prefix: { type: "string", default: "cold" }, worker: { type: "string", default: "nanocode" }, label: { type: "string", default: "run" } } });
const origin = new URL(positionals[0] ?? "https://nanocode.gakonst.workers.dev");
const token = process.env.WORKER_TOKEN;
if (!token || /[\r\n]/.test(token)) throw new Error("Provide WORKER_TOKEN through the environment");
const auth = { authorization: "Bearer " + token, "content-type": "application/json" };
const round = value => typeof value === "number" ? Math.round(value * 10) / 10 : undefined;
const session = values.session ?? values.prefix + "-" + values.provider + "-" + Date.now().toString(36);
const path = suffix => new URL("/sessions/" + encodeURIComponent(session) + suffix, origin);
const filler = "Ignore this filler line, it only makes the session history larger.\n".repeat(Math.ceil(Number(values["prompt-bytes"]) / 68)).slice(0, Number(values["prompt-bytes"]));
const events = [];
const tail = spawn("npx", ["wrangler", "tail", values.worker, "--format", "json"], { stdio: ["ignore", "pipe", "ignore"] });
let buffer = "";
tail.stdout.on("data", chunk => {
  buffer += chunk;
  let depth = 0, start = -1, quoted = false, escaped = false;
  for (let index = 0; index < buffer.length; index++) {
    const char = buffer[index];
    if (quoted) { if (escaped) escaped = false; else if (char === "\\") escaped = true; else if (char === '"') quoted = false; continue; }
    if (char === '"') quoted = true;
    else if (char === "{") { if (depth++ === 0) start = index; }
    else if (char === "}" && --depth === 0) {
      try { events.push(JSON.parse(buffer.slice(start, index + 1))); } catch {}
      buffer = buffer.slice(index + 1);
      index = -1;
    }
  }
});
const sleep = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds));

async function request(step, url, init = {}) {
  const started = performance.now();
  const at = Date.now();
  const response = await fetch(url, { ...init, headers: { ...auth, ...init.headers } });
  const sample = { step, at, status: response.status, headers_ms: round(performance.now() - started) };
  if (response.headers.get("content-type")?.includes("text/event-stream")) {
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let text = "";
    for (;;) {
      const next = await reader.read();
      if (next.done) break;
      text += decoder.decode(next.value, { stream: true });
      const elapsed = round(performance.now() - started);
      if (text.includes('"model.call.started"')) sample.model_call_started_ms ??= elapsed;
      if (text.includes('"assistant.delta"')) sample.first_text_ms ??= elapsed;
    }
    sample.result = text.match(/"type":"result"[^}]*?"status":"([a-z]+)"/)?.[1] ?? (text.includes('"type":"result"') ? "result" : "missing");
  } else await response.arrayBuffer();
  sample.total_ms = round(performance.now() - started);
  return sample;
}

const turn = (step, id, prompt) => request(step, path("/turns"), { method: "POST", headers: { accept: "text/event-stream" }, body: JSON.stringify({ request_id: id, prompt, provider: values.provider }) });
const samples = [];
await sleep(8000);
const stamp = Date.now().toString(36);
if (!values.session) {
  for (let index = 0; index < Number(values.turns); index++) samples.push(await turn("history", "history-" + index, filler + "\nReply with only the word ok."));
}
await sleep(Number(values.idle) * 1000);
samples.push(await request("restart", path("/hands")));
samples.push(await request("reader", path("/turns/none-" + stamp)));
samples.push(await turn("open", "cold-" + stamp, "Reply with only the word ok."));
samples.push(await request("warm_reader", path("/turns/none-" + stamp)));
samples.push(await turn("warm", "warm-" + stamp, "Reply with only the word ok."));
await sleep(15000);
tail.kill();
const ours = events.filter(event => event.executionModel === "durableObject" && event.event?.request?.headers?.["x-nanocode-descriptor"]?.includes('"session_name":' + JSON.stringify(session))).sort((a, b) => a.eventTimestamp - b.eventTimestamp);
const kind = event => { const { pathname } = new URL(event.event.request.url); return event.event.request.method === "POST" ? "turn" : pathname.endsWith("/hands") ? "hands" : "read"; };
const expected = { history: "turn", restart: "hands", reader: "read", open: "turn", warm_reader: "read", warm: "turn" };
for (const sample of samples) {
  const index = ours.findIndex(event => event.eventTimestamp >= sample.at - 2000 && kind(event) === expected[sample.step]);
  if (index < 0) continue;
  const [event] = ours.splice(index, 1);
  sample.do_cpu_ms = event.cpuTime;
  sample.do_wall_ms = event.wallTime;
  sample.do_path = new URL(event.event.request.url).pathname;
}
const report = { origin: origin.origin, worker: values.worker, provider: values.provider, session, label: values.label, idle_s: Number(values.idle), prompt_bytes: Number(values["prompt-bytes"]), at: new Date().toISOString(), samples, unmatched_events: ours.map(event => ({ path: new URL(event.event.request.url).pathname, cpu: event.cpuTime, wall: event.wallTime })) };
mkdirSync("output", { recursive: true });
const file = "output/cold-deployed-" + values.label + "-" + values.provider + "-" + Date.now() + ".json";
writeFileSync(file, JSON.stringify(report, null, 2));
console.log(JSON.stringify({ file, session, samples: samples.map(({ at, ...rest }) => rest) }, null, 2));
process.exit(0);

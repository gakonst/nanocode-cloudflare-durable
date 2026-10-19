import { connect } from "node:http2";
import { readFileSync } from "node:fs";
import { createInterface } from "node:readline";
import { performance } from "node:perf_hooks";
import { parseArgs } from "node:util";

const { values, positionals } = parseArgs({ allowPositionals: true, options: { origin: { type: "string", default: "https://nanocode-worker.gakonst.workers.dev" }, session: { type: "string" }, provider: { type: "string", default: "claude" }, executor: { type: "string" }, hand: { type: "string" }, "hand-log": { type: "string" }, label: { type: "string" }, timing: { type: "boolean", default: false } } });
const origin = new URL(values.origin);
if (origin.username || origin.password || origin.search || origin.hash || origin.pathname !== "/" || origin.protocol !== "https:") throw new Error("Use an HTTPS origin");
if (!values.session || !/^[A-Za-z0-9._-]{1,128}$/.test(values.session)) throw new Error("Pass --session");
if (!["claude", "codex"].includes(values.provider)) throw new Error("provider must be claude or codex");
const token = process.env.WORKER_TOKEN;
if (!token || /[\r\n]/.test(token)) throw new Error("Provide WORKER_TOKEN through the environment");
const target = values.hand ? { hand: values.hand } : values.executor ? { executor: values.executor } : {};
const label = values.label ?? values.hand ?? values.executor ?? "no executor";
const started = performance.now();
let connectedMs;
const client = connect(origin.origin);
client.once("connect", () => { connectedMs = performance.now() - started; });
client.on("error", error => { process.stderr.write("Worker connection failed: " + error.message + "\n"); process.exitCode = 1; });
const keepalive = setInterval(() => { if (!client.closed && !client.destroyed) client.ping(() => {}); }, 30000);

function listing() {
  return new Promise((resolve, reject) => {
    const stream = client.request({ ":method": "GET", ":path": "/sessions/" + encodeURIComponent(values.session) + "/hands", authorization: "Bearer " + token });
    let body = "";
    stream.setEncoding("utf8");
    stream.on("data", chunk => { body += chunk; });
    stream.on("error", reject);
    stream.on("end", () => { try { resolve(JSON.parse(body).hands ?? []); } catch { reject(new Error("Invalid hand listing")); } });
    stream.end();
  });
}

async function handAttached() {
  if (!values.hand) return;
  const deadline = performance.now() + 30000;
  let connected = !values["hand-log"];
  while (performance.now() < deadline) {
    if (!connected) {
      let log = "";
      try { log = readFileSync(values["hand-log"], "utf8"); } catch {}
      connected = /(hand|executor): connected/.test(log);
    }
    if (connected) {
      const hands = await listing();
      if (hands.includes(values.hand)) return;
    }
    await new Promise(resolve => setTimeout(resolve, connected ? 25 : 10));
  }
  throw new Error("The hand did not attach" + (values["hand-log"] ? "; see " + values["hand-log"] : ""));
}

function render() {
  const calls = new Map();
  let last = "\n", speaker = "", channel = "", nextCall = 0, terminal, firstText;
  const write = value => {
    if (!value) return;
    process.stdout.write(value);
    last = value.slice(-1);
  };
  const line = (value = "") => {
    if (last !== "\n") write("\n");
    write(value + "\n");
  };
  const pretty = value => typeof value === "string" ? value : JSON.stringify(value, null, 2);
  const readable = value => {
    if (typeof value !== "string") return pretty(value);
    try {
      const parsed = JSON.parse(value);
      if (parsed && typeof parsed === "object") return typeof parsed.output === "string" ? parsed.output.trimEnd() : pretty(parsed);
    } catch {}
    return value;
  };
  const content = value => {
    if (Array.isArray(value)) return value.map(block => content(block)).filter(Boolean).join("\n");
    if (typeof value?.text === "string") return readable(value.text);
    if (Array.isArray(value?.content_blocks)) return content(value.content_blocks);
    if (value?.type) return "[" + value.type + "]";
    return readable(value);
  };
  return {
    get terminal() { return terminal; },
    get firstText() { return firstText; },
    finish() { if (last !== "\n") write("\n"); },
    envelope(envelope) {
      const outer = envelope.event;
      const child = outer?.source === "subagent" ? outer.update : undefined;
      const event = outer?.source === "agent" ? outer.event : child?.event;
      const agent = child?.id ? String(child.id) : "";
      const prefix = agent ? "[agent " + agent.slice(0, 8) + "] " : "";
      const payload = event?.payload ?? {};
      if (["assistant.delta", "reasoning.summary.delta"].includes(event?.type) && typeof payload.text === "string" && payload.text) {
        const nextChannel = event.type === "reasoning.summary.delta" ? "reasoning" : "answer";
        if (nextChannel === "answer" && !agent && firstText === undefined) firstText = performance.now();
        if (speaker !== agent || channel !== nextChannel) {
          if (nextChannel === "reasoning") line(prefix + "[reasoning]");
          else if (prefix || channel === "reasoning") line(prefix + "[answer]");
          speaker = agent;
          channel = nextChannel;
        }
        write(payload.text);
      }
      if (event?.type === "assistant.message" && last !== "\n") line();
      if (outer?.type === "queued") line("[queued]");
      if (outer?.type === "running") line("[running] " + values.provider + " on " + label);
      if (event?.type === "model.call.started") { line(prefix + "[model] " + values.provider + " generating"); channel = ""; }
      if (child?.type === "added") line("[agent started] " + (child.agent?.name ?? child.agent?.id ?? "subagent"));
      if (child?.type === "completion") line(prefix + "[agent finished] " + pretty(child.status));
      if (event?.type === "tool.call" || event?.type === "tool.result") {
        const key = agent + ":" + payload.call_id;
        let call = calls.get(key);
        if (!call) { call = { number: ++nextCall, tool: payload.tool ?? "tool" }; calls.set(key, call); }
        if (event.type === "tool.call") {
          const parent = calls.get(agent + ":" + payload.parent_call_id);
          line(prefix + "[tool #" + call.number + "] " + call.tool + (parent ? " (in #" + parent.number + ")" : ""));
          const args = payload.arguments;
          if (typeof args?.cmd === "string") { line("$ " + args.cmd); if (args.workdir) line("cwd: " + args.workdir); }
          else if (typeof args?.code === "string") line(args.code);
          else if (args !== undefined) line(pretty(args));
        } else {
          const result = payload.structured_result;
          const duration = typeof payload.duration_ns === "number" ? ", " + (payload.duration_ns / 1e6).toFixed(1) + "ms" : "";
          const exit = typeof result?.exit_code === "number" ? ", exit " + result.exit_code : "";
          line(prefix + "[result #" + call.number + "] " + call.tool + " (" + (payload.status ?? "finished") + duration + exit + ")");
          const output = typeof result?.output === "string" ? result.output : content(payload.result ?? result);
          if (output) line(output.trimEnd());
        }
      }
      if (outer?.type === "result") {
        terminal = outer.status ?? "failed";
        line("[" + terminal + "]");
      }
    },
  };
}

function turn(index, prompt) {
  const submitted = performance.now();
  const view = render();
  return new Promise((resolve, reject) => {
    const stream = client.request({ ":method": "POST", ":path": "/sessions/" + encodeURIComponent(values.session) + "/turns", authorization: "Bearer " + token, "content-type": "application/json", accept: "text/event-stream" });
    let buffer = "", status, headersMs, failure = "";
    stream.setEncoding("utf8");
    stream.on("response", headers => { status = headers[":status"]; headersMs = performance.now() - submitted; });
    stream.on("data", chunk => {
      if (status !== 200) { failure += chunk; return; }
      buffer += chunk.replace(/\r\n/g, "\n");
      let split;
      while ((split = buffer.indexOf("\n\n")) >= 0) {
        const frame = buffer.slice(0, split);
        buffer = buffer.slice(split + 2);
        const data = frame.split("\n").filter(item => item.startsWith("data:")).map(item => item.slice(5).replace(/^ /, "")).join("\n");
        if (!data) continue;
        try { view.envelope(JSON.parse(data)); } catch { process.stderr.write("Invalid Worker stream.\n"); }
      }
    });
    stream.on("error", reject);
    stream.on("end", () => {
      view.finish();
      if (status !== 200) { reject(new Error("Worker rejected the turn with HTTP " + status + ": " + failure.slice(0, 500))); return; }
      if (!view.terminal) { reject(new Error("Stream ended without a final result.")); return; }
      if (values.timing) process.stderr.write(JSON.stringify({ turn: index, connection_ms: index === 1 ? connectedMs : 0, headers_ms: headersMs, ttft_ms: view.firstText === undefined ? undefined : view.firstText - submitted, total_ms: performance.now() - submitted }) + "\n");
      if (view.terminal !== "completed") process.exitCode = 1;
      resolve();
    });
    stream.end(JSON.stringify({ request_id: "try-" + index, provider: values.provider, ...target, prompt }));
  });
}

try {
  await handAttached();
  let index = 0;
  if (positionals.length === 1 && positionals[0] === "-") {
    const input = createInterface({ input: process.stdin, crlfDelay: Infinity });
    if (process.stdin.isTTY) process.stdout.write("> ");
    for await (const prompt of input) {
      if (prompt.trim()) await turn(++index, prompt);
      if (process.stdin.isTTY) process.stdout.write("> ");
    }
  } else {
    for (const prompt of positionals) await turn(++index, prompt);
  }
} catch (error) {
  process.stderr.write(error.message + "\n");
  process.exitCode = 1;
} finally {
  clearInterval(keepalive);
  client.close();
}

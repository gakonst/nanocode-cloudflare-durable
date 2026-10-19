import { readFileSync } from "node:fs";
import { createInterface } from "node:readline";
import { performance } from "node:perf_hooks";
import { parseArgs } from "node:util";

const { values, positionals } = parseArgs({ allowPositionals: true, options: { origin: { type: "string" }, session: { type: "string" }, provider: { type: "string", default: "claude" }, executor: { type: "string" }, hand: { type: "string" }, "hand-log": { type: "string" }, label: { type: "string" }, timing: { type: "boolean", default: false } } });
if (!values.origin) throw new Error("Pass --origin with the Worker's HTTPS origin");
const origin = new URL(values.origin);
if (origin.username || origin.password || origin.search || origin.hash || origin.pathname !== "/" || origin.protocol !== "https:") throw new Error("Use an HTTPS origin");
if (!values.session || !/^[A-Za-z0-9._-]{1,128}$/.test(values.session)) throw new Error("Pass --session");
if (!["claude", "codex"].includes(values.provider)) throw new Error("provider must be claude or codex");
const token = process.env.WORKER_TOKEN;
if (!token || /[\r\n]/.test(token)) throw new Error("Provide WORKER_TOKEN through the environment");
const target = values.hand ? { hand: values.hand } : values.executor ? { executor: values.executor } : {};
const label = values.label ?? values.hand ?? values.executor ?? "no executor";
const KEEPALIVE_MS = 30000;
const sessionPath = "/sessions/" + encodeURIComponent(values.session);
let connectedMs, socket, connecting, pending, cursor = 0, warmed = false;

function sessionSocket() {
  return socket ? Promise.resolve() : connecting ??= open().finally(() => { connecting = undefined; });
}

function open() {
  const started = performance.now();
  const url = new URL(sessionPath + "/socket?after=" + cursor, origin);
  url.protocol = "wss:";
  const next = new WebSocket(url, { headers: { authorization: "Bearer " + token } });
  return new Promise((resolve, reject) => {
    next.addEventListener("open", () => { connectedMs ??= performance.now() - started; socket = next; resolve(); }, { once: true });
    next.addEventListener("error", () => reject(new Error("Worker socket failed")), { once: true });
    next.addEventListener("message", event => receive(String(event.data)));
    next.addEventListener("close", event => {
      if (socket !== next) return;
      socket = undefined;
      if (!pending) return;
      sessionSocket().then(() => pending && socket.send(pending.message), error => { pending?.reject(new Error("Worker socket closed with " + event.code + " and could not reconnect: " + error.message)); pending = undefined; });
    });
  });
}

function receive(data) {
  if (data === "pong") return;
  let message;
  try { message = JSON.parse(data); } catch { process.stderr.write("Invalid Worker message.\n"); return; }
  if (message.type === "event") {
    cursor = Math.max(cursor, message.seq);
    if (!pending || message.request_id !== pending.requestId) return;
    pending.view.envelope(message);
    if (pending.view.terminal) settle();
  } else if (message.type === "turn" && message.request_id === pending?.requestId) {
    pending.acceptedMs = performance.now() - pending.submitted;
    if (message.status === 200) { pending.view.envelope({ event: { type: "result", status: message.body.status } }); settle(); }
    else if (message.status !== 202) { pending.view.finish(); pending.reject(new Error("Worker rejected the turn with HTTP " + message.status + ": " + JSON.stringify(message.body).slice(0, 500))); pending = undefined; }
  } else if (message.type === "error") {
    process.stderr.write("Worker: " + message.error + "\n");
  }
}

function settle() {
  const { view, index, submitted, acceptedMs, resolve } = pending;
  pending = undefined;
  view.finish();
  if (values.timing) process.stderr.write(JSON.stringify({ turn: index, connection_ms: index === 1 ? connectedMs : 0, accepted_ms: acceptedMs, ttft_ms: view.firstText === undefined ? undefined : view.firstText - submitted, total_ms: performance.now() - submitted }) + "\n");
  if (view.terminal !== "completed") process.exitCode = 1;
  resolve();
}

function warm() {
  if (warmed || pending) return;
  warmed = true;
  sessionSocket().then(() => socket.send(JSON.stringify({ type: "warm" })), () => { warmed = false; });
}

async function listing() {
  const response = await fetch(new URL(sessionPath + "/hands", origin), { headers: { authorization: "Bearer " + token } });
  const body = await response.json().catch(() => { throw new Error("Invalid hand listing"); });
  return body.hands ?? [];
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

async function turn(index, prompt) {
  await sessionSocket();
  const requestId = "try-" + index;
  const message = JSON.stringify({ type: "turn", request_id: requestId, provider: values.provider, ...target, prompt });
  return new Promise((resolve, reject) => {
    pending = { index, requestId, message, view: render(), submitted: performance.now(), resolve, reject };
    socket.send(message);
  }).finally(() => { warmed = false; });
}

const keepalive = setInterval(() => socket?.send("ping"), KEEPALIVE_MS);
try {
  await sessionSocket();
  await handAttached();
  let index = 0;
  if (positionals.length === 1 && positionals[0] === "-") {
    const input = createInterface({ input: process.stdin, crlfDelay: Infinity });
    if (process.stdin.isTTY) {
      process.stdin.on("keypress", warm);
      process.stdout.write("> ");
    }
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
  socket?.close(1000, "done");
}

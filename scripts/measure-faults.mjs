import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { spawn } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { parseArgs } from "node:util";
import { GATEWAY_KEY, LOCAL_TOKEN, localOptions } from "./local-runtime.mjs";

const { values, positionals } = parseArgs({ allowPositionals: true, options: { providers: { type: "string", default: "claude,codex" }, delay: { type: "string", default: "200" }, rounds: { type: "string", default: "6" }, slow: { type: "string", default: "20000" }, seed: { type: "string", default: String(Date.now() % 100000) } } });
const PROVIDERS = values.providers.split(",");
const DELAY = Number(values.delay);
const SLOW = Number(values.slow);
const ROUNDS = Number(values.rounds);
let seed = Number(values.seed);
const random = () => { seed = (seed * 1103515245 + 12345) % 2147483648; return seed / 2147483648; };
const sleep = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds));
const TERMINAL = new Set(["completed", "failed", "cancelled"]);

const calls = [];
const holds = new Map();
const held = new Set();
const cells = {};

const sse = events => new Response(events.map(([event, data]) => (event ? `event: ${event}\n` : "") + `data: ${JSON.stringify(data)}\n\n`).join(""), { headers: { "content-type": "text/event-stream" } });
const claudeText = content => typeof content === "string" ? content : (content ?? []).filter(block => block.type === "text").map(block => block.text).join("\n");

function claudeReply(input, block) {
  const index = 0;
  const start = block.type === "text" ? { type: "text", text: "" } : { type: "tool_use", id: block.id, name: block.name, input: {} };
  const delta = block.type === "text" ? { type: "text_delta", text: block.text } : { type: "input_json_delta", partial_json: JSON.stringify(block.input) };
  return [
    ["message_start", { type: "message_start", message: { id: `message-${calls.length}`, type: "message", role: "assistant", model: input.model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 8, output_tokens: 0 } } }],
    ["content_block_start", { type: "content_block_start", index, content_block: start }],
    ["content_block_delta", { type: "content_block_delta", index, delta }],
    ["content_block_stop", { type: "content_block_stop", index }],
    ["message_delta", { type: "message_delta", delta: { stop_reason: block.type === "text" ? "end_turn" : "tool_use", stop_sequence: null }, usage: { output_tokens: 4 } }],
    ["message_stop", { type: "message_stop" }],
  ];
}

function codexReply(item) {
  const id = `resp_${calls.length}`;
  return [
    [null, { type: "response.created", response: { id, status: "in_progress" } }],
    [null, { type: "response.output_item.added", output_index: 0, item: item.type === "message" ? { ...item, status: "in_progress", content: [] } : item }],
    ...(item.type === "message" ? [[null, { type: "response.output_text.delta", output_index: 0, item_id: item.id, content_index: 0, delta: item.content[0].text }]] : []),
    [null, { type: "response.output_item.done", output_index: 0, item }],
    [null, { type: "response.completed", response: { id, status: "completed", model: "gpt-6.1-sol", output: [item], usage: { input_tokens: 8, output_tokens: 4, total_tokens: 12 } } }],
  ];
}

function completionStatus(texts) {
  const found = texts.find(value => value.includes("<subagent_completion"));
  return found === undefined ? false : /<subagent_completion [^>]*status="(\w+)"/.exec(found)?.[1] ?? true;
}

function parse(provider, input) {
  if (provider === "claude") {
    const messages = input.messages ?? [];
    const index = messages.findLastIndex(message => message.role === "user" && /turn=[\w-]+/.test(claudeText(message.content)));
    const prompt = index < 0 ? "" : claudeText(messages[index].content);
    const results = messages.slice(index + 1).flatMap(message => Array.isArray(message.content) ? message.content.filter(block => block.type === "tool_result") : []);
    const names = (input.tools ?? []).map(entry => entry.name);
    const tool = suffix => names.find(name => name === suffix || name?.endsWith(`__${suffix}`)) ?? suffix;
    const completion = completionStatus(messages.slice(index + 1).filter(message => message.role === "user").map(message => claudeText(message.content)));
    const output = results.map(block => typeof block.content === "string" ? block.content : claudeText(block.content)).join("\n");
    return { prompt, round: results.length, errors: results.filter(block => block.is_error).length, tool, completion, output };
  }
  const items = Array.isArray(input.input) ? input.input : [];
  const text = item => (Array.isArray(item.content) ? item.content : []).map(part => part.text ?? "").join("\n");
  const index = items.findLastIndex(item => item.type === "message" && item.role === "user" && /turn=[\w-]+/.test(text(item)));
  const prompt = index < 0 ? "" : text(items[index]);
  const outputs = items.slice(index + 1).filter(item => item.type === "function_call_output" || item.type === "custom_tool_call_output");
  const completion = completionStatus(items.slice(index + 1).filter(item => item.type === "message").map(text));
  return { prompt, round: outputs.length, errors: 0, tool: suffix => suffix, completion, output: outputs.map(item => typeof item.output === "string" ? item.output : Array.isArray(item.output) ? item.output.map(part => part.text ?? JSON.stringify(part)).join("\n") : JSON.stringify(item.output)).join("\n") };
}

function respond(provider, input, block) {
  return sse(provider === "claude" ? claudeReply(input, block) : codexReply(block.type === "text"
    ? { type: "message", id: `msg_${calls.length}`, role: "assistant", status: "completed", content: [{ type: "output_text", text: block.text }] }
    : block.name === "exec" ? { type: "custom_tool_call", id: `ctc_${calls.length}`, call_id: `call_${calls.length}`, name: "exec", input: block.input.code, status: "completed" }
    : { type: "function_call", id: `fc_${calls.length}`, call_id: `call_${calls.length}`, name: block.name, arguments: JSON.stringify(block.input), status: "completed" }));
}

function hold(key, provider, reply) {
  let release;
  const released = new Promise(resolve => { release = resolve; });
  holds.set(key, { release, provider, reply });
  return new Response(new ReadableStream({
    async start(controller) {
      const response = await released;
      if (!response) return;
      const reader = response.body.getReader();
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        try { controller.enqueue(value); } catch { return; }
      }
      try { controller.close(); } catch {}
    },
  }), { headers: { "content-type": "text/event-stream" } });
}

const provider = globalThis.faultProvider = async request => {
  const url = new URL(request.url);
  const kind = url.pathname === "/openai/v1/responses" ? "codex" : "claude";
  if (url.hostname !== "provider.invalid") throw new Error("model request escaped the gateway");
  const body = await request.text();
  if (body.includes(GATEWAY_KEY)) throw new Error("a provider credential entered model input");
  const input = JSON.parse(body);
  const { prompt, round, errors, tool, completion, output } = parse(kind, input);
  const turn = /turn=([\w-]+)/.exec(prompt)?.[1] ?? "unknown";
  const entry = { turn, provider: kind, round, errors, at: performance.now(), ...(completion ? { completion } : {}), ...(output ? { output: output.slice(0, 2000) } : {}) };
  calls.push(entry);
  const slow = /\[slow:(\d+)\]/.exec(prompt);
  if (slow) await sleep(Number(slow[1]));
  const use = (name, value) => ({ type: "tool_use", id: `toolu_${calls.length}`, name: tool(name), input: value });
  let block = completion ? { type: "text", text: `completion for ${turn}` }
    : prompt.includes("[tool]") && round === 0 ? use("list_agents", kind === "claude" ? { include_completed: true } : {})
    : prompt.includes("[list]") && round === 0 ? use("list_agents", { include_completed: true })
    : prompt.includes("[wait]") && round === 0 ? use("wait_agent", { agent_ids: [1], timeout_ms: 1000 })
    : prompt.includes("[spawn]") && round === 0 ? use("spawn_agent", { role: "background child", task: `turn=kid-${turn} [child] [hold]${prompt.includes("[large]") ? ` ${"Context the child needs for its task. ".repeat(600)}` : ""}`, harness: null, model: null, thinking: null, reasoning_mode: null, output_contract: { kind: "string" } })
    : prompt.includes("[child]") && round === 0 ? use("submit_result", { output: "child-ok" })
    : prompt.includes("[exec]") && round === 0 ? { type: "tool_use", id: `toolu_${calls.length}`, name: "exec", input: { code: cells[turn] } }
    : { type: "text", text: `reply to ${turn}` };
  const offered = kind === "claude" ? (input.tools ?? []).map(entry => entry.name) : [...(input.tools ?? []), ...(Array.isArray(input.input) ? input.input : []).filter(item => item.type === "additional_tools").flatMap(item => item.tools ?? [])].map(entry => entry.name);
  if (block.type === "tool_use" && block.name !== "exec" && !offered.includes(block.name) && offered.includes("exec")) block = { type: "tool_use", id: block.id, name: "exec", input: { code: "text(JSON.stringify(await tools." + block.name + "(" + JSON.stringify(block.input) + ")));" } };
  const holdHere = !completion && ((prompt.includes("[hold]") && round === 0) || (prompt.includes("[hold-after-tool]") && round === 1));
  const key = `${turn}:${round}`;
  if (holdHere && !held.has(key)) {
    held.add(key);
    entry.held = true;
    return hold(key, kind, () => respond(kind, input, block));
  }
  return respond(kind, input, block);
};

const baseOptions = localOptions({ provider, agent: { RECOVERY_DELAY_MS: String(DELAY) } });
let mf;
let generation = 0;
const authorization = `Bearer ${LOCAL_TOKEN}`;

const PORT = 20000 + Math.floor(Math.random() * 20000);
function bundleOptions(bundle) {
  const wasm = process.env.NANOCODE_WASM_PATH;
  delete process.env.NANOCODE_WASM_PATH;
  try { return localOptions({ provider, agent: { RECOVERY_DELAY_MS: String(DELAY) }, bundle, persistence: baseOptions.resourcePersistencePath }); }
  finally { if (wasm !== undefined) process.env.NANOCODE_WASM_PATH = wasm; }
}

async function boot(bundle) {
  mf = new Miniflare({ ...convertV4MiniflareOptions(bundle ? bundleOptions(bundle) : baseOptions), port: PORT });
  await mf.ready;
  generation++;
}

async function restart(bundle) {
  const started = performance.now();
  const previous = mf;
  mf = undefined;
  await previous.dispose();
  for (const [key, entry] of holds) { entry.release(undefined); holds.delete(key); }
  await boot(bundle);
  for (const watcher of watchers) await watcher.reconnect();
  return performance.now() - started;
}

async function request(url, init = {}) {
  for (let attempt = 0; ; attempt++) {
    while (!mf) await sleep(5);
    try { return await mf.dispatchFetch(url, { ...init, headers: { authorization, "content-type": "application/json", ...init.headers } }); }
    catch (error) { if (attempt > 50) throw error; await sleep(20); }
  }
}

const sessionUrl = name => `http://worker/sessions/${name}`;
const objectName = name => JSON.stringify(["owner", name]);
async function sql(name, query, ...bindings) {
  for (let attempt = 0; ; attempt++) {
    while (!mf) await sleep(5);
    try { return await (await mf.unsafeGetDurableObjectStorage("nanocode", "AgentSession", { name: objectName(name) })).exec(query, ...bindings); }
    catch (error) { if (attempt > 50) throw error; await sleep(20); }
  }
}

async function submit(session, id, prompt, extra = {}) {
  const response = await request(`${sessionUrl(session.name)}/turns`, { method: "POST", body: JSON.stringify({ request_id: id, prompt: `turn=${id} ${prompt}`, provider: session.provider, ...extra }) });
  return { status: response.status, body: await response.json().catch(() => null) };
}

async function status(session, id) {
  const response = await request(`${sessionUrl(session.name)}/turns/${encodeURIComponent(id)}`);
  return response.status === 200 ? response.json() : { status: `http ${response.status}` };
}

async function cancel(session, id) {
  const response = await request(`${sessionUrl(session.name)}/turns/${encodeURIComponent(id)}/cancel`, { method: "POST" });
  return { status: response.status, body: await response.json().catch(() => null) };
}

const metrics = async session => (await request(`${sessionUrl(session.name)}/metrics`)).json();

async function until(condition, detail, timeout = 30000) {
  const started = performance.now();
  for (;;) {
    const value = await condition();
    if (value) return value;
    if (performance.now() - started > timeout) throw new Error(`timed out: ${detail}`);
    await sleep(10);
  }
}

async function settled(session, id, timeout) {
  let last;
  await until(async () => { last = await status(session, id); return TERMINAL.has(last.status); }, `turn ${id} did not finish (last ${JSON.stringify(last)})`, timeout);
  return last;
}

const watchers = new Set();
async function watch(session) {
  const watcher = { session, events: [], duplicates: [], gaps: [], socket: undefined, connects: 0 };
  watcher.connect = async () => {
    const after = watcher.events.at(-1)?.seq ?? 0;
    const response = await request(`${sessionUrl(session.name)}/socket?after=${after}`, { headers: { upgrade: "websocket" } });
    const socket = response.webSocket;
    if (!socket) throw new Error(`watcher rejected with ${response.status}`);
    watcher.connects++;
    socket.addEventListener("message", event => {
      if (watcher.socket !== socket) return;
      const data = String(event.data);
      if (data === "pong") return;
      const message = JSON.parse(data);
      if (message.type !== "event") return;
      const last = watcher.events.at(-1)?.seq ?? 0;
      if (message.seq <= last) watcher.duplicates.push({ seq: message.seq, last });
      else {
        if (message.seq !== last + 1) watcher.gaps.push({ from: last, to: message.seq });
        watcher.events.push({ seq: message.seq, request_id: message.request_id, type: message.event?.type ?? message.event?.source });
      }
    });
    socket.accept();
    watcher.socket = socket;
  };
  watcher.reconnect = async () => { const previous = watcher.socket; watcher.socket = undefined; try { previous?.close(1000, "reconnect"); } catch {} await watcher.connect(); };
  watcher.close = () => { watchers.delete(watcher); try { watcher.socket?.close(1000, "done"); } catch {} };
  await watcher.connect();
  watchers.add(watcher);
  return watcher;
}

async function stream(session, id) {
  const collected = { events: [], reconnects: 0, done: false, error: undefined };
  const run = async () => {
    for (;;) {
      const after = collected.events.at(-1)?.seq ?? 0;
      try {
        const response = await request(`${sessionUrl(session.name)}/turns?after=${after}`, { method: "POST", headers: { accept: "text/event-stream" }, body: JSON.stringify({ request_id: id, prompt: `turn=${id} ${collected.prompt}`, provider: session.provider }) });
        if (response.status >= 300) { collected.error = `stream answered ${response.status}`; return; }
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          let index;
          while ((index = buffer.indexOf("\n\n")) >= 0) {
            const frame = buffer.slice(0, index);
            buffer = buffer.slice(index + 2);
            const data = frame.split("\n").find(line => line.startsWith("data: "));
            if (!data) continue;
            const event = JSON.parse(data.slice(6));
            const last = collected.events.at(-1)?.seq ?? 0;
            if (event.seq <= last) collected.duplicate = true;
            collected.events.push({ seq: event.seq, request_id: event.request_id, type: event.event?.type ?? event.event?.source });
            if (event.event?.type === "result") { collected.done = true; }
          }
        }
        if (collected.done) return;
      } catch {}
      collected.reconnects++;
      await sleep(20);
    }
  };
  collected.start = prompt => { collected.prompt = prompt; collected.running = run(); return collected; };
  return collected;
}

function operationStatus(value) {
  if (typeof value === "string") return value;
  if (value && typeof value === "object") return Object.keys(value)[0];
  return null;
}

async function operations(name) {
  const [root] = await sql(name, "SELECT value FROM metadata WHERE key = 'session_id'");
  if (!root) return {};
  const [state] = await sql(name, "SELECT payload FROM states WHERE id = ?", root.value);
  if (!state) return {};
  const found = {};
  const pages = [];
  const collect = operations => {
    for (const [id, operation] of Object.entries(operations)) found[id] = { status: operationStatus(operation.status), order: operation.accepted_order, steps: Object.entries(operation.steps ?? {}).map(([key, step]) => ({ key, kind: step.kind, status: operationStatus(step.status), attempts: step.attempts })) };
  };
  const visit = value => {
    if (!value || typeof value !== "object") return;
    if (value.operations && typeof value.operations === "object" && !Array.isArray(value.operations)) {
      collect(value.operations);
      pages.push(...(value.sealed?.levels ?? []).flat());
      return;
    }
    for (const child of Object.values(value)) visit(child);
  };
  try { visit(JSON.parse(state.payload)); } catch { return { unreadable: true }; }
  while (pages.length) {
    const [record] = await sql(name, "SELECT value FROM blobs WHERE state_id = ?1 AND key = ?2 UNION ALL SELECT value FROM records WHERE state_id = ?1 AND key = ?2", root.value, pages.shift());
    if (!record?.value?.startsWith("=")) return { unreadable: true };
    const page = JSON.parse(record.value.slice(1));
    collect(page.operations ?? {});
    pages.push(...(page.pages ?? []));
  }
  return found;
}

const findings = [];
const checks = [];
function check(scenario, condition, detail, evidence) {
  checks.push({ scenario, check: detail, ok: !!condition, ...(condition ? {} : { evidence }) });
  if (!condition) findings.push({ scenario, check: detail, evidence });
  return !!condition;
}

async function invariants(scenario, session, { quiescent = true } = {}) {
  const name = session.name;
  const turns = await sql(name, "SELECT request_id, status, CAST(first_seq AS INTEGER) AS first_seq, last_seq, cancel_requested, note, retry FROM turns WHERE scope = 'owner' ORDER BY first_seq");
  const events = await sql(name, "SELECT seq, request_id, json_extract(body, '$.type') AS type, json_extract(body, '$.status') AS status, json_extract(body, '$.source') AS source FROM events ORDER BY seq");
  const ops = await operations(name);
  const gaps = events.filter((event, index) => event.seq !== (index === 0 ? events[0].seq : events[index - 1].seq + 1));
  const unattributed = await sql(name, "SELECT seq, json_extract(body, '$.source') AS source, json_extract(body, '$.event.type') AS type, json_extract(body, '$.update.type') AS update_type FROM events WHERE request_id IS NULL ORDER BY seq");
  report.unattributed[name] = unattributed.map(event => `${event.source}:${event.type ?? event.update_type}`).reduce((counts, key) => ({ ...counts, [key]: (counts[key] ?? 0) + 1 }), {});
  check(scenario, unattributed.every(event => event.source !== "agent"), `${name}: every root agent event is attributed to a turn`, unattributed.filter(event => event.source === "agent").slice(0, 5));
  check(scenario, gaps.length === 0, `${name}: event sequence is contiguous`, gaps.slice(0, 5));
  for (const turn of turns) {
    const own = events.filter(event => event.request_id === turn.request_id);
    const results = own.filter(event => event.type === "result");
    const queued = own.filter(event => event.type === "queued");
    if (TERMINAL.has(turn.status)) {
      check(scenario, results.length === 1 && results[0].seq === turn.last_seq && results[0].status === turn.status, `${name}/${turn.request_id}: one result event at last_seq matching its status`, { status: turn.status, last_seq: turn.last_seq, results });
      check(scenario, own.every(event => event.seq >= turn.first_seq && event.seq <= turn.last_seq), `${name}/${turn.request_id}: attributed events lie within its range`, own.filter(event => event.seq < turn.first_seq || event.seq > turn.last_seq).slice(0, 5));
    } else check(scenario, results.length === 0, `${name}/${turn.request_id}: unfinished turn has no result event`, results);
    check(scenario, queued.length === 1, `${name}/${turn.request_id}: one queued event`, queued.length);
    const operation = ops[turn.request_id];
    if (turn.status === "completed") check(scenario, operation?.status === "completed", `${name}/${turn.request_id}: SDK operation completed with the log`, operation);
    if (turn.status === "cancelled") check(scenario, !operation || operation.status === "cancelled", `${name}/${turn.request_id}: SDK operation cancelled with the log`, operation);
    if (turn.status === "failed") check(scenario, !operation || ["failed", "cancelled"].includes(operation.status), `${name}/${turn.request_id}: SDK operation not left pending under a failed turn`, operation);
  }
  const pendingOps = Object.entries(ops).filter(([, operation]) => operation.status === "pending").map(([id]) => id);
  if (quiescent) {
    check(scenario, turns.every(turn => TERMINAL.has(turn.status)), `${name}: every turn finished`, turns.filter(turn => !TERMINAL.has(turn.status)));
    check(scenario, pendingOps.length === 0, `${name}: no SDK operation pending`, pendingOps);
    const quiet = await until(async () => {
      const [marker] = await sql(name, "SELECT COUNT(*) AS count FROM metadata WHERE key LIKE 'background:%'");
      const current = await metrics(session);
      return marker.count === 0 && current.alarm_at === null ? true : undefined;
    }, "quiescence", 10 * DELAY + 3000).catch(() => false);
    const [marker] = await sql(name, "SELECT COUNT(*) AS count FROM metadata WHERE key LIKE 'background:%'");
    check(scenario, quiet, `${name}: background marker and alarm cleared once quiescent`, { marker: marker.count, alarm_at: (await metrics(session)).alarm_at });
  }
  for (const watcher of watchers) {
    if (watcher.session !== session) continue;
    const latest = events.at(-1)?.seq ?? 0;
    await until(async () => (watcher.events.at(-1)?.seq ?? 0) >= latest, "watcher catch-up", 5000).catch(() => undefined);
    const seen = watcher.events.map(event => event.seq);
    const missing = events.map(event => event.seq).filter(seq => !seen.includes(seq));
    check(scenario, watcher.duplicates.length === 0 && watcher.gaps.length === 0 && missing.length === 0, `${name}: socket watcher saw every event once in order across ${watcher.connects} connections`, { duplicates: watcher.duplicates.slice(0, 5), gaps: watcher.gaps.slice(0, 5), missing: missing.slice(0, 10), latest, seen: seen.at(-1) });
  }
  return { turns, events, ops };
}

const modelCalls = (turn, round) => calls.filter(call => call.turn === turn && (round === undefined || call.round === round)).length;

let sessions = 0;
async function newSession(provider, label) {
  return { name: `faults-${label}-${provider}-${Date.now()}-${sessions++}`, provider };
}

const scenarios = {
  async slowEvict(provider) {
    const session = await newSession(provider, "slow-evict");
    const first = `se0-${provider}`;
    await submit(session, first, "warm up");
    await settled(session, first);
    await restart();
    const id = `se-${provider}`;
    const response = await request(`${sessionUrl(session.name)}/turns`, { method: "POST", headers: { accept: "text/event-stream" }, body: JSON.stringify({ request_id: id, prompt: `turn=${id} [slow:3000]`, provider }) });
    let text = "", ended;
    const reading = (async () => { const reader = response.body.getReader(); const decoder = new TextDecoder(); try { for (;;) { const next = await reader.read(); if (next.done) break; text += decoder.decode(next.value, { stream: true }); } ended = "done"; } catch (error) { ended = `error: ${error?.message ?? error}`; } })();
    await until(() => modelCalls(id) > 0, "slow call started");
    await sleep(500);
    const started = modelCalls(id);
    await restart();
    await Promise.race([reading, sleep(15000)]);
    check("slowEvict", ended !== undefined, `${provider}: the turn's stream ended at the restart`, ended);
    check("slowEvict", text.includes("model.call.started") && !text.includes('"type":"result"'), `${provider}: the stream carried model.call.started and no result`, text.slice(-400));
    const done = await settled(session, id);
    check("slowEvict", done.status === "completed", `${provider}: the turn interrupted before its model call answered recovered`, done);
    check("slowEvict", started === 1 && modelCalls(id) === 2, `${provider}: the unanswered model call repeated once`, modelCalls(id));
    const page = await (await request(`${sessionUrl(session.name)}/events?after=0&limit=128`)).json();
    const result = (page.events ?? []).filter(entry => entry.request_id === id && entry.event?.type === "result").at(-1);
    check("slowEvict", result?.event?.recovered === true || JSON.stringify(result ?? {}).includes('"recovered":true'), `${provider}: the recovered turn's result says recovered`, result);
    await invariants("slowEvict", session);
    return { ended };
  },

  async slowCold(provider) {
    const session = await newSession(provider, "slow-cold");
    const first = `sc0-${provider}`;
    await submit(session, first, "warm up");
    await settled(session, first);
    await restart();
    const id = `sc-${provider}`;
    const started = performance.now();
    const response = await request(`${sessionUrl(session.name)}/turns`, { method: "POST", headers: { accept: "text/event-stream" }, body: JSON.stringify({ request_id: id, prompt: `turn=${id} [slow:${SLOW}]`, provider }) });
    const text = await response.text();
    check("slowCold", text.includes('"type":"result"') && text.includes('"status":"completed"'), `${provider}: a cold turn whose model answers after ${SLOW} ms streams its result`, text.slice(-400));
    check("slowCold", modelCalls(id) === 1, `${provider}: the slow model call ran once`, modelCalls(id));
    await invariants("slowCold", session);
    return { milliseconds: Math.round(performance.now() - started) };
  },

  async basic(provider) {
    const session = await newSession(provider, "basic");
    const watcher = await watch(session);
    for (const id of ["b1", "b2", "b3"].map(id => `${id}-${provider}`)) {
      const submitted = await submit(session, id, id.startsWith("b2") ? "[tool]" : "plain");
      check("basic", submitted.status === 202, `${provider}/${id}: accepted`, submitted);
      const done = await settled(session, id);
      check("basic", done.status === "completed", `${provider}/${id}: completed`, done);
    }
    check("basic", modelCalls(`b1-${provider}`) === 1 && modelCalls(`b2-${provider}`) === 2 && modelCalls(`b3-${provider}`) === 1, `${provider}: one model call per round`, { b1: modelCalls(`b1-${provider}`), b2: modelCalls(`b2-${provider}`), b3: modelCalls(`b3-${provider}`) });
    await invariants("basic", session);
    watcher.close();
  },

  async evictModel(provider) {
    const session = await newSession(provider, "evict-model");
    const watcher = await watch(session);
    const id = `em-${provider}`;
    await submit(session, id, "[hold]");
    await until(() => holds.has(`${id}:0`), "held model call");
    await restart();
    const done = await settled(session, id);
    check("evictModel", done.status === "completed", `${provider}: interrupted model call recovered`, done);
    check("evictModel", modelCalls(id) === 2, `${provider}: interrupted model call repeated once`, modelCalls(id));
    await invariants("evictModel", session);
    watcher.close();
  },

  async evictAfterTool(provider) {
    const session = await newSession(provider, "evict-tool");
    const watcher = await watch(session);
    const id = `et-${provider}`;
    await submit(session, id, "[tool] [hold-after-tool]");
    await until(() => holds.has(`${id}:1`), "held call after the tool");
    await restart();
    const done = await settled(session, id);
    check("evictAfterTool", done.status === "completed", `${provider}: turn after a recorded tool call recovered`, done);
    check("evictAfterTool", modelCalls(id, 0) === 1, `${provider}: model call before the tool was not repeated`, modelCalls(id, 0));
    check("evictAfterTool", modelCalls(id, 1) === 2, `${provider}: held call after the tool repeated once`, modelCalls(id, 1));
    await invariants("evictAfterTool", session);
    watcher.close();
  },

  async faults(provider) {
    const plans = [
      { label: "start", trigger: "BEFORE INSERT ON events WHEN json_extract(NEW.body, '$.type') = 'running'", calls: 1 },
      { label: "agent-events", trigger: "BEFORE INSERT ON events WHEN json_extract(NEW.body, '$.source') = 'agent'", calls: [1, 2] },
      { label: "result", trigger: "BEFORE INSERT ON events WHEN json_extract(NEW.body, '$.type') = 'result'", calls: 1 },
      { label: "checkpoint", trigger: "BEFORE UPDATE ON states", calls: [0, 1] },
      { label: "marker", trigger: "BEFORE DELETE ON metadata WHEN OLD.key LIKE 'background:%'", calls: 1 },
      { label: "submit", trigger: "BEFORE INSERT ON events WHEN json_extract(NEW.body, '$.type') = 'queued'", calls: 1, submit: true },
    ];
    for (const plan of plans) {
      const session = await newSession(provider, `fault-${plan.label}`);
      const watcher = await watch(session);
      const warm = `fw-${plan.label}-${provider}`;
      await submit(session, warm, "plain");
      await settled(session, warm);
      const id = `f-${plan.label}-${provider}`;
      await sql(session.name, `CREATE TRIGGER fault_injection ${plan.trigger} BEGIN SELECT RAISE(ABORT, 'injected fault'); END`);
      const first = await submit(session, id, "plain");
      if (plan.submit) check("faults", first.status === 503, `${provider}/${plan.label}: failed submission answered 503`, first);
      await sleep(4 * DELAY);
      const before = await status(session, id);
      await sql(session.name, "DROP TRIGGER fault_injection");
      if (plan.submit) {
        check("faults", before.status === "http 404", `${provider}/${plan.label}: failed submission left no turn`, before);
        const retried = await submit(session, id, "plain");
        check("faults", retried.status === 202, `${provider}/${plan.label}: resubmission accepted`, retried);
      }
      const done = await settled(session, id);
      check("faults", done.status === "completed", `${provider}/${plan.label}: turn completed after the fault cleared`, { before, done });
      const count = modelCalls(id);
      const allowed = Array.isArray(plan.calls) ? plan.calls : [plan.calls];
      check("faults", count >= Math.min(...allowed) && count <= Math.max(...allowed), `${provider}/${plan.label}: model calls ${allowed.join(" or ")}`, count);
      const next = `fn-${plan.label}-${provider}`;
      await submit(session, next, "plain");
      const after = await settled(session, next);
      check("faults", after.status === "completed", `${provider}/${plan.label}: next turn completed`, after);
      await invariants("faults", session);
      watcher.close();
    }
  },

  async concurrent(provider) {
    const session = await newSession(provider, "concurrent");
    const watcher = await watch(session);
    const same = await Promise.all(Array.from({ length: 8 }, () => submit(session, `same-${provider}`, "[slow:50]")));
    check("concurrent", same.every(entry => entry.status === 202 || entry.status === 200), `${provider}: concurrent duplicate submissions accepted`, same.map(entry => entry.status));
    const ids = Array.from({ length: 8 }, (_, index) => `diff-${index}-${provider}`);
    const different = await Promise.all(ids.map(id => submit(session, id, "[slow:20]")));
    check("concurrent", different.every(entry => entry.status === 202), `${provider}: concurrent distinct submissions accepted`, different.map(entry => entry.status));
    for (const id of [`same-${provider}`, ...ids]) {
      const done = await settled(session, id);
      check("concurrent", done.status === "completed", `${provider}/${id}: completed`, done);
    }
    check("concurrent", modelCalls(`same-${provider}`) === 1, `${provider}: duplicate submissions ran once`, modelCalls(`same-${provider}`));
    const { turns, ops } = await invariants("concurrent", session);
    const order = turns.map(turn => turn.request_id).filter(id => ops[id]);
    const accepted = [...order].sort((left, right) => ops[left].order - ops[right].order);
    check("concurrent", JSON.stringify(order) === JSON.stringify(accepted), `${provider}: turns ran in submission order`, { log: order, sdk: accepted });
    const firstCalls = ids.map(id => calls.find(call => call.turn === id)?.at ?? Infinity);
    check("concurrent", firstCalls.every((at, index) => index === 0 || at >= firstCalls[index - 1]), `${provider}: root turns ran serially in order`, firstCalls);
    watcher.close();
  },

  async cancelRace(provider) {
    const session = await newSession(provider, "cancel-race");
    const watcher = await watch(session);
    const answers = [];
    for (let round = 0; round < ROUNDS; round++) {
      const id = `cs-${round}-${provider}`;
      const [submitted, cancelled] = await Promise.all([submit(session, id, "[slow:30]"), (async () => { await sleep(Math.floor(random() * 40)); return cancel(session, id); })()]);
      answers.push({ id, submitted: submitted.status, cancelled: cancelled.status });
    }
    for (let round = 0; round < ROUNDS; round++) {
      const id = `cf-${round}-${provider}`;
      await submit(session, id, "[slow:60]");
      await until(() => modelCalls(id) > 0, "slow call started");
      await sleep(50 + Math.floor(random() * 30));
      const cancelled = await cancel(session, id);
      answers.push({ id, cancelled: cancelled.status, status: cancelled.body?.status });
      const done = await settled(session, id);
      const next = `cn-${round}-${provider}`;
      await submit(session, next, "plain");
      const after = await settled(session, next);
      check("cancelRace", after.status === "completed", `${provider}/${next}: turn after a cancel racing finish completed`, { cancelled: done.status, after });
    }
    for (let round = 0; round < ROUNDS; round++) await settled(session, `cs-${round}-${provider}`);
    const last = `cl-${provider}`;
    await submit(session, last, "plain");
    const final = await settled(session, last);
    check("cancelRace", final.status === "completed", `${provider}: final turn completed`, final);
    await invariants("cancelRace", session);
    watcher.close();
    return answers;
  },

  async cancelFinish(provider) {
    const session = await newSession(provider, "cancel-finish");
    const watcher = await watch(session);
    const outcomes = {};
    for (let round = 0; round < ROUNDS; round++) {
      const id = `cr-${round}-${provider}`;
      await submit(session, id, "[hold]");
      await until(() => holds.has(`${id}:0`), "held model call");
      const entry = holds.get(`${id}:0`);
      const releaseAt = Math.floor(random() * 4);
      const cancelAt = Math.floor(random() * 4);
      const [, cancelled] = await Promise.all([
        (async () => { await sleep(releaseAt); holds.delete(`${id}:0`); entry.release(entry.reply()); })(),
        (async () => { await sleep(cancelAt); return cancel(session, id); })(),
      ]);
      const done = await settled(session, id);
      outcomes[done.status] = (outcomes[done.status] ?? 0) + 1;
      const next = `crn-${round}-${provider}`;
      const started = performance.now();
      await submit(session, next, "plain");
      const after = await settled(session, next, 20000).catch(error => ({ status: String(error) }));
      check("cancelFinish", after.status === "completed" && modelCalls(next) === 1, `${provider}/${next}: the turn after a cancel racing completion runs normally`, { cancelled: cancelled.status, previous: done.status, after: after.status, error: after.error, calls: modelCalls(next), milliseconds: Math.round(performance.now() - started) });
    }
    await invariants("cancelFinish", session);
    watcher.close();
    return outcomes;
  },

  async retryColumn(provider) {
    const session = await newSession(provider, "retry-column");
    const watcher = await watch(session);
    const first = `rc-first-${provider}`;
    await submit(session, first, "plain");
    await settled(session, first);
    await sql(session.name, "ALTER TABLE turns DROP COLUMN retry");
    await sql(session.name, "DELETE FROM nanocode_formats WHERE component = 'session_log'");
    const dropped = await sql(session.name, "SELECT COUNT(*) AS count FROM pragma_table_info('turns') WHERE name = 'retry'");
    check("retryColumn", dropped[0].count === 0, `${provider}: the turns table lost its retry column`, dropped);
    await mf.unsafeEvictDurableObject("nanocode", "AgentSession", { name: objectName(session.name) });
    await watcher.reconnect();
    const resubmitted = await submit(session, first, "plain");
    check("retryColumn", resubmitted.status === 200 && resubmitted.body?.status === "completed", `${provider}: a finished turn reads back on a table without the retry column`, resubmitted);
    const waiting = `rc-hand-${provider}`;
    const submitted = await submit(session, waiting, "plain", { hand: "absent" });
    check("retryColumn", submitted.status === 202, `${provider}: a turn naming an absent hand is accepted`, submitted);
    const retried = await until(async () => { const current = await status(session, waiting); return current.kind ? current : undefined; }, "retry recorded");
    check("retryColumn", retried.status === "queued" && retried.kind === "detached" && retried.attempts >= 1 && typeof retried.retry_at === "number", `${provider}: the retry is recorded after the column migration`, retried);
    const cancelled = await cancel(session, waiting);
    check("retryColumn", cancelled.status === 200 && cancelled.body?.status === "cancelled", `${provider}: the waiting turn cancels`, cancelled);
    const last = `rc-last-${provider}`;
    await submit(session, last, "plain");
    const done = await settled(session, last);
    check("retryColumn", done.status === "completed", `${provider}: a later turn completes`, done);
    await invariants("retryColumn", session);
    watcher.close();
  },

  async background(provider) {
    for (const size of ["small", "large"]) await backgroundRun(provider, size);
  },

  async chaos(provider) {
    return chaosRun(provider);
  },

  async warm(provider) {
    return warmRun(provider);
  },

  async retention(provider) {
    return retentionRun(provider);
  },

  async destroy(provider) {
    return destroyRun(provider);
  },

  async cell(provider) {
    return cellRun(provider);
  },

  async cancelHeld(provider) {
    return cancelHeldRun(provider);
  },

  async binding(provider) {
    return bindingRun(provider);
  },

  async replyOrder(provider) {
    return replyOrderRun(provider);
  },

  async pumpFault(provider) {
    return pumpFaultRun(provider);
  },

  async legacy(provider) {
    const bundle = process.env.FAULTS_LEGACY_BUNDLE;
    if (!bundle) throw new Error("set FAULTS_LEGACY_BUNDLE to an output/deploy bundle built from an SDK between b22a444ae and 405fff643");
    const outcomes = {};
    for (const size of ["small", "large"]) for (const manifests of ["kept", "lost"]) outcomes[`${size}/${manifests}`] = await legacyRun(provider, size, manifests, bundle);
    return outcomes;
  },
};

async function backgroundRun(provider, size) {
  const session = await newSession(provider, `background-${size}`);
  const watcher = await watch(session);
  const root = `bg-${size}-${provider}`;
  const child = `kid-${root}`;
  const label = `${provider}/${size}`;
  await submit(session, root, size === "large" ? "[spawn] [large]" : "[spawn]");
  await until(() => holds.has(`${child}:0`), "child held in inference");
  const done = await settled(session, root);
  check("background", done.status === "completed", `${label}: root turn completed while its child runs`, done);
  await sleep(3 * DELAY);
  const [marker] = await sql(session.name, "SELECT COUNT(*) AS count FROM metadata WHERE key LIKE 'background:%'");
  const armed = await metrics(session);
  check("background", marker.count === 1 && armed.alarm_at !== null, `${label}: background marker and alarm stay while the child runs`, { marker: marker.count, alarm_at: armed.alarm_at });
  const before = calls.length;
  await restart();
  const resumed = await until(() => calls.some(call => call.turn === root && call.completion), "completion turn after restart", 8000).then(() => true, () => false);
  if (!resumed) {
    const probe = `bgp-${size}-${provider}`;
    await submit(session, probe, "[tool]");
    await settled(session, probe).catch(() => undefined);
    const current = await metrics(session);
    check("background", false, `${label}: the interrupted child resumed and the root ran its completion turn after a restart`, { child_calls_after_restart: calls.slice(before).filter(call => call.turn === child).length, list_agents: calls.find(call => call.turn === probe && call.round === 1)?.output, alarms: current.alarms, alarm_at: current.alarm_at, runtime_runs: current.runtime?.runs });
    watcher.close();
    return;
  }
  const cleared = await until(async () => (await sql(session.name, "SELECT COUNT(*) AS count FROM metadata WHERE key LIKE 'background:%'"))[0].count === 0, "background marker cleared", 20000).then(() => true, () => false);
  check("background", cleared, `${label}: background marker cleared after the child and the completion turn`);
  const childCalls = calls.slice(before).filter(call => call.turn === child);
  check("background", childCalls.length >= 1 && childCalls.length <= 3, `${label}: the child resumed with its interrupted call and the recovery steer`, childCalls.map(({ round, errors }) => ({ round, errors })));
  check("background", calls.filter(call => call.turn === root && call.completion).length === 1, `${label}: root ran one completion turn`, calls.filter(call => call.turn === root).map(({ round, completion }) => ({ round, completion })));
  check("background", modelCalls(root, 0) === 1, `${label}: root spawn call was not repeated`, modelCalls(root, 0));
  const after = `bga-${size}-${provider}`;
  await submit(session, after, "plain");
  const final = await settled(session, after);
  check("background", final.status === "completed" && modelCalls(after) === 1, `${label}: a later turn completes once`, final);
  await invariants("background", session);
  watcher.close();
}

function patched(patch) {
  const options = localOptions({ provider: async request => globalThis.faultProvider(request), agent: { RECOVERY_DELAY_MS: String(DELAY) } });
  const shim = options.workers[0].modules[0];
  const original = "this.sqlTotals = sql;";
  if (!shim.contents.includes(original)) throw new Error("the local shim changed; update the fault probes");
  shim.contents = shim.contents.replace(original, `${original}\n${patch}`);
  return new Miniflare(convertV4MiniflareOptions(options));
}

function client(local, name) {
  const call = (path, init = {}) => local.dispatchFetch(`http://worker/sessions/${name}${path}`, { ...init, headers: { authorization, "content-type": "application/json", ...init.headers } });
  const post = async (id, prompt, extra = {}) => { const response = await call("/turns", { method: "POST", body: JSON.stringify({ request_id: id, prompt: `turn=${id} ${prompt}`, ...extra }) }); return { status: response.status, body: await response.json() }; };
  const read = async id => (await call(`/turns/${encodeURIComponent(id)}`)).json();
  const finished = async id => { let last; await until(async () => { last = await read(id); return TERMINAL.has(last.status); }, `turn ${id} did not finish (last ${JSON.stringify(last)})`); return last; };
  return { call, post, read, finished };
}

async function cancelHeldRun(provider) {
  const local = patched(`
    const instance = this.instance.bind(this);
    this.instance = (...args) => instance(...args).then(runtime => {
      if (runtime.cancelHeld) return runtime;
      runtime.cancelHeld = true;
      const cancel = runtime.cancel.bind(runtime);
      runtime.cancel = async (scope, id, fields) => {
        const answer = await cancel(scope, id, fields);
        if (answer.turn?.report.status !== "running") return answer;
        for (let waited = 0; waited < 10000; waited += 5) {
          if ((await runtime.pending(scope)).some(turn => turn.report.request_id !== id && turn.report.status === "running")) break;
          await new Promise(resolve => setTimeout(resolve, 5));
        }
        return answer;
      };
      return runtime;
    });`);
  const outcomes = [];
  try {
    await local.ready;
    const { call, post, finished } = client(local, `faults-cancel-held-${provider}-${Date.now()}`);
    for (let round = 0; round < ROUNDS; round++) {
      const held = `chx-${round}-${provider}`;
      const next = `chy-${round}-${provider}`;
      await post(held, "[hold]", { provider });
      await until(() => holds.has(`${held}:0`), "held model call");
      const cancelling = call(`/turns/${encodeURIComponent(held)}/cancel`, { method: "POST" });
      await post(next, "[tool] [slow:100]", { provider });
      const answered = await cancelling;
      const cancelled = await finished(held);
      const done = await finished(next);
      const outcome = { round, cancel_status: answered.status, cancelled: cancelled.status, next: done.status, next_error: done.error, next_calls: modelCalls(next) };
      outcomes.push(outcome);
      check("cancelHeld", cancelled.status === "cancelled" && done.status === "completed" && modelCalls(next) === 2, `${provider}/round ${round}: a turn that starts while a cancel reply is held runs its model calls`, outcome);
    }
  } finally { await local.dispose(); }
  return outcomes;
}

async function bindingRun(provider) {
  const other = provider === "claude" ? "codex" : "claude";
  const local = patched(`
    const instance = this.instance.bind(this);
    const cold = new Promise(resolve => setTimeout(resolve, 100));
    this.instance = (...args) => cold.then(() => instance(...args));`);
  const outcomes = [];
  try {
    await local.ready;
    for (let round = 0; round < ROUNDS; round++) {
      for (const [field, values] of [["provider", [provider, other]], ["executor", ["alpha", "beta"]]]) {
        const name = `faults-binding-${field}-${round}-${provider}-${Date.now()}`;
        const session = client(local, name);
        const ids = values.map((_, index) => `bd-${field}-${round}-${index}-${provider}`);
        const answers = await Promise.all(values.map((value, index) => session.post(ids[index], "plain", { provider, [field]: value })));
        const accepted = answers.map((answer, index) => answer.status === 202 ? index : -1).filter(index => index >= 0);
        const storage = await local.unsafeGetDurableObjectStorage("nanocode", "AgentSession", { name: objectName(name) });
        const [bound] = await storage.exec("SELECT value FROM metadata WHERE key = ?", field === "provider" ? "model_provider:owner" : "executor:owner");
        const results = [];
        for (const id of accepted.map(index => ids[index])) results.push(await session.finished(id).catch(error => ({ status: String(error) })));
        const ran = ids.map(id => [...new Set(calls.filter(entry => entry.turn === id).map(entry => entry.provider))]);
        const outcome = { field, round, statuses: answers.map(answer => answer.status), errors: answers.map(answer => answer.body?.error ?? null), bound: bound?.value, ran, results: results.map(result => result.status) };
        outcomes.push(outcome);
        check("binding", accepted.length === 1 && answers.every(answer => answer.status === 202 || answer.status === 409) && bound?.value === values[accepted[0]], `${provider}/${field} round ${round}: of two first turns with different ${field}s one is accepted and binds the session and the other gets 409`, outcome);
        if (field === "provider") check("binding", ran.every((providers, index) => providers.every(value => value === values[index])), `${provider}/${field} round ${round}: every turn ran on the provider it was admitted with`, outcome);
      }
    }
  } finally { await local.dispose(); }
  return outcomes;
}

async function replyOrderRun(provider) {
  const local = patched(`
    const instance = this.instance.bind(this);
    this.instance = (...args) => instance(...args).then(runtime => {
      if (runtime.lateReads) return runtime;
      runtime.lateReads = true;
      const pending = runtime.pending.bind(runtime);
      runtime.pending = async scope => {
        const answer = await pending(scope);
        await new Promise(resolve => setTimeout(resolve, 50));
        return answer;
      };
      return runtime;
    });`);
  const outcomes = [];
  try {
    await local.ready;
    const session = client(local, `faults-reply-order-${provider}-${Date.now()}`);
    for (let round = 0; round < ROUNDS; round++) {
      const first = `ro-a-${round}-${provider}`;
      const second = `ro-b-${round}-${provider}`;
      const admitted = [await session.post(first, "plain", { provider })];
      await sleep(10);
      admitted.push(await session.post(second, "plain", { provider }));
      const done = await Promise.all([first, second].map(id => session.finished(id).catch(error => ({ status: String(error).slice(0, 120) }))));
      const outcome = { round, admitted: admitted.map(answer => answer.status), finished: done.map(turn => turn.status) };
      outcomes.push(outcome);
      check("replyOrder", done.every(turn => turn.status === "completed"), `${provider}/round ${round}: a turn admitted while the pump's pending read is answered late still runs`, outcome);
    }
  } finally { await local.dispose(); }
  return outcomes;
}

async function pumpFaultRun(provider) {
  const session = await newSession(provider, "pump-fault");
  const watcher = await watch(session);
  const root = `pf-${provider}`;
  const child = `kid-${root}`;
  await submit(session, root, "[spawn]");
  await until(() => holds.has(`${child}:0`), "child held in inference");
  await settled(session, root);
  await sleep(3 * DELAY);
  const before = await metrics(session);
  await sql(session.name, "CREATE TRIGGER fault_injection BEFORE INSERT ON metadata WHEN NEW.key LIKE 'background:%' BEGIN SELECT RAISE(ABORT, 'injected fault'); END");
  await until(async () => (await metrics(session)).alarms >= before.alarms + 2, "alarm pumps failed under the fault", 10 * DELAY + 5000);
  const during = await metrics(session);
  await sql(session.name, "DROP TRIGGER fault_injection");
  await sleep(2 * DELAY);
  const entry = holds.get(`${child}:0`);
  if (entry) { holds.delete(`${child}:0`); entry.release(entry.reply()); }
  await until(() => calls.some(call => call.turn === root && call.completion), "completion turn after the fault", 20000);
  await invariants("pumpFault", session);
  const after = await metrics(session);
  const outcome = { resident_runtimes: [before.resident_runtimes, during.resident_runtimes], runtime_initializations: [before.runtime_initializations, during.runtime_initializations, after.runtime_initializations], child_calls: modelCalls(child, 0), released_held_call: !!entry };
  check("pumpFault", during.resident_runtimes === before.resident_runtimes, `${provider}: a pump that fails before it has a runtime keeps the resident runtime`, outcome);
  check("pumpFault", outcome.child_calls === 1 && after.runtime_initializations === before.runtime_initializations, `${provider}: the background child's held call finished on the resident runtime without being repeated`, outcome);
  watcher.close();
  return outcome;
}

const LARGE_RECORDS = "FROM records WHERE state_id = ? AND key LIKE 'b3:%' AND substr(value, 1, 1) IN ('%', '^')";

async function legacyRun(provider, size, manifests, bundle) {
  const label = `${provider}/${size}/${manifests}`;
  await restart(bundle);
  const session = await newSession(provider, `legacy-${size}-${manifests}`);
  const watcher = await watch(session);
  const root = `lg-${size}-${manifests}-${provider}`;
  const child = `kid-${root}`;
  await submit(session, root, size === "large" ? "[spawn] [large]" : "[spawn]");
  await until(() => holds.has(`${child}:0`), "child held in inference");
  const done = await settled(session, root);
  check("legacy", done.status === "completed", `${label}: root turn completed on the affected build while its child runs`, done);
  await sleep(3 * DELAY);
  const [{ value: rootId }] = await sql(session.name, "SELECT value FROM metadata WHERE key = 'session_id'");
  const journal = `${rootId}:subagents`;
  const large = await sql(session.name, `SELECT key ${LARGE_RECORDS}`, journal);
  const outcome = { large_records: large.length };
  if (manifests === "lost") {
    if (!large.length) { await restart(); await invariants("legacy", session); watcher.close(); return { ...outcome, skipped: "the affected build wrote no record of 16 KiB or more" }; }
    await sql(session.name, `DELETE ${LARGE_RECORDS}`, journal);
  }
  const before = calls.length;
  await restart();
  outcome.quiet = await until(async () => {
    const [marker] = await sql(session.name, "SELECT COUNT(*) AS count FROM metadata WHERE key LIKE 'background:%'");
    return marker.count === 0 && (await metrics(session)).alarm_at === null ? true : undefined;
  }, "quiescence after the restart", 20 * DELAY + 5000).then(() => true, () => false);
  const window = await metrics(session);
  await sleep(5 * DELAY);
  const later = await metrics(session);
  outcome.alarms_after = later.alarms - window.alarms;
  outcome.alarm_at = later.alarm_at;
  const completions = () => calls.slice(before).filter(call => call.turn === root && call.completion).map(call => call.completion);
  const childCalls = calls.slice(before).filter(call => call.turn === child);
  outcome.completions = completions();
  outcome.child_calls = childCalls.length;
  const probe = async kind => {
    const id = `lgp-${kind}-${size}-${manifests}-${provider}`;
    await submit(session, id, `[${kind}]`);
    await settled(session, id).catch(() => undefined);
    return calls.find(call => call.turn === id && call.round === 1)?.output ?? "";
  };
  const listed = await probe("list");
  const waited = await probe("wait");
  outcome.list_agents = listed.slice(0, 600);
  outcome.wait_agent = waited.slice(0, 600);
  const lost = manifests === "lost";
  const unreadable = lost && size === "large";
  if (!lost) {
    check("legacy", completions().length === 1 && completions()[0] === "completed", `${label}: the child resumed from the affected journal and the root ran one completion turn`, outcome);
    check("legacy", childCalls.length >= 1 && childCalls.length <= 3, `${label}: the child made its interrupted call again after the restart`, outcome);
    check("legacy", /"state":"completed"/.test(listed) && !/missing payload record|could not/.test(listed + waited), `${label}: list_agents and wait_agent report the resumed child as completed`, outcome);
  } else if (!unreadable) {
    check("legacy", completions().length === 1 && completions()[0] === "failed", `${label}: the child whose checkpoint is unreadable failed and the root ran one completion turn for it`, outcome);
    check("legacy", childCalls.length === 0, `${label}: the child with an unreadable checkpoint made no model call`, outcome);
    for (const [tool, output] of [["list_agents", listed], ["wait_agent", waited]]) check("legacy", /"state":"failed"/.test(output) && /checkpoint [^ ]+ is unreadable/.test(output), `${label}: ${tool} reports the child as failed because its checkpoint is unreadable`, outcome);
  } else {
    check("legacy", completions().length === 0 && childCalls.length === 0, `${label}: an unreadable journal resumed nothing`, outcome);
    check("legacy", /could not restore subagent journal/.test(listed) && /could not restore subagent journal/.test(waited), `${label}: list_agents and wait_agent report the unreadable journal`, outcome);
  }
  check("legacy", outcome.quiet && outcome.alarms_after === 0, `${label}: the session becomes quiescent and its alarms stop after the restart`, outcome);
  const after = `lga-${size}-${manifests}-${provider}`;
  await submit(session, after, "plain");
  const final = await settled(session, after);
  check("legacy", final.status === "completed" && modelCalls(after) === 1, `${label}: a later turn completes once`, final);
  check("legacy", modelCalls(root, 0) === 1, `${label}: root spawn call was not repeated`, modelCalls(root, 0));
  await invariants("legacy", session);
  watcher.close();
  return outcome;
}

const handBinary = resolve(process.env.NANOCODE_SDK_PATH ?? "../nanocodex", "target/debug/nanocode");
const handGroups = new Set();
process.on("exit", () => { for (const group of handGroups) { try { process.kill(-group, "SIGKILL"); } catch {} } });

function startHand(session, hand) {
  const workspace = resolve(mkdtempSync(`output/faults-hand-${hand}-`));
  const child = spawn(handBinary, ["hand", `ws://127.0.0.1:${PORT}/sessions/${session.name}/hands`, "--name", hand, "-C", workspace], { env: { ...process.env, NANOCODE_RUNNER_TOKEN: LOCAL_TOKEN }, stdio: ["ignore", "ignore", "pipe"], detached: true });
  handGroups.add(child.pid);
  let log = "";
  child.stderr.on("data", chunk => { log += chunk; });
  const exited = new Promise(done => child.once("exit", () => { handGroups.delete(child.pid); done(); }));
  return { workspace, child, exited, log: () => log, stop: async () => { try { process.kill(-child.pid, "SIGKILL"); } catch {} await exited; } };
}

const listed = async (session, hand) => ((await (await request(`${sessionUrl(session.name)}/hands`)).json()).hands ?? []).includes(hand);

async function warmRun(provider) {
  if (!existsSync(handBinary)) throw new Error(`build ${handBinary} first (cargo build -p nanocode-cli --bin nanocode)`);
  const session = await newSession(provider, "warm");
  const id = `wd-${provider}`;
  await submit(session, id, "[hold]");
  await until(() => holds.has(`${id}:0`), "held model call");
  const before = await metrics(session);
  const hand = startHand(session, "laptop");
  try {
    await until(() => listed(session, "laptop"), "hand attached", 20000);
    const response = await request(`${sessionUrl(session.name)}/socket`, { headers: { upgrade: "websocket" } });
    const socket = response.webSocket;
    const replies = [];
    socket.addEventListener("message", event => { const message = JSON.parse(String(event.data)); if (message.type !== "event") replies.push(message); });
    socket.accept();
    socket.send(JSON.stringify({ type: "warm" }));
    const warmed = await until(() => replies.find(message => message.type === "warm"), "warm reply", 20000);
    const entry = holds.get(`${id}:0`);
    if (entry) { holds.delete(`${id}:0`); entry.release(entry.reply()); }
    const done = await settled(session, id);
    const after = await metrics(session);
    check("warm", done.status === "completed" && modelCalls(id) === 1, `${provider}: a warm sent while a turn runs leaves the turn running`, { warmed: warmed.warmed, status: done.status, calls: modelCalls(id), runtime_initializations: [before.runtime_initializations, after.runtime_initializations], recoveries: after.recoveries });
    await until(async () => (await metrics(session)).alarm_at === null, "idle after the turn", 10000);
    socket.send(JSON.stringify({ type: "warm" }));
    await until(() => replies.filter(message => message.type === "warm").length === 2, "second warm reply", 20000);
    const warmedIdle = await metrics(session);
    const next = `wn-${provider}`;
    cells[next] = `text("ok");`;
    await submit(session, next, "[exec]", { hand: "laptop" });
    const nextDone = await settled(session, next);
    const final = await metrics(session);
    check("warm", nextDone.status === "completed" && final.runtime_initializations === warmedIdle.runtime_initializations && warmedIdle.runtime_initializations === after.runtime_initializations + 1, `${provider}: a warm on an idle session opens the hands-mode runtime the next turn reuses`, { status: nextDone.status, runtime_initializations: [after.runtime_initializations, warmedIdle.runtime_initializations, final.runtime_initializations] });
    try { socket.close(1000, "done"); } catch {}
  } finally { await hand.stop(); }
}

async function cellRun(provider) {
  if (!existsSync(handBinary)) throw new Error(`build ${handBinary} first (cargo build -p nanocode-cli --bin nanocode)`);
  const session = await newSession(provider, "cell");
  const hand = startHand(session, "desk");
  try {
    await until(() => listed(session, "desk"), "hand attached", 20000);
    const id = `cell-${provider}`;
    cells[id] = `const result = await tools.exec_command({ cmd: "echo started >> marks.txt; sleep 2; echo finished >> marks.txt; echo done" }); text(JSON.stringify(result));`;
    await submit(session, id, "[exec]", { hand: "desk" });
    const marks = resolve(hand.workspace, "marks.txt");
    await until(() => existsSync(marks), "cell started on the hand", 20000);
    await restart();
    const done = await settled(session, id, 60000);
    await sleep(2500);
    const lines = readFileSync(marks, "utf8").trim().split("\n");
    const reported = calls.filter(call => call.turn === id && call.round === 1).map(call => call.output);
    check("cell", done.status === "completed", `${provider}: the turn whose cell ran across a restart completed`, done);
    check("cell", lines.filter(line => line === "started").length === 1, `${provider}: the interrupted cell was not run again`, lines);
    check("cell", modelCalls(id, 0) === 1, `${provider}: the model call that issued the cell was not repeated`, modelCalls(id, 0));
    check("cell", reported.length >= 1 && reported.every(text => !/"output":\s*"done/.test(text ?? "")), `${provider}: the cell's outcome reached the model as unknown, not as a replayed result`, reported);
    await invariants("cell", session);
  } finally { await hand.stop(); }
}

async function retentionRun(provider) {
  const options = localOptions({ provider: async request => globalThis.faultProvider(request), agent: { RECOVERY_DELAY_MS: String(DELAY), RUNTIME_MEMORY_BUDGET_BYTES: "1" } });
  const shim = options.workers[0].modules[0];
  const original = "return Response.json({ ...await response.json(), sql: this.sqlTotals(), idle: { timers_ms: pendingTimers(), wait_until: this.waiting.count } });";
  if (!shim.contents.includes(original)) throw new Error("the local shim changed; update the retention probe");
  shim.contents = shim.contents.replace(original, "const retained = await Promise.all([...(this.instances?.values() ?? [])].map(entry => entry.then(runtime => ({ closed: runtime.closed, wasm_memory_bytes: runtime.memoryBytes }), () => null))); return Response.json({ ...await response.json(), sql: this.sqlTotals(), idle: { timers_ms: pendingTimers(), wait_until: this.waiting.count }, retained });");
  const local = new Miniflare(convertV4MiniflareOptions(options));
  try {
    await local.ready;
    const name = `faults-retention-${provider}-${Date.now()}`;
    const call = (path, init = {}) => local.dispatchFetch(`http://worker/sessions/${name}${path}`, { ...init, headers: { authorization, "content-type": "application/json" } });
    const id = `rt-${provider}`;
    const streamed = await local.dispatchFetch(`http://worker/sessions/${name}/turns`, { method: "POST", headers: { authorization, "content-type": "application/json", accept: "text/event-stream" }, body: JSON.stringify({ request_id: id, prompt: `turn=${id} plain`, provider }) });
    const finished = await streamed.text();
    check("retention", finished.includes('"type":"result"'), `${provider}: the turn's stream ended with its result`, finished.slice(-300));
    const idle = await until(async () => { const current = await (await call("/metrics")).json(); return current.alarm_at === null && current.runtime_retirements > 0 ? current : undefined; }, "retired after the turn", 10000);
    report.diagnostics[`retention/${provider}`] = { retained: idle.retained, runtime_retirements: idle.runtime_retirements };
    const closed = idle.retained.filter(entry => entry?.closed);
    check("retention", closed.length === 0, `${provider}: a runtime retired over the memory budget is no longer referenced`, { runtime_retirements: idle.runtime_retirements, resident_runtimes: idle.resident_runtimes, retained: idle.retained });
  } finally { await local.dispose(); }
}

async function destroyRun(provider) {
  const session = await newSession(provider, "destroy");
  const id = `ds-${provider}`;
  await submit(session, id, "[hold]");
  await until(() => holds.has(`${id}:0`), "held model call");
  const before = (await sql(session.name, "SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name")).map(row => row.name);
  const namespace = await mf.getDurableObjectNamespace("SESSIONS", "nanocode");
  const stub = namespace.get(namespace.idFromName(objectName(session.name)));
  const response = await stub.fetch("https://session.internal/internal/delete", { method: "POST", headers: { "x-nanocode-descriptor": JSON.stringify({ account_id: "owner", session_id: "", session_name: session.name, subscription: false }) } });
  const deleted = await response.json();
  await sleep(10 * DELAY);
  const after = (await sql(session.name, "SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name")).map(row => row.name);
  const gone = await status(session, id);
  check("destroy", deleted.deleted === true && gone.status === "http 410", `${provider}: deleting a session with a running turn answers 410 afterwards`, { deleted, status: gone.status });
  check("destroy", !after.some(table => ["turns", "events", "states", "records"].includes(table)), `${provider}: deleting a session with a running turn leaves no session tables behind`, { before, after });
}

async function chaosRun(provider) {
  const session = await newSession(provider, "chaos");
  const watcher = await watch(session);
  const ids = Array.from({ length: 10 }, (_, index) => `ch-${index}-${provider}`);
  const kinds = ["plain", "[tool]", "[slow:80]", "[tool] [slow:40]"];
  const streams = [];
  for (const [index, id] of ids.entries()) {
    const kind = kinds[index % kinds.length];
    if (index % 3 === 0) streams.push({ id, stream: (await stream(session, id)).start(kind) });
    else await submit(session, id, kind);
  }
  const restarts = [];
  for (let round = 0; round < 4; round++) {
    await sleep(30 + Math.floor(random() * 150));
    restarts.push(Math.round(await restart()));
  }
  for (const id of ids) {
    const done = await settled(session, id, 60000);
    check("chaos", done.status === "completed", `${provider}/${id}: completed through restarts`, done);
  }
  for (const { id, stream: collected } of streams) {
    await Promise.race([collected.running, sleep(10000)]);
    const own = collected.events.filter(event => event.request_id === id);
    check("chaos", collected.done && !collected.duplicate && own.at(-1)?.type === "result", `${provider}/${id}: SSE stream reached its result once across ${collected.reconnects} reconnects`, { done: collected.done, duplicate: collected.duplicate, error: collected.error, last: own.at(-1) });
  }
  const { events } = await invariants("chaos", session);
  for (const { id, stream: collected } of streams) {
    const expected = events.filter(event => event.request_id === id).map(event => event.seq);
    const seen = collected.events.map(event => event.seq);
    check("chaos", JSON.stringify(expected) === JSON.stringify(seen), `${provider}/${id}: SSE stream carried exactly the turn's events`, { expected, seen });
  }
  const resubmitted = await Promise.all(ids.map(id => submit(session, id, kinds[ids.indexOf(id) % kinds.length])));
  check("chaos", resubmitted.every(entry => entry.status === 200 && entry.body?.status === "completed"), `${provider}: resubmission after finish returns the saved result`, resubmitted.map(entry => entry.status));
  const conflicting = await submit(session, ids[0], "different");
  check("chaos", conflicting.status === 409, `${provider}: resubmission with different input conflicts`, conflicting.status);
  watcher.close();
  return { restarts };
}

mkdirSync("output", { recursive: true });
const selected = positionals.length ? positionals : Object.keys(scenarios);
const report = { seed: Number(values.seed), delay_ms: DELAY, providers: PROVIDERS, scenarios: {}, findings, checks, unattributed: {}, diagnostics: {} };
try {
  await boot();
  for (const name of selected) {
    for (const provider of PROVIDERS) {
      const started = performance.now();
      try {
        const detail = await scenarios[name](provider);
        report.scenarios[`${name}/${provider}`] = { milliseconds: Math.round(performance.now() - started), ...(detail ? { detail } : {}) };
      } catch (error) {
        findings.push({ scenario: name, provider, error: error instanceof Error ? error.message : String(error) });
        report.scenarios[`${name}/${provider}`] = { failed: error instanceof Error ? error.message : String(error) };
      }
      for (const watcher of [...watchers]) watcher.close();
    }
  }
} finally {
  report.result = findings.length ? "findings" : "passed";
  report.model_calls = calls.length;
  report.calls = calls;
  writeFileSync("output/faults-measurement.json", JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ result: report.result, seed: report.seed, scenarios: report.scenarios, findings, checks: checks.length }, null, 2));
  const forced = setTimeout(() => process.exit(1), 30000);
  await mf?.dispose().catch(() => {});
  clearTimeout(forced);
  if (findings.length) process.exitCode = 1;
}

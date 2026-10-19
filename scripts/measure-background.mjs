import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { localOptions } from "./local-runtime.mjs";

const limit = 30000;
const childOutput = "background-child-ok";
const prompts = {
  spawn: "BG-SPAWN delegate one background check and end this turn.",
  after: "BG-AFTER reply after background recovery.",
  hang: "BG-HANG this root turn is interrupted and cancelled.",
  final: "BG-FINAL reply after interrupted cancellation.",
};
const mode = { holdChild: true, hangCompletion: false, hangRoot: true };
const calls = [];
const anomalies = [];
const offered = {};
const observed = { childIds: new Set(), spawnResults: [] };
let modelCalls = 0;
let phase = "spawn";

const text = content => typeof content === "string" ? content : (content ?? []).filter(block => block.type === "text").map(block => block.text).join("\n");
const results = content => typeof content === "string" ? [] : (content ?? []).filter(block => block.type === "tool_result");

function sse(input, blocks, stop) {
  const events = [["message_start", { type: "message_start", message: { id: `message-${modelCalls}`, type: "message", role: "assistant", model: input.model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 8, output_tokens: 0 } } }]];
  blocks.forEach((block, index) => {
    if (block.type === "text") {
      events.push(["content_block_start", { type: "content_block_start", index, content_block: { type: "text", text: "" } }]);
      events.push(["content_block_delta", { type: "content_block_delta", index, delta: { type: "text_delta", text: block.text } }]);
    } else {
      events.push(["content_block_start", { type: "content_block_start", index, content_block: { type: "tool_use", id: block.id, name: block.name, input: {} } }]);
      events.push(["content_block_delta", { type: "content_block_delta", index, delta: { type: "input_json_delta", partial_json: JSON.stringify(block.input) } }]);
    }
    events.push(["content_block_stop", { type: "content_block_stop", index }]);
  });
  events.push(["message_delta", { type: "message_delta", delta: { stop_reason: stop, stop_sequence: null }, usage: { output_tokens: 4 } }]);
  events.push(["message_stop", { type: "message_stop" }]);
  return new Response(events.map(([event, data]) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`).join(""), { headers: { "content-type": "text/event-stream" } });
}

const reply = (input, value) => sse(input, [{ type: "text", text: value }], "end_turn");
const invoke = (input, name, value) => sse(input, [{ type: "tool_use", id: `toolu_background_${modelCalls}`, name, input: value }], "tool_use");
const hang = () => new Response(new ReadableStream({ start() {} }), { headers: { "content-type": "text/event-stream" } });

function tool(input, suffix) {
  const names = (input.tools ?? []).map(entry => entry.name).filter(name => name === suffix || name?.endsWith(`__${suffix}`));
  if (names.length !== 1) throw new Error(`Expected one ${suffix} tool, offered ${names.length}`);
  return names[0];
}

function classify(input) {
  const messages = input.messages ?? [];
  const first = text(messages[0]?.content);
  const boundary = messages.findLastIndex(message => message.role === "assistant");
  const trailing = messages.slice(boundary + 1);
  const answered = trailing.flatMap(message => results(message.content));
  const uses = new Map(messages.flatMap(message => Array.isArray(message.content) ? message.content.filter(block => block.type === "tool_use").map(block => [block.id, block.name]) : []));
  const answeredTool = answered.length ? uses.get(answered[0].tool_use_id) : undefined;
  const accepted = messages.some(message => results(message.content).some(block => !block.is_error && uses.get(block.tool_use_id)?.endsWith("submit_result")));
  const prompt = trailing.map(message => text(message.content)).filter(Boolean).join("\n");
  const steer = answered.length && prompt ? (prompt.includes("The sub-agent runtime restarted") ? "recovery" : "other") : undefined;
  let duplicate = false;
  if (!answered.length && prompt) {
    for (let index = boundary; index >= 0; index--) {
      const message = messages[index];
      if (message.role === "assistant" && !(Array.isArray(message.content) && message.content.some(block => block.type === "tool_use"))) break;
      if (message.role === "user" && text(message.content) === prompt) { duplicate = true; break; }
    }
  }
  const child = /Your agent ID is ([^\s.]+)\./.exec(first);
  const shape = { messages: messages.length, roles: messages.map(message => `${message.role}:${typeof message.content === "string" ? "text" : message.content.map(block => block.type).join("+")}`), ...(duplicate ? { duplicate } : {}), ...(steer ? { steer } : {}) };
  return { actor: child ? "child" : "root", childId: child?.[1], answered, answeredTool, accepted, prompt, duplicate, shape };
}

let current;
function record(entry) {
  calls.push({ phase, ...entry, ...current.shape });
}

const provider = async request => {
  const url = new URL(request.url);
  if (url.hostname !== "provider.invalid" || request.headers.get("x-api-key") !== "local-provider") throw new Error("Provider request escaped configured credential policy");
  const input = await request.json();
  if (input.model !== "claude-opus-5-5" || JSON.stringify(input).includes("local-provider")) throw new Error("Invalid model or credentials entered model input");
  modelCalls++;
  const call = classify(input);
  current = call;
  if (call.actor === "child") {
    observed.childIds.add(call.childId);
    offered.child ??= (input.tools ?? []).map(entry => entry.name).sort();
    if (call.answeredTool?.endsWith("submit_result")) {
      const receipt = call.answered[0].content;
      record({ actor: "child", kind: "submit_receipt", error: !!call.answered[0].is_error, receipt: typeof receipt === "string" ? receipt : text(receipt) });
      return reply(input, "child finished");
    }
    if (call.answered.length) {
      record({ actor: "child", kind: "unexpected_tool_result", tool: call.answeredTool });
      anomalies.push(`child answered ${call.answeredTool}`);
      return reply(input, "child unexpected");
    }
    if (call.accepted) {
      record({ actor: "child", kind: "after_accept" });
      return reply(input, "child already submitted");
    }
    if (mode.holdChild) {
      record({ actor: "child", kind: "initial", action: "hang" });
      return hang();
    }
    record({ actor: "child", kind: "initial", action: "submit_result" });
    return invoke(input, tool(input, "submit_result"), { output: childOutput });
  }
  offered.root ??= (input.tools ?? []).map(entry => entry.name).sort();
  if (call.answeredTool?.endsWith("spawn_agent")) {
    const content = call.answered[0].content;
    const body = typeof content === "string" ? content : text(content);
    observed.spawnResults.push({ error: !!call.answered[0].is_error, body: call.answered[0].is_error ? body : undefined, agent_id: /"agent_id"\s*:\s*"?([^",}\s]+)/.exec(body)?.[1] });
    record({ actor: "root", kind: "spawn_receipt", error: !!call.answered[0].is_error });
    return reply(input, "root turn ended while the child runs");
  }
  if (call.answeredTool?.endsWith("list_agents")) {
    const content = call.answered[0].content;
    const body = typeof content === "string" ? content : text(content);
    record({ actor: "root", kind: "completion_receipt", error: !!call.answered[0].is_error, child_output: body.includes(childOutput) });
    return reply(input, "root integrated the background result");
  }
  if (call.answered.length) {
    record({ actor: "root", kind: "unexpected_tool_result", tool: call.answeredTool });
    anomalies.push(`root answered ${call.answeredTool}`);
    return reply(input, "root unexpected");
  }
  if (call.duplicate) {
    record({ actor: "root", kind: "duplicate_prompt", prompt: call.prompt.slice(0, 120) });
    return reply(input, "root ignored a repeated prompt");
  }
  if (call.prompt.includes("<subagent_completion")) {
    const status = /<subagent_completion agent_id="([^"]+)" status="([^"]+)"/.exec(call.prompt);
    const hold = mode.hangCompletion;
    record({ actor: "root", kind: "completion", agent_id: status?.[1], status: status?.[2], action: hold ? "hang" : "list_agents" });
    return hold ? hang() : invoke(input, tool(input, "list_agents"), { include_completed: true });
  }
  if (call.prompt.includes("BG-SPAWN")) {
    record({ actor: "root", kind: "spawn" });
    return invoke(input, tool(input, "spawn_agent"), { role: "background verifier", task: `Return the string ${childOutput}.`, harness: null, model: null, thinking: null, reasoning_mode: null, output_contract: { kind: "string" } });
  }
  if (call.prompt.includes("BG-HANG")) {
    const hold = mode.hangRoot;
    record({ actor: "root", kind: "hang", action: hold ? "hang" : "reply" });
    if (!hold) anomalies.push("cancelled root turn dispatched a recovered model request");
    return hold ? hang() : reply(input, "cancelled turn unexpectedly continued");
  }
  for (const marker of ["BG-AFTER", "BG-FINAL"]) {
    if (call.prompt.includes(marker)) {
      record({ actor: "root", kind: marker });
      return reply(input, `${marker} done`);
    }
  }
  record({ actor: "root", kind: "unrecognized" });
  anomalies.push("unrecognized root request");
  return reply(input, "unrecognized");
};

const options = localOptions({ provider });
let mf = new Miniflare(convertV4MiniflareOptions(options));
const name = `background-${Date.now()}`;
const base = `http://worker/sessions/${name}`;
const headers = { authorization: "Bearer local-measurement", "content-type": "application/json" };
const request = async (url, init = {}) => mf.dispatchFetch(url, { ...init, headers });
const sql = async (query, ...bindings) => (await mf.unsafeGetDurableObjectStorage("nanocode-worker", "AgentSession", { name })).exec(query, ...bindings);
const count = (filter = () => true) => calls.filter(filter).length;
const steps = [];
const findings = [];
const samples = { taken: 0, markerAbsentWhileBusy: [] };
let sampling = false;
let childDone = false;
let report;

function check(condition, detail, evidence, fatal = true) {
  steps.push({ phase, check: detail, ok: !!condition, ...(evidence === undefined ? {} : { evidence }) });
  if (condition) return;
  if (!fatal) { findings.push({ phase, check: detail, evidence }); return; }
  throw new Error(`${phase}: ${detail}${evidence === undefined ? "" : ` ${JSON.stringify(evidence)}`}`);
}

async function until(condition, detail, timeout = limit) {
  const start = performance.now();
  while (!await condition()) {
    if (performance.now() - start > timeout) throw new Error(`${phase}: ${detail}`);
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  return performance.now() - start;
}

async function restart() {
  sampling = false;
  const start = performance.now();
  await mf.dispose();
  mf = new Miniflare(convertV4MiniflareOptions(options));
  await mf.ready;
  steps.push({ phase, restart_ms: performance.now() - start });
}

async function submit(id, prompt) {
  const response = await request(`${base}/turns`, { method: "POST", body: JSON.stringify({ request_id: id, prompt }) });
  if (response.status !== 202) throw new Error(`Submit ${id} rejected with ${response.status}: ${await response.text()}`);
}

async function job(id) {
  const response = await request(`${base}/turns/${encodeURIComponent(id)}`);
  if (response.status !== 200) throw new Error(`Status ${id} rejected with ${response.status}`);
  return response.json();
}

async function settled(id) {
  let state;
  const milliseconds = await until(async () => { state = await job(id); return ["completed", "failed", "cancelled"].includes(state.status); }, `Job ${id} did not finish`);
  return { state, milliseconds };
}

const metrics = async () => (await request(`${base}/metrics`)).json();
const marker = async () => (await sql("SELECT COUNT(*) AS count FROM metadata WHERE key LIKE 'background:%'"))[0].count;
const sessionUuid = async () => (await sql("SELECT value FROM metadata WHERE key = 'session_id'"))[0]?.value;
const stateIds = async () => (await sql("SELECT id FROM states ORDER BY rowid")).map(row => row.id);

async function operations(stateId) {
  const [valid] = await sql("SELECT json_valid(payload) AS valid FROM states WHERE id = ?", stateId);
  if (!valid) return [];
  if (!valid.valid) throw new Error(`State ${stateId} payload is not JSON`);
  const [located] = await sql("SELECT t.fullkey AS path FROM states s, json_tree(s.payload) t WHERE s.id = ? AND t.key = 'operations' AND t.type = 'object' ORDER BY length(t.fullkey) LIMIT 1", stateId);
  if (!located) return [];
  const tag = value => `CASE json_type(${value}, '$.status') WHEN 'text' THEN json_extract(${value}, '$.status') ELSE (SELECT key FROM json_each(${value}, '$.status') LIMIT 1) END`;
  const rows = await sql(`SELECT e.key AS id, ${tag("e.value")} AS status, json_extract(e.value, '$.accepted_order') AS accepted_order, (SELECT json_group_array(json_object('kind', json_extract(s.value, '$.kind'), 'status', ${tag("s.value")}, 'attempts', json_extract(s.value, '$.attempts'))) FROM json_each(e.value, '$.steps') s) AS steps FROM states st, json_each(st.payload, ?) e WHERE st.id = ? ORDER BY accepted_order`, located.path, stateId);
  return rows.map(row => ({ ...row, steps: JSON.parse(row.steps) }));
}

async function tree() {
  const root = await sessionUuid();
  const ids = await stateIds();
  const result = { root, states: ids.length, operations: {} };
  for (const id of ids) result.operations[id === root ? "root" : id] = await operations(id);
  return result;
}

const pendingRoot = async () => (await operations(await sessionUuid())).filter(operation => operation.status === "pending").map(operation => operation.id);

function sampler() {
  sampling = true;
  const current = mf;
  void (async () => {
    while (sampling && current === mf) {
      try {
        const present = await marker();
        const pending = await pendingRoot();
        samples.taken++;
        if (!present && (pending.length || !childDone)) samples.markerAbsentWhileBusy.push({ phase, pending, childDone });
      } catch {}
      await new Promise(resolve => setTimeout(resolve, 25));
    }
  })();
}

async function eventSummary() {
  const types = {};
  const attribution = {};
  const accepted = [];
  const terminal = [];
  let after = 0;
  let mentions = 0;
  for (;;) {
    const page = await (await request(`${base}/events?after=${after}&limit=128`)).json();
    for (const entry of page.events) {
      const body = entry.event ?? {};
      const inner = body.source === "agent" ? body.event : body.source === "subagent" ? body.update : body;
      const kind = inner?.type ?? inner?.kind ?? (inner && typeof inner === "object" ? Object.keys(inner).filter(key => !["request_id", "session_id"].includes(key)).sort().join("+") : "unknown");
      const type = body.source ? `${body.source}:${kind}` : kind;
      types[type] = (types[type] ?? 0) + 1;
      if (body.source === "agent" && inner?.type === "input.accepted") accepted.push({ stored_request_id: entry.request_id, stream_request_id: inner.request_id, operation_request_id: inner.payload?.request_id ?? null, turn_id: inner.payload?.turn_id ?? null });
      if (body.source === "agent" && /^(run\.|model\.call\.failed)/.test(inner?.type ?? "") && inner.type !== "run.started") terminal.push({ type: inner.type, turn_id: inner.payload?.turn_id ?? null });
      const owner = entry.request_id ?? "unattributed";
      attribution[owner] = (attribution[owner] ?? 0) + 1;
      if (observed.childSession && JSON.stringify(entry.event).includes(observed.childSession)) mentions++;
    }
    after = page.next_cursor;
    if (!page.has_more) break;
  }
  return { types, attribution, accepted, terminal, child_session_mentions: mentions };
}

try {
  await mf.ready;
  const bindings = await mf.getBindings("nanocode-worker");
  for (const key of ["ANTHROPIC_API_KEY", "OAUTH_JSON", "ACCOUNTS", "VAULT_KEY", "RUNNER_TOKEN", "WORKER_TOKEN", "VAULT"]) if (key in bindings) throw new Error("Provider secret boundary is violated");

  phase = "1-spawn";
  await submit("bg-spawn", prompts.spawn);
  await until(() => count(call => call.actor === "child" && call.kind === "initial") >= 1, "Child inference did not start");
  const spawned = await settled("bg-spawn");
  check(spawned.state.status === "completed", "root job completed while child is held in inference", spawned.state.status);
  check(observed.spawnResults.length === 1 && !observed.spawnResults[0].error, "spawn_agent tool succeeded once", observed.spawnResults);
  check(observed.childIds.size === 1, "one child agent ID observed", [...observed.childIds]);
  const childId = [...observed.childIds][0];
  check(observed.spawnResults[0].agent_id === undefined || observed.spawnResults[0].agent_id === childId, "spawn receipt agent_id matches child prompt", observed.spawnResults[0].agent_id);
  const root = await sessionUuid();
  observed.root = root;
  check(spawned.state.ok?.session_id === root, "job session_id equals persisted SDK session UUID", { job: spawned.state.ok?.session_id, root });
  sampler();
  const before = await metrics();
  await new Promise(resolve => setTimeout(resolve, 3 * Number(options.workers[0].bindings.RECOVERY_DELAY_MS)));
  const later = await metrics();
  check(await marker() === 1, "background marker remains after jobs completed");
  check((await sql("SELECT COUNT(*) AS count FROM jobs WHERE status IN ('queued', 'running')"))[0].count === 0, "no unfinished jobs while child is pending");
  check(later.alarms > before.alarms, "alarm keeps firing with no unfinished jobs", { before: before.alarms, later: later.alarms });
  const heldTree = await tree();
  const childStates = Object.keys(heldTree.operations).filter(id => id !== "root");
  steps.push({ phase, held_tree: heldTree });

  phase = "2a-child-restore";
  mode.holdChild = false;
  mode.hangCompletion = true;
  const childInitialBefore = count(call => call.actor === "child" && call.kind === "initial");
  await restart();
  const restoreMs = await until(() => count(call => call.actor === "root" && call.kind === "completion") >= 1, "Restored child did not complete and continue the root without a client");
  childDone = true;
  steps.push({ phase, restore_to_completion_dispatch_ms: restoreMs });
  check(observed.childIds.size === 1, "restored child kept its ID", [...observed.childIds]);
  check(!calls.some(call => call.actor === "child" && (call.duplicate || call.kind === "after_accept" || call.steer === "other")), "restored child turn saw no duplicated prompt or unexplained steer", calls.filter(call => call.actor === "child").map(({ kind, roles, steer }) => ({ kind, roles, steer })), false);
  check(count(call => call.actor === "child" && call.kind === "initial") === childInitialBefore + 1, "interrupted child inference repeated once after restore", count(call => call.actor === "child" && call.kind === "initial"));
  check(count(call => call.actor === "child" && call.kind === "submit_receipt" && !call.error) === 1 && count(call => call.actor === "child" && call.kind === "submit_receipt") === 1, "child submit_result accepted once and child sent final message", calls.filter(call => call.kind === "submit_receipt"));
  check(count(call => call.actor === "root" && call.kind === "spawn") === 1, "root spawn turn was not replayed to the model");
  const completionCall = calls.find(call => call.kind === "completion");
  check(completionCall.agent_id === childId && completionCall.status === "completed", "root continuation names the restored child as completed", completionCall);
  sampler();
  await new Promise(resolve => setTimeout(resolve, 200));
  const orphanPending = await pendingRoot();
  check(orphanPending.length === 1 && orphanPending[0].startsWith("subagent-completion:"), "root completion operation is Pending in SDK state before kill", orphanPending);
  observed.childSession = orphanPending[0].split(":")[1];
  check(childStates.includes(observed.childSession), "completion operation names the persisted child session", { child_session: observed.childSession, states: childStates });
  check(await marker() === 1, "background marker remains during root completion inference");

  phase = "2b-orphan-recovery";
  mode.hangCompletion = false;
  await restart();
  await until(() => count(call => call.kind === "completion") >= 2, "Orphaned SDK root completion was not resumed without a client");
  sampler();
  const quiescentMs = await until(async () => await marker() === 0, "Background marker was not cleared after quiescence");
  steps.push({ phase, clear_marker_ms: quiescentMs });
  const recoveredTree = await tree();
  const rootOperations = recoveredTree.operations.root;
  check(rootOperations.every(operation => operation.status !== "pending"), "all SDK root operations terminal when marker cleared", rootOperations.map(({ id, status }) => ({ id, status })));
  check(rootOperations.filter(operation => operation.id.startsWith("subagent-completion:")).length === 1, "exactly one SDK completion operation exists");
  check(count(call => call.kind === "completion") === 2, "completion inference repeated once after interruption", count(call => call.kind === "completion"));
  check(count(call => call.kind === "completion_receipt" && !call.error && call.child_output) === 1, "recovered root continuation read the child's string output through list_agents once", calls.filter(call => call.kind === "completion_receipt"));
  const alarmsQuiet = (await metrics()).alarms;
  await new Promise(resolve => setTimeout(resolve, 3 * Number(options.workers[0].bindings.RECOVERY_DELAY_MS)));
  check((await metrics()).alarms === alarmsQuiet, "alarm stops after durable quiescence", { before: alarmsQuiet, after: (await metrics()).alarms });

  phase = "2c-new-root-job";
  sampling = false;
  const callsBeforeAfter = modelCalls;
  await submit("bg-after", prompts.after);
  const after = await settled("bg-after");
  check(after.state.status === "completed", "new root job completed after orphan recovery", after.state);
  check(after.state.ok?.session_id === root, "new root job used the same SDK session UUID", after.state.ok?.session_id);
  check(modelCalls === callsBeforeAfter + 1, "new root job made one model call without loop or respawn", modelCalls - callsBeforeAfter);
  check(observed.childIds.size === 1 && count(call => call.kind === "spawn") === 1, "children were not respawned");
  check(Object.keys((await tree()).operations).filter(id => id !== "root").length === childStates.length, "child state count unchanged");

  phase = "3-interrupted-cancel";
  const hangCalls = () => count(call => call.kind === "hang");
  await submit("bg-cancel", prompts.hang);
  await until(() => hangCalls() === 1, "Interrupted root turn did not reach inference");
  check((await pendingRoot()).includes("bg-cancel"), "interrupted root operation is Pending in SDK state");
  await sql("CREATE TRIGGER measurement_hold_insert BEFORE INSERT ON metadata WHEN NEW.key LIKE 'background:%' BEGIN SELECT RAISE(ABORT, 'measurement recovery hold'); END");
  await sql("CREATE TRIGGER measurement_hold_update BEFORE UPDATE ON metadata WHEN NEW.key LIKE 'background:%' BEGIN SELECT RAISE(ABORT, 'measurement recovery hold'); END");
  mode.hangRoot = false;
  await restart();
  const cancelResponse = await request(`${base}/turns/bg-cancel/cancel`, { method: "POST" });
  check(cancelResponse.status === 202, "cancel of interrupted running job accepted before recovery", cancelResponse.status);
  check((await sql("SELECT cancel_requested FROM jobs WHERE request_id = 'bg-cancel'"))[0].cancel_requested === 1, "cancel request is durable");
  const held = await metrics();
  check(held.runtime_initializations === 0, "status and cancel requests did not initialize WASM during the recovery hold", { runtime_initializations: held.runtime_initializations, alarms: held.alarms });
  check(hangCalls() === 1, "no recovered model dispatch before cancel");
  await sql("DROP TRIGGER measurement_hold_insert");
  await sql("DROP TRIGGER measurement_hold_update");
  const cancelled = await settled("bg-cancel");
  check(cancelled.state.status === "cancelled", "interrupted job finished cancelled", cancelled.state);
  const cancelOperation = (await operations(root)).find(operation => operation.id === "bg-cancel");
  check(cancelOperation?.status === "cancelled", "SDK root operation is terminally Cancelled", cancelOperation?.status);
  check(hangCalls() === 1, "cancelled turn made no recovered model dispatch", hangCalls());
  await submit("bg-final", prompts.final);
  const final = await settled("bg-final");
  check(final.state.status === "completed" && final.state.ok?.session_id === root, "later root job completed in the same SDK session", final.state);
  await until(async () => await marker() === 0, "Background marker was not cleared after cancellation scenario");
  const persisted = await sql("SELECT payload FROM states UNION ALL SELECT value AS payload FROM records UNION ALL SELECT body AS payload FROM events UNION ALL SELECT result AS payload FROM jobs");
  check(!JSON.stringify(persisted).includes("local-provider"), "provider credential absent from agent persistence");
  check(anomalies.length === 0, "provider saw only expected requests", anomalies);
  check(!calls.some(call => call.duplicate), "no model request repeated a prompt inside one turn", calls.filter(call => call.duplicate).map(({ phase, actor, kind }) => ({ phase, actor, kind })), false);

  report = { result: findings.length ? "findings" : "passed" };
  if (findings.length) process.exitCode = 1;
} catch (error) {
  report = { result: "failed", failure: error instanceof Error ? error.message : String(error) };
  process.exitCode = 1;
} finally {
  sampling = false;
  try {
    report.final_tree = await tree();
    report.events = await eventSummary();
    report.metrics = await metrics();
  } catch (error) {
    report.inspection_failure = error instanceof Error ? error.message : String(error);
  }
  const forced = setTimeout(() => process.exit(1), limit);
  await mf.dispose().catch(() => {});
  clearTimeout(forced);
}

Object.assign(report, { root_session: observed.root, child_session: observed.childSession, findings, steered: calls.filter(call => call.steer || call.duplicate || call.kind === "after_accept").map(({ phase, actor, kind, steer, duplicate, roles }) => ({ phase, actor, kind, steer, duplicate, roles })), runtime: "local workerd", model: "mock Opus 5.5 SSE", session: name, child_ids: [...observed.childIds], spawn_results: observed.spawnResults, offered_tools: offered, model_calls: modelCalls, calls, anomalies, samples, steps });
writeFileSync("output/background-measurement.json", JSON.stringify(report, null, 2));
console.log(JSON.stringify(report, null, 2));

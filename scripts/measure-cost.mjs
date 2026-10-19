import { spawn } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { parseArgs } from "node:util";

const { values } = parseArgs({ options: { origin: { type: "string", default: "https://nanocode-worker.gakonst.workers.dev" }, bin: { type: "string", default: "nanocode" }, turns: { type: "string", default: "3" }, idle: { type: "string", default: "90000" }, label: { type: "string", default: "run" }, scenarios: { type: "string", default: "codex,claude,codex-hand" }, account: { type: "string", default: process.env.CLOUDFLARE_ACCOUNT_ID }, analyze: { type: "string" } } });
const token = process.env.WORKER_TOKEN;
const apiToken = process.env.CLOUDFLARE_API_TOKEN;
if ((!token && !values.analyze) || !apiToken || !values.account) throw new Error("Provide WORKER_TOKEN, CLOUDFLARE_API_TOKEN and CLOUDFLARE_ACCOUNT_ID through the environment");
const origin = new URL(values.origin);
const auth = { authorization: "Bearer " + token };
const turns = Number(values.turns);
const idle = Number(values.idle);
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const stamp = Date.now().toString(36);
const report = { label: values.label, started_at: new Date().toISOString(), turns, idle_ms: idle, scenarios: [] };

async function turn(session, provider, index, prompt, hand) {
  const started = performance.now();
  const response = await fetch(origin.origin + "/sessions/" + session + "/turns", { method: "POST", headers: { ...auth, "content-type": "application/json", accept: "text/event-stream" }, body: JSON.stringify({ request_id: "cost-" + index, provider, prompt, ...(hand ? { hand } : {}) }) });
  const body = await response.text();
  const result = body.split("\n\n").map(frame => frame.split("\n").filter(line => line.startsWith("data:")).map(line => line.slice(5).trim()).join("\n")).filter(Boolean).map(data => JSON.parse(data)).find(envelope => envelope.event?.type === "result");
  return { status: result?.event?.status ?? "missing:" + response.status, ms: Math.round(performance.now() - started) };
}

async function scenario(name) {
  const provider = name.startsWith("claude") ? "claude" : "codex";
  const withHand = name.endsWith("-hand");
  const session = "cost-" + values.label + "-" + name + "-" + stamp;
  const entry = { name, session, provider, hand: withHand, start: new Date().toISOString(), turns: [] };
  let child;
  if (withHand) {
    child = spawn(values.bin, ["hand", "wss://" + origin.host + "/sessions/" + session + "/hands/laptop", "-C", mkdtempSync(join(tmpdir(), "cost-hand-"))], { env: { ...process.env, NANOCODE_RUNNER_TOKEN: token }, stdio: "ignore" });
    for (let i = 0; i < 400; i++) { const listed = await (await fetch(origin.origin + "/sessions/" + session + "/hands", { headers: auth })).json().catch(() => ({})); if (listed.hands?.includes("laptop")) break; await sleep(100); }
  }
  for (let i = 1; i <= turns; i++) {
    const prompt = withHand ? "Use your tools to run echo cost-" + i + " on the attached machine, then reply with only what it printed." : "Reply with only the word ok.";
    entry.turns.push(await turn(session, provider, i, prompt, withHand ? "laptop" : undefined));
  }
  entry.turns_done = new Date().toISOString();
  await sleep(idle);
  child?.kill();
  entry.end = new Date().toISOString();
  const metrics = await (await fetch(origin.origin + "/sessions/" + session + "/metrics", { headers: auth })).json().catch(() => ({}));
  entry.object_id = metrics.object_id;
  entry.database_bytes = metrics.database_bytes;
  entry.events_written = metrics.events_written;
  entry.persisted = metrics.persisted;
  process.stderr.write(name + " " + JSON.stringify(entry.turns) + "\n");
  return entry;
}

mkdirSync("output", { recursive: true });
const out = values.analyze ?? "output/cost-" + values.label + "-" + stamp + ".json";
if (values.analyze) Object.assign(report, JSON.parse(readFileSync(values.analyze, "utf8")));
else {
  for (const name of values.scenarios.split(",")) report.scenarios.push(await scenario(name));
  report.ended_at = new Date().toISOString();
  writeFileSync(out, JSON.stringify(report, null, 2));
}
const ended = new Date(report.ended_at);

async function graphql(query, variables) {
  for (;;) {
    const r = await (await fetch("https://api.cloudflare.com/client/v4/graphql", { method: "POST", headers: { authorization: "Bearer " + apiToken, "content-type": "application/json" }, body: JSON.stringify({ query, variables }) })).json();
    if (r.errors?.some(error => error.extensions?.code === "budget")) { await sleep(60000); continue; }
    if (r.errors) throw new Error(JSON.stringify(r.errors).slice(0, 500));
    return r.data.viewer.accounts[0];
  }
}
const namespaces = (await (await fetch("https://api.cloudflare.com/client/v4/accounts/" + values.account + "/workers/durable_objects/namespaces?per_page=1000", { headers: { authorization: "Bearer " + apiToken } })).json()).result;
const namespace = namespaces.find(n => n.script === "nanocode-worker" && n.class === "AgentSession").id;
const start = new Date(Date.parse(report.started_at) - 60000);
const until = new Date(ended.getTime() + 120000);
const query = "query($a: String!, $s: Time!, $e: Time!, $n: String!) { viewer { accounts(filter: {accountTag: $a}) { durableObjectsPeriodicGroups(limit: 2000, filter: {datetimeMinute_geq: $s, datetimeMinute_leq: $e, namespaceId: $n}) { dimensions { objectId datetimeMinute } sum { activeTime cpuTime rowsRead rowsWritten inboundWebsocketMsgCount outboundWebsocketMsgCount subrequests } max { activeWebsocketConnections } } durableObjectsInvocationsAdaptiveGroups(limit: 2000, filter: {datetimeMinute_geq: $s, datetimeMinute_leq: $e, namespaceId: $n}) { dimensions { objectId } sum { requests } } } } }";
let rows;
for (let attempt = 0; attempt < 12; attempt++) {
  await sleep(attempt ? 30000 : Math.max(0, ended.getTime() + 90000 - Date.now()));
  const data = await graphql(query, { a: values.account, s: start.toISOString(), e: until.toISOString(), n: namespace });
  rows = data;
  const latest = Math.max(0, ...data.durableObjectsPeriodicGroups.map(g => Date.parse(g.dimensions.datetimeMinute)));
  if (latest >= Date.parse(report.scenarios.at(-1).end) - 60000) break;
}
const objects = {};
for (const g of rows.durableObjectsPeriodicGroups) {
  const o = objects[g.dimensions.objectId] ??= { first: g.dimensions.datetimeMinute, active_s: 0, cpu_ms: 0, rows_read: 0, rows_written: 0, ws_in: 0, ws_out: 0, subrequests: 0, max_ws: 0, requests: 0 };
  if (g.dimensions.datetimeMinute < o.first) o.first = g.dimensions.datetimeMinute;
  o.active_s += g.sum.activeTime / 1e6; o.cpu_ms += g.sum.cpuTime / 1e3; o.rows_read += g.sum.rowsRead; o.rows_written += g.sum.rowsWritten; o.ws_in += g.sum.inboundWebsocketMsgCount; o.ws_out += g.sum.outboundWebsocketMsgCount; o.subrequests += g.sum.subrequests; o.max_ws = Math.max(o.max_ws, g.max.activeWebsocketConnections);
}
for (const g of rows.durableObjectsInvocationsAdaptiveGroups) if (objects[g.dimensions.objectId]) objects[g.dimensions.objectId].requests += g.sum.requests;
for (const entry of report.scenarios) {
  const id = entry.object_id ?? Object.entries(objects).filter(([, o]) => o.first >= entry.start.slice(0, 16) && o.first <= entry.turns_done).sort((a, b) => a[1].first.localeCompare(b[1].first))[0]?.[0];
  const o = objects[id];
  if (!o) continue;
  entry.object_id = id;
  entry.cloudflare = { ...o, active_s: Math.round(o.active_s * 10) / 10, cpu_ms: Math.round(o.cpu_ms), gb_s: Math.round(o.active_s * 0.125 * 10) / 10, request_units: o.requests + Math.ceil(o.ws_in / 20) };
  entry.wall_s = Math.round((Date.parse(entry.end) - Date.parse(entry.start)) / 100) / 10;
}
writeFileSync(out, JSON.stringify(report, null, 2));
for (const entry of report.scenarios) console.log(entry.name.padEnd(12), JSON.stringify({ wall_s: entry.wall_s, turns: entry.turns.map(t => t.status + ":" + t.ms), ...entry.cloudflare, database_bytes: entry.database_bytes }));
console.log("report", out);

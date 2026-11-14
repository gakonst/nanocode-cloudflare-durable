import { mkdirSync, writeFileSync } from "node:fs";
import { parseArgs } from "node:util";

const { values } = parseArgs({ options: { origin: { type: "string", default: "https://nanocode-cloudflare-durable.tempo-dev.workers.dev" }, sessions: { type: "string", default: "6" }, idle: { type: "string", default: "990" }, label: { type: "string", default: "evicted" }, split: { type: "boolean", default: false }, gap: { type: "string", default: "2100" }, out: { type: "string", default: "output" } } });
const origin = values.origin;
const count = Number(values.sessions);
const gap = Number(values.gap);
const prompt = "Reply with only the word ok.";
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const unfinished = new Set(["queued", "running", "pending", "accepted", "waiting"]);
let last = 0;

function timing(header) {
  const entries = {};
  for (const part of (header ?? "").split(",")) {
    const [name, ...params] = part.trim().split(";");
    if (!name) continue;
    for (const param of params) {
      const [key, value] = param.split("=");
      if (key === "dur" || key === "desc") entries[name] = Number(value);
    }
  }
  return entries;
}

async function call(path, init) {
  for (;;) {
    const wait = last + gap - Date.now();
    if (wait > 0) await sleep(wait);
    last = Date.now();
    const started = performance.now();
    const response = await fetch(origin + path, init);
    const ms = performance.now() - started;
    const body = await response.text();
    if (response.status === 429) { await sleep(10 * gap); continue; }
    return { body, ms, status: response.status, timing: timing(response.headers.get("server-timing")) };
  }
}

async function get(path) {
  const response = await call(path);
  return { http: response.status, ms: response.ms, session_response: response.timing.session_response, server_timing: response.timing };
}

async function turn(session, id) {
  const sent = await call("/sessions/" + session + "/turns", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ request_id: id, prompt }) });
  const row = { accept: sent.ms, http: sent.status, session_response: sent.timing.session_response, session: sent.timing.session, isolate_objects: sent.timing.isolate_objects, object_requests: sent.timing.object_requests, server_timing: sent.timing };
  if (sent.status >= 300) return { ...row, body: sent.body.slice(0, 300) };
  for (;;) {
    await sleep(gap);
    const polled = await call("/sessions/" + session + "/turns/" + id);
    const report = (() => { try { return JSON.parse(polled.body); } catch { return undefined; } })();
    if (!report?.status || unfinished.has(report.status)) continue;
    const t = report.timing ?? {};
    return { ...row, status: report.status, admission: t.admission_ms, runtime_open: t.runtime_open_ms, run: t.run_ms, reused: t.runtime_reused, first_model: (t.model_requests ?? []).find(request => request.provider)?.started_ms };
  }
}

const median = list => {
  const sorted = list.filter(value => typeof value === "number").sort((a, b) => a - b);
  const middle = sorted.length >> 1;
  if (!sorted.length) return null;
  return Math.round(sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2);
};
const keys = ["accept", "session_response", "session", "admission", "runtime_open", "first_model", "run"];
const summary = rows => ({ n: rows.length, completed: rows.filter(row => row.status === "completed").length, ...Object.fromEntries(keys.map(key => [key, median(rows.map(row => row[key]))])), fresh_isolate: rows.filter(row => row.isolate_objects === 1).length });

const sessions = Array.from({ length: count }, (_, index) => values.label + "-" + Date.now().toString(36) + "-" + index);
const report = { origin, label: values.label, idle_s: Number(values.idle), split: values.split, sessions, started_at: new Date().toISOString(), first: [], cold: [], warm: [], probes: [] };
mkdirSync(values.out, { recursive: true });
const file = values.out + "/cold-evicted-" + values.label + ".json";
const save = () => writeFileSync(file, JSON.stringify(report, null, 1));
for (const session of sessions) report.first.push(await turn(session, "first"));
save();
console.error("seeded " + count + " sessions, idle " + values.idle + " s");
await sleep(Number(values.idle) * 1000);
report.cold_at = new Date().toISOString();
for (const [index, session] of sessions.entries()) {
  const probed = values.split && index % 2 === 0;
  if (probed) report.probes.push({ index, hands: await get("/sessions/" + session + "/hands"), again: await get("/sessions/" + session + "/hands"), reader: await get("/sessions/" + session + "/turns/none") });
  report.cold.push({ index, probed, ...(await turn(session, "cold")) });
  report.warm.push({ index, ...(await turn(session, "warm")) });
  save();
}
report.summary = { first: summary(report.first), cold: summary(report.cold.filter(row => !row.probed)), warm: summary(report.warm) };
if (report.probes.length) report.summary.probed = { cold_after_reader: summary(report.cold.filter(row => row.probed)), hands: median(report.probes.map(probe => probe.hands.session_response)), hands_again: median(report.probes.map(probe => probe.again.session_response)), reader: median(report.probes.map(probe => probe.reader.session_response)) };
save();
console.log(JSON.stringify({ file, summary: report.summary }, null, 1));

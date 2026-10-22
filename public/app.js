import { Chat } from "./ui.js";
import { liveEvent, modelItems as models, serverEvents, thinkingItems as levels } from "./client.js";
import { harnesses as HARNESSES } from "./models.js";

const KEY = "nanocode-cloudflare-durable";
const TOKEN = `${KEY}:token`;
const RECONNECT_MS = 1000;
const CRASH_AFTER_MS = 1500;
const WARM_MS = 10000;
const MODELS = HARNESSES.flatMap(harness => harness.models);
const COMMANDS = [
  { label: "/model", detail: "choose harness, model and thinking for a new session" },
  { label: "/effort", detail: "choose the thinking of the next turns" },
  { label: "/new", detail: "start a new session" },
];

let state = load();
let warmed;
let used = 0;
let queued = [];

const access = document.createElement("label");
access.className = "row";
access.innerHTML = '<span aria-hidden="true" class="chevron">#</span><input aria-label="Access token" class="input" placeholder="Access token" type="password" />';
const token = access.querySelector("input");

const reset = document.createElement("button");
reset.className = "suggestion";
reset.type = "button";
reset.textContent = "New session";

const chat = new Chat(document.getElementById("chat"), {
  submit: text => void ask(text),
  input: warm,
  commands: COMMANDS,
  entries: state.entries,
  save: entries => {
    state.entries = [...entries];
    save();
  },
  highlight: () => import("./syntax.js").then(module => module.highlight),
  empty: "Ask a question to start a turn, or type /model to choose a model.",
  suggestions: [
    "Charge me $42",
    "List your tools",
    { label: "Kill the next tool call", run: () => void crash(), enabled: () => Boolean(state.session) },
  ],
  link: { href: "https://nanocode.xyz/", label: "nanocode.xyz →" },
});
chat.extras.append(access);
chat.actions.append(reset);

function load() {
  const saved = JSON.parse(localStorage.getItem(KEY) ?? "{}");
  return { entries: [], ...saved };
}

function save() {
  localStorage.setItem(KEY, JSON.stringify(state));
}

function identifier(prefix) {
  const bytes = crypto.getRandomValues(new Uint8Array(12));
  return `${prefix}-${Array.from(bytes, byte => byte.toString(16).padStart(2, "0")).join("")}`;
}

function current() {
  return MODELS.find(model => model.id === state.model) ?? MODELS[0];
}

function harnessOf(model) {
  return HARNESSES.find(harness => harness.models.includes(model));
}

function render() {
  const model = current();
  chat.status = `${harnessOf(model).label} · ${model.label} · ${state.thinking ?? model.effort} · ${state.session ?? "new session"} · ${chat.busy ? "running" : "idle"}`;
  reset.disabled = chat.busy;
  chat.refresh();
}

function setBusy(value) {
  chat.busy = value;
  render();
}

function note(kind, text) {
  chat.note(kind, text);
}

function openModels() {
  chat.pick({ title: "Model", filter: true, items: query => models(HARNESSES, current().id, query), choose: item => pick(item.label) });
}

function openThinking(id) {
  const model = MODELS.find(entry => entry.id === id);
  chat.pick({ title: `Thinking · ${id}`, items: () => levels(model, id === current().id ? state.thinking : undefined), choose: item => think(id, item.label) });
}

function pick(model) {
  if (state.session && model !== current().id) return note("tool", "the model is fixed for this session; use /new before choosing another model");
  openThinking(model);
}

function think(model, level) {
  state.model = model;
  state.thinking = level;
  save();
  render();
  note("tool", `using ${model} · thinking ${level}`);
}

function receive(logged, requestId) {
  const body = logged.event ?? {};
  if (body.source === "agent") {
    const event = liveEvent(logged);
    if (event) chat.push(event);
    return false;
  }
  if (logged.request_id !== requestId) return false;
  if (body.type === "interrupted") note("tool", "The session object reset. Its alarm started it again, and the turn continues from its checkpoint.");
  if (body.type !== "retrying" && body.type !== "result") return false;
  chat.push(liveEvent(logged));
  return body.type === "result";
}

async function follow(response, requestId) {
  if (!response.ok || !response.body) {
    note("error", `${response.status} ${await response.text()}`);
    return true;
  }
  let settled = false;
  try {
    for await (const message of serverEvents(response.body)) {
      if (message.id) {
        state.cursor = Number(message.id);
        save();
      }
      if (message.data && receive(JSON.parse(message.data), requestId)) {
        settled = true;
        break;
      }
    }
  } catch {
    note("tool", "The stream dropped. Reconnecting from the last event.");
    return false;
  }
  if (settled) void response.body.cancel().catch(() => undefined);
  return settled;
}

async function configure() {
  const open = await fetch("health").catch(() => undefined);
  access.hidden = open?.ok === true;
  const health = open?.ok ? open : await fetch("health", { headers: headers() }).catch(() => undefined);
  if (health?.ok && !MODELS.some(entry => entry.id === state.model)) {
    const configured = (await health.json()).models.claude;
    const model = MODELS.find(entry => entry.id === configured) ?? MODELS[0];
    if (model) {
      state.model = model.id;
      if (!model.efforts.includes(state.thinking)) state.thinking = model.efforts.includes("low") ? "low" : model.effort;
      save();
    }
  }
  render();
}

function headers(extra = {}) {
  return { ...extra, ...(token.value ? { authorization: `Bearer ${token.value}` } : {}) };
}

async function reconnect(requestId) {
  for (;;) {
    const response = await fetch(`sessions/${encodeURIComponent(state.session)}/events?after=${state.cursor ?? 0}`, { headers: headers({ accept: "text/event-stream" }) }).catch(() => undefined);
    if (response && await follow(response, requestId)) return;
    await new Promise(resolve => setTimeout(resolve, RECONNECT_MS));
  }
}

function clear() {
  queued = [];
  warmed = undefined;
  used = 0;
  const { model, thinking } = state;
  localStorage.removeItem(KEY);
  state = { ...load(), model, thinking };
  chat.clear();
  save();
  render();
}

function command(text) {
  const [name, ...words] = text.split(/\s+/);
  const argument = words.join(" ").toLowerCase();
  if (name === "/new") {
    clear();
    note("tool", "new session");
    return true;
  }
  if (name === "/model") {
    if (!argument) {
      openModels();
      return true;
    }
    const match = MODELS.find(model => argument.split(" ").every(word => `${model.id} ${model.name}`.toLowerCase().includes(word)));
    if (match) pick(match.id);
    else note("error", `no model matches "${argument}"; choose one of ${MODELS.map(model => model.id).join(", ")}`);
    return true;
  }
  if (name !== "/effort") return false;
  const model = current();
  if (!argument) {
    openThinking(model.id);
    return true;
  }
  if (model.efforts.includes(argument)) think(model.id, argument);
  else note("error", `${model.id} takes ${model.efforts.join("/")} thinking, not ${argument}`);
  return true;
}

async function steer(text) {
  note("user", text);
  const model = current();
  const response = await fetch(`sessions/${encodeURIComponent(state.session)}/turns`, { method: "POST", headers: headers({ "content-type": "application/json" }), body: JSON.stringify({ request_id: identifier("steer"), prompt: text, provider: harnessOf(model).id, model: model.id, steer: true }) }).catch(error => error);
  if (response instanceof Error) return note("error", response.message);
  if (response.ok) return note("tool", "steered into the running turn");
  if (response.status === 409) return queued.push(text);
  note("error", `${response.status} ${await response.text()}`);
}

async function ask(text, echoed = false) {
  if (!text || command(text)) return;
  if (chat.busy) return void steer(text);
  state.session ??= warmed ?? identifier("web");
  used = performance.now();
  const requestId = identifier("turn");
  state.open = requestId;
  if (!echoed) note("user", text);
  setBusy(true);
  const model = current();
  try {
    const response = await fetch(`sessions/${encodeURIComponent(state.session)}/turns?after=${state.cursor ?? 0}`, { method: "POST", headers: headers({ accept: "text/event-stream", "content-type": "application/json" }), body: JSON.stringify({ request_id: requestId, prompt: text, provider: harnessOf(model).id, model: model.id, thinking: state.thinking ?? model.effort }) });
    if (!(await follow(response, requestId))) await reconnect(requestId);
  } catch (error) {
    note("error", error instanceof Error ? error.message : String(error));
  } finally {
    chat.run(() => {
      state.open = undefined;
      used = performance.now();
      save();
      setBusy(false);
      const next = queued.shift();
      if (next) void ask(next, true);
    });
  }
}

function warm(text) {
  if (chat.busy || !text || text.startsWith("/") || (used && performance.now() - used < WARM_MS)) return;
  used = performance.now();
  const session = state.session ?? (warmed ??= identifier("web"));
  const model = current();
  void fetch(`sessions/${encodeURIComponent(session)}/warm`, { method: "POST", headers: headers({ "content-type": "application/json" }), body: JSON.stringify({ provider: harnessOf(model).id, model: model.id }) }).catch(() => undefined);
}

async function crash() {
  if (!state.session) return note("error", "Start a session before arming a crash.");
  const response = await fetch(`sessions/${encodeURIComponent(state.session)}/crash`, { method: "POST", headers: headers({ "content-type": "application/json" }), body: JSON.stringify({ after_ms: CRASH_AFTER_MS }) });
  if (response.ok) note("tool", "Armed: the next tool call resets its Durable Object 1.5 s in.");
  else note("error", `${response.status} ${await response.text()}`);
}

token.value = localStorage.getItem(TOKEN) ?? "";
token.addEventListener("input", () => localStorage.setItem(TOKEN, token.value));
token.addEventListener("change", () => void configure());
reset.addEventListener("click", () => clear());

render();
void configure();
if (state.open && state.session) {
  setBusy(true);
  void reconnect(state.open).finally(() => chat.run(() => {
    state.open = undefined;
    save();
    setBusy(false);
  }));
}

import { liveEvent, modelItems as models, serverEvents, thinkingItems as levels, Transcript } from "./client.js";
import { harnesses as HARNESSES } from "./models.js";

const KEY = "nanocode-cloudflare-durable";
const TOKEN = `${KEY}:token`;
const RECONNECT_MS = 1000;
const TYPING_DELAY_MS = 500;
const CRASH_AFTER_MS = 1500;
const WARM_MS = 10000;
const MODELS = HARNESSES.flatMap(harness => harness.models);
const COMMANDS = [
  { label: "/model", detail: "choose harness, model and thinking for a new session" },
  { label: "/effort", detail: "choose the thinking of the next turns" },
  { label: "/new", detail: "start a new session" },
];

const elements = {
  access: document.getElementById("access"),
  chat: document.getElementById("chat"),
  crash: document.getElementById("crash"),
  form: document.getElementById("form"),
  log: document.getElementById("log"),
  picker: document.getElementById("picker"),
  prompt: document.getElementById("prompt"),
  reset: document.getElementById("reset"),
  send: document.getElementById("send"),
  status: document.getElementById("status"),
  suggestions: [...document.querySelectorAll("[data-prompt]")],
  token: document.getElementById("token"),
  typing: document.getElementById("typing"),
};

let state = load();
let busy = false;
let typing = false;
let typingTimer;
let picker = null;
let highlight = 0;
let warmed;
let used = 0;
let queued = [];
let live;
const transcript = new Transcript({
  add,
  live: entry => {
    live = entry ?? undefined;
    render();
  },
  reducedMotion: () => matchMedia("(prefers-reduced-motion: reduce)").matches,
});

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
  const shown = live ? [...state.entries, live] : state.entries;
  elements.log.replaceChildren(...(shown.length === 0 ? [paragraph("entry entry-tool", "Ask a question to start a turn, or type /model to choose a model.")] : shown.map((entry, index) => {
    const previous = shown[index - 1];
    const label = entry.kind === "user" ? "you" : !previous || previous.kind === "user" ? "agent" : undefined;
    const turn = document.createElement("div");
    turn.className = "turn";
    if (label) {
      const speaker = paragraph(`speaker speaker-${label}`, ` ${label}`);
      const dot = document.createElement("span");
      dot.ariaHidden = "true";
      dot.textContent = "●";
      speaker.prepend(dot);
      turn.append(speaker);
    }
    turn.append(paragraph(`entry entry-${entry.kind}${entry === live ? " entry-live" : ""}`, entry.kind === "reasoning" ? entry.text.replace(/\*\*(.+?)\*\*/g, "$1") : entry.text));
    return turn;
  })));
  elements.log.scrollTo({ top: elements.log.scrollHeight });
  elements.typing.className = typing ? "typing typing-visible" : "typing";
  if (typing) {
    const speaker = document.createElement("span");
    speaker.className = "speaker speaker-agent";
    speaker.textContent = "agent";
    elements.typing.replaceChildren(speaker, live?.kind === "reasoning" ? " is thinking" : " is typing");
  } else elements.typing.replaceChildren();
  const model = current();
  elements.status.textContent = `${harnessOf(model).label} · ${model.label} · ${state.thinking ?? model.effort} · ${state.session ?? "new session"} · ${busy ? "running" : "idle"}`;
  renderMenu();
  for (const control of [elements.reset, ...elements.suggestions]) control.disabled = busy;
  elements.crash.disabled = !state.session;
}

function menuItems() {
  if (picker?.kind === "model") return modelItems(elements.prompt.value);
  if (picker?.kind === "thinking") return thinkingItems(picker.model);
  const query = elements.prompt.value;
  if (!/^\/\S*$/.test(query)) return [];
  return COMMANDS.filter(entry => entry.label.startsWith(query)).map(entry => ({ kind: "choice", ...entry }));
}

function modelItems(query) {
  return models(HARNESSES, current().id, query);
}

function thinkingItems(id) {
  return levels(MODELS.find(entry => entry.id === id), id === current().id ? state.thinking : undefined);
}

function renderMenu() {
  const items = menuItems();
  const choices = items.filter(item => item.kind === "choice");
  highlight = Math.min(highlight, Math.max(0, choices.length - 1));
  const open = picker !== null || items.length > 0;
  elements.picker.hidden = !open;
  elements.prompt.placeholder = picker?.kind === "model" ? "type to filter" : busy ? "Steer the running turn" : "Ask the agent, or type / for commands";
  if (!open) return elements.picker.replaceChildren();
  const width = Math.max(0, ...choices.map(item => item.label.length));
  const children = [];
  if (picker) children.push(paragraph("picker-title", picker.kind === "model" ? "Model" : `Thinking · ${picker.model}`));
  if (picker?.kind === "model") {
    const filter = paragraph("picker-muted", "filter: ");
    filter.append(span("picker-query", elements.prompt.value), span("picker-caret", "▏"));
    children.push(filter);
    if (choices.length === 0) children.push(paragraph("picker-muted", "  no matches"));
  }
  let position = -1;
  for (const item of items) {
    if (item.kind === "header") {
      children.push(paragraph("picker-header", item.label));
      continue;
    }
    position += 1;
    const index = position;
    const button = document.createElement("button");
    button.type = "button";
    button.className = "picker-choice";
    button.role = "option";
    button.ariaSelected = String(index === highlight);
    button.style.gridTemplateColumns = `2ch ${width}ch 2ch auto`;
    button.append(span("picker-marker", index === highlight ? "›" : ""), span("picker-label", item.label), span("picker-marker", item.current ? "✓" : ""), span("picker-detail", item.detail ?? ""));
    button.addEventListener("mousemove", () => {
      if (highlight === index) return;
      highlight = index;
      renderMenu();
    });
    button.addEventListener("click", () => choose(item));
    children.push(button);
  }
  if (picker) children.push(paragraph("picker-muted", picker.kind === "model" ? "type to filter · enter select · esc cancel" : "enter select · esc cancel"));
  elements.picker.ariaLabel = picker?.kind === "model" ? "Model" : picker ? "Thinking" : "Commands";
  elements.picker.replaceChildren(...children);
}

function span(className, text) {
  const element = document.createElement("span");
  element.className = className;
  element.textContent = text;
  return element;
}

function open(next) {
  elements.prompt.value = "";
  picker = next;
  const items = next.kind === "model" ? modelItems("") : thinkingItems(next.model);
  highlight = Math.max(0, items.filter(item => item.kind === "choice").findIndex(item => item.current));
  renderMenu();
}

function close() {
  picker = null;
  elements.prompt.value = "";
  renderMenu();
}

function pick(model) {
  if (state.session && model !== current().id) {
    close();
    note("tool", "the model is fixed for this session; use /new before choosing another model");
    return;
  }
  open({ kind: "thinking", model });
}

function think(model, level) {
  close();
  state.model = model;
  state.thinking = level;
  save();
  note("tool", `using ${model} · thinking ${level}`);
}

function choose(item) {
  if (picker?.kind === "model") pick(item.label);
  else if (picker?.kind === "thinking") think(picker.model, item.label);
  else command(item.label);
}

function paragraph(className, text) {
  const element = document.createElement("p");
  element.className = className;
  element.textContent = text;
  return element;
}

function add(entry) {
  state.entries.push(entry);
  save();
  render();
}

function setBusy(value) {
  busy = value;
  typing = false;
  clearTimeout(typingTimer);
  if (value) typingTimer = setTimeout(() => {
    typing = true;
    render();
  }, TYPING_DELAY_MS);
  render();
}

function note(kind, text) {
  transcript.run(() => add({ kind, text }));
}

function receive(logged, requestId) {
  const body = logged.event ?? {};
  if (body.source === "agent") {
    const event = liveEvent(logged);
    if (event) transcript.push(event);
    return false;
  }
  if (logged.request_id !== requestId) return false;
  if (body.type === "interrupted") note("tool", "The session object reset. Its alarm started it again, and the turn continues from its checkpoint.");
  if (body.type !== "retrying" && body.type !== "result") return false;
  transcript.push(liveEvent(logged));
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
  elements.access.hidden = open?.ok === true;
  const health = open?.ok ? open : await fetch("health", { headers: headers() }).catch(() => undefined);
  if (health?.ok && !MODELS.some(entry => entry.id === state.model)) {
    const configured = (await health.json()).models.claude;
    const model = MODELS.find(entry => entry.id === configured) ?? MODELS[0];
    if (model) {
      state.model = model.id;
      if (!model.efforts.includes(state.thinking)) state.thinking = model.effort;
      save();
    }
  }
  render();
}

function headers(extra = {}) {
  const token = elements.token.value;
  return { ...extra, ...(token ? { authorization: `Bearer ${token}` } : {}) };
}

async function reconnect(requestId) {
  for (;;) {
    const response = await fetch(`sessions/${encodeURIComponent(state.session)}/events?after=${state.cursor ?? 0}`, { headers: headers({ accept: "text/event-stream" }) }).catch(() => undefined);
    if (response && await follow(response, requestId)) return;
    await new Promise(resolve => setTimeout(resolve, RECONNECT_MS));
  }
}

function reset() {
  transcript.reset();
  live = undefined;
  picker = null;
  queued = [];
  warmed = undefined;
  used = 0;
  const { model, thinking } = state;
  localStorage.removeItem(KEY);
  state = { ...load(), model, thinking };
  save();
  render();
}

function command(text) {
  const [name, ...words] = text.split(/\s+/);
  const argument = words.join(" ").toLowerCase();
  if (name === "/new") {
    elements.prompt.value = "";
    reset();
    note("tool", "new session");
    return true;
  }
  if (name === "/model") {
    if (!argument) {
      open({ kind: "model" });
      return true;
    }
    const match = MODELS.find(model => argument.split(" ").every(word => `${model.id} ${model.name}`.toLowerCase().includes(word)));
    elements.prompt.value = "";
    if (match) pick(match.id);
    else note("error", `no model matches "${argument}"; choose one of ${MODELS.map(model => model.id).join(", ")}`);
    return true;
  }
  if (name !== "/effort") return false;
  const model = current();
  if (!argument) {
    open({ kind: "thinking", model: model.id });
    return true;
  }
  elements.prompt.value = "";
  if (model.efforts.includes(argument)) think(model.id, argument);
  else note("error", `${model.id} takes ${model.efforts.join("/")} thinking, not ${argument}`);
  return true;
}

async function steer(text) {
  elements.prompt.value = "";
  renderMenu();
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
  if (busy) return void steer(text);
  state.session ??= warmed ?? identifier("web");
  used = performance.now();
  const requestId = identifier("turn");
  state.open = requestId;
  elements.prompt.value = "";
  if (!echoed) note("user", text);
  setBusy(true);
  const model = current();
  try {
    const response = await fetch(`sessions/${encodeURIComponent(state.session)}/turns?after=${state.cursor ?? 0}`, { method: "POST", headers: headers({ accept: "text/event-stream", "content-type": "application/json" }), body: JSON.stringify({ request_id: requestId, prompt: text, provider: harnessOf(model).id, model: model.id, thinking: state.thinking ?? model.effort }) });
    if (!(await follow(response, requestId))) await reconnect(requestId);
  } catch (error) {
    note("error", error instanceof Error ? error.message : String(error));
  } finally {
    transcript.run(() => {
      state.open = undefined;
      used = performance.now();
      save();
      setBusy(false);
      const next = queued.shift();
      if (next) void ask(next, true);
    });
  }
}

async function crash() {
  if (!state.session) return note("error", "Start a session before arming a crash.");
  const response = await fetch(`sessions/${encodeURIComponent(state.session)}/crash`, { method: "POST", headers: headers({ "content-type": "application/json" }), body: JSON.stringify({ after_ms: CRASH_AFTER_MS }) });
  if (response.ok) note("tool", "Armed: the next tool call resets its Durable Object 1.5 s in.");
  else note("error", `${response.status} ${await response.text()}`);
}


elements.token.value = localStorage.getItem(TOKEN) ?? "";
elements.token.addEventListener("input", () => localStorage.setItem(TOKEN, elements.token.value));
elements.token.addEventListener("change", () => void configure());
elements.form.addEventListener("submit", event => {
  event.preventDefault();
  void ask(elements.prompt.value.trim());
});
elements.prompt.addEventListener("input", () => {
  if (picker?.kind === "model") highlight = 0;
  renderMenu();
  if (busy || elements.prompt.value.startsWith("/") || (used && performance.now() - used < WARM_MS)) return;
  used = performance.now();
  const session = state.session ?? (warmed ??= identifier("web"));
  const model = current();
  void fetch(`sessions/${encodeURIComponent(session)}/warm`, { method: "POST", headers: headers({ "content-type": "application/json" }), body: JSON.stringify({ provider: harnessOf(model).id, model: model.id }) }).catch(() => undefined);
});
elements.prompt.addEventListener("keydown", event => {
  const choices = menuItems().filter(item => item.kind === "choice");
  if (picker || choices.length > 0) {
    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      highlight = Math.min(Math.max(0, highlight + (event.key === "ArrowDown" ? 1 : -1)), Math.max(0, choices.length - 1));
      renderMenu();
      return;
    }
    if (event.key === "Escape") {
      event.preventDefault();
      close();
      return;
    }
    if ((event.key === "Enter" && !event.shiftKey) || (event.key === "Tab" && !picker)) {
      event.preventDefault();
      if (choices[highlight]) choose(choices[highlight]);
      return;
    }
  }
  if (event.key !== "Enter" || event.shiftKey) return;
  event.preventDefault();
  elements.form.requestSubmit();
});
for (const suggestion of elements.suggestions) suggestion.addEventListener("click", () => void ask(suggestion.dataset.prompt));
elements.crash.addEventListener("click", () => void crash());
elements.reset.addEventListener("click", () => reset());
elements.chat.addEventListener("click", event => {
  if (event.target.closest("a, button, input, textarea") || getSelection()?.toString()) return;
  elements.prompt.focus();
});

render();
void configure();
if (state.open && state.session) {
  setBusy(true);
  void reconnect(state.open).finally(() => transcript.run(() => {
    state.open = undefined;
    save();
    setBusy(false);
  }));
}

const KEY = "nanocode-cloudflare-durable";
const TOKEN = `${KEY}:token`;
const RECONNECT_MS = 1000;
const RESULT_CHARACTERS = 160;
const TYPING_DELAY_MS = 500;
const CRASH_AFTER_MS = 1500;
const REVEAL_MS = 30;
const WARM_MS = 10000;
const FRAME_MS = 16;
const EVERY = ["none", "low", "medium", "high", "xhigh", "max"];
const THINKING = ["low", "medium", "high", "xhigh", "max"];
const HARNESSES = [
  { id: "claude", label: "Claude", models: [{ id: "claude-haiku-5-5", label: "Haiku 5.5", name: "Claude Haiku 5.5", efforts: EVERY, effort: "medium" }, { id: "claude-sonnet-5-5", label: "Sonnet 5.5", name: "Claude Sonnet 5.5", efforts: THINKING, effort: "high" }] },
  { id: "codex", label: "Codex", models: [{ id: "gpt-6-luna", label: "Luna", name: "GPT-6 Luna", efforts: EVERY, effort: "medium" }, { id: "gpt-6.1-sol", label: "Sol", name: "GPT-6.1 Sol", efforts: THINKING, effort: "medium" }, { id: "glm-5.3", label: "GLM-5.3", name: "GLM-5.3 on Workers AI", efforts: ["low", "medium", "high"], effort: "low" }] },
];
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
let stream = { posted: 0, shown: 0, text: "" };
let live;
let queue = [];
let frame = 0;
let clock = 0;

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
  const words = query.toLowerCase().split(/\s+/).filter(Boolean);
  return HARNESSES.flatMap(harness => {
    const models = harness.models.map(model => ({ kind: "choice", label: model.id, detail: `${model.name} · thinking ${model.efforts.join("/")}`, current: model.id === current().id })).filter(item => words.every(word => `${item.label} ${item.detail}`.toLowerCase().includes(word)));
    return models.length ? [{ kind: "header", label: harness.label }, ...models] : [];
  });
}

function thinkingItems(id) {
  const model = MODELS.find(entry => entry.id === id);
  const selected = id === current().id && model.efforts.includes(state.thinking) ? state.thinking : model.effort;
  return model.efforts.map(level => ({ kind: "choice", label: level, detail: level === model.effort ? "default" : "", current: level === selected }));
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

function flush() {
  const { kind, posted, text } = stream;
  if (kind === "assistant") for (const chunk of paragraphs(text, posted, true).chunks) state.entries.push({ kind, text: chunk });
  else if (kind === "reasoning" && text.trim()) state.entries.push({ kind, text: text.trim() });
  stream = { posted: 0, shown: 0, text: "" };
  live = undefined;
  save();
  render();
}

function reveal() {
  const visible = stream.text.slice(0, stream.shown);
  if (stream.kind === "assistant") {
    const { chunks, next } = paragraphs(visible, stream.posted);
    stream.posted = next;
    for (const text of chunks) state.entries.push({ kind: "assistant", text });
    if (chunks.length) save();
  }
  const rest = visible.slice(stream.posted).trim();
  live = rest ? { kind: stream.kind, text: rest } : undefined;
  render();
}

function step() {
  frame = 0;
  const now = performance.now();
  const elapsed = clock ? now - clock : 0;
  clock = now;
  const instant = matchMedia("(prefers-reduced-motion: reduce)").matches;
  let revealed = false;
  for (;;) {
    const next = queue[0];
    if (next?.delta !== undefined && (!stream.kind || (stream.kind === next.kind && stream.item === next.item))) {
      queue.shift();
      stream.kind = next.kind;
      stream.item = next.item;
      stream.text += next.delta;
      continue;
    }
    if (stream.shown < stream.text.length) {
      if (revealed) break;
      revealed = true;
      const backlog = stream.text.length - stream.shown;
      stream.shown += instant ? backlog : Math.max(1, Math.ceil(backlog * (1 - Math.exp(-elapsed / REVEAL_MS))));
      reveal();
      continue;
    }
    if (!next) break;
    queue.shift();
    flush();
    if (next.delta !== undefined) {
      stream.kind = next.kind;
      stream.item = next.item;
      stream.text = next.delta;
    } else next.run();
  }
  if (queue.length || stream.shown < stream.text.length) frame = setTimeout(step, FRAME_MS);
  else clock = 0;
}

function enqueue(item) {
  queue.push(item);
  if (!frame) frame = setTimeout(step);
}

function note(kind, text) {
  enqueue({ run: () => add({ kind, text }) });
}

function receive(logged, requestId) {
  const body = logged.event ?? {};
  if (body.source === "agent" && body.event) {
    const { type, payload } = body.event;
    if (type === "assistant.delta") enqueue({ kind: "assistant", item: payload.item_id, delta: payload.text ?? "" });
    else if (type === "reasoning.summary.delta") enqueue({ kind: "reasoning", item: `reasoning:${payload.model_call_index}`, delta: payload.text ?? "" });
    else if (type === "tool.call") note("tool", `${payload.tool} ${JSON.stringify(payload.arguments)}`);
    else if (type === "tool.result") note("tool", `${payload.tool} ${payload.status}: ${clip(typeof payload.result === "string" ? payload.result : payload.result?.text ?? "")}`);
    return false;
  }
  if (logged.request_id !== requestId) return false;
  if (body.type === "retrying") note("tool", `Retrying: ${body.error ?? ""}`);
  if (body.type === "interrupted") note("tool", "The session object reset. Its alarm started it again, and the turn continues from its checkpoint.");
  if (body.type !== "result") return false;
  if (body.status !== "completed") note("error", `The turn ${body.status}: ${JSON.stringify(body.error ?? "")}`);
  else enqueue({ run: () => {} });
  return true;
}

async function follow(response, requestId) {
  if (!response.ok || !response.body) {
    note("error", `${response.status} ${await response.text()}`);
    return true;
  }
  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
  let buffered = "";
  for (;;) {
    const read = await reader.read().catch(() => undefined);
    if (!read) {
      note("tool", "The stream dropped. Reconnecting from the last event.");
      return false;
    }
    if (read.done) return false;
    buffered += read.value;
    let end;
    while ((end = buffered.indexOf("\n\n")) >= 0) {
      const block = buffered.slice(0, end);
      buffered = buffered.slice(end + 2);
      let data = "";
      for (const line of block.split("\n")) {
        if (line.startsWith("id:")) {
          state.cursor = Number(line.slice(3).trim());
          save();
        } else if (line.startsWith("data:")) data += line.slice(5).trim();
      }
      if (data && receive(JSON.parse(data), requestId)) {
        void reader.cancel().catch(() => undefined);
        return true;
      }
    }
  }
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
  clearTimeout(frame);
  frame = 0;
  clock = 0;
  queue = [];
  stream = { posted: 0, shown: 0, text: "" };
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
  if (!echoed) add({ kind: "user", text });
  setBusy(true);
  const model = current();
  try {
    const response = await fetch(`sessions/${encodeURIComponent(state.session)}/turns?after=${state.cursor ?? 0}`, { method: "POST", headers: headers({ accept: "text/event-stream", "content-type": "application/json" }), body: JSON.stringify({ request_id: requestId, prompt: text, provider: harnessOf(model).id, model: model.id, thinking: state.thinking ?? model.effort }) });
    if (!(await follow(response, requestId))) await reconnect(requestId);
  } catch (error) {
    note("error", error instanceof Error ? error.message : String(error));
  } finally {
    enqueue({
      run: () => {
        state.open = undefined;
        used = performance.now();
        save();
        setBusy(false);
        const next = queued.shift();
        if (next) void ask(next, true);
      },
    });
  }
}

async function crash() {
  if (!state.session) return note("error", "Start a session before arming a crash.");
  const response = await fetch(`sessions/${encodeURIComponent(state.session)}/crash`, { method: "POST", headers: headers({ "content-type": "application/json" }), body: JSON.stringify({ after_ms: CRASH_AFTER_MS }) });
  if (response.ok) note("tool", "Armed: the next tool call resets its Durable Object 1.5 s in.");
  else note("error", `${response.status} ${await response.text()}`);
}

function paragraphs(text, from, final = false) {
  const chunks = [];
  let start = from;
  let fenced = (text.slice(0, from).match(/^```/gm)?.length ?? 0) % 2 === 1;
  let index = from;
  while (index < text.length) {
    const line = text.indexOf("\n", index);
    if (line === -1) break;
    if (text.startsWith("```", index)) fenced = !fenced;
    if (!fenced && text.startsWith("\n", line + 1)) {
      const chunk = text.slice(start, line).trim();
      if (chunk) chunks.push(chunk);
      start = line + 2;
    }
    index = line + 1;
  }
  if (!final) return { chunks, next: start };
  const rest = text.slice(start).trim();
  if (rest) chunks.push(rest);
  return { chunks, next: text.length };
}

function clip(text) {
  return text.length > RESULT_CHARACTERS ? `${text.slice(0, RESULT_CHARACTERS)}…` : text;
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
  void reconnect(state.open).finally(() => enqueue({
    run: () => {
      state.open = undefined;
      save();
      setBusy(false);
    },
  }));
}

const KEY = "nanocode-cloudflare-durable";
const TOKEN = `${KEY}:token`;
const RECONNECT_MS = 1000;
const RESULT_CHARACTERS = 160;
const TYPING_DELAY_MS = 500;
const CRASH_AFTER_MS = 1500;
const REVEAL_MS = 100;
const FRAME_MS = 16;
const MODELS = [
  { id: "claude-haiku-5-5", label: "Haiku 5.5", efforts: ["none", "low", "medium", "high", "xhigh", "max"], effort: "medium" },
  { id: "claude-sonnet-5-5", label: "Sonnet 5.5", efforts: ["low", "medium", "high", "xhigh", "max"], effort: "high" },
  { id: "claude-opus-5-5", label: "Opus 5.5", efforts: ["low", "medium", "high", "xhigh", "max"], effort: "medium" },
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
let choosing = false;
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

function render() {
  const shown = live ? [...state.entries, live] : state.entries;
  elements.log.replaceChildren(...(shown.length === 0 ? [paragraph("entry entry-tool", "Pick a model and an effort, then ask a question to start a turn.")] : shown.map((entry, index) => {
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
  elements.status.textContent = `Claude · ${model.label} · ${state.thinking ?? model.effort} · ${state.session ?? "new session"} · ${busy ? "running" : "idle"}`;
  renderPicker(model);
  for (const control of [elements.prompt, elements.send, elements.reset, ...elements.suggestions]) control.disabled = busy;
  elements.crash.disabled = !state.session;
}

function renderPicker(model) {
  const open = !state.session || choosing;
  elements.picker.hidden = !open;
  if (!open) return;
  const chevron = document.createElement("span");
  chevron.ariaHidden = "true";
  chevron.className = "chevron";
  chevron.textContent = "❯";
  const children = [chevron];
  if (!state.session) {
    children.push(label("model"), ...MODELS.map(entry => option(entry.label, entry.id === model.id, () => {
      state.model = entry.id;
      if (!entry.efforts.includes(state.thinking)) state.thinking = entry.effort;
      save();
      render();
    })));
  }
  children.push(label("effort"), ...model.efforts.map(level => option(level, level === (state.thinking ?? model.effort), () => {
    choosing = false;
    effort(level);
  })));
  elements.picker.replaceChildren(...children);
}

function label(text) {
  const element = document.createElement("span");
  element.className = "picker-label";
  element.textContent = text;
  return element;
}

function option(text, pressed, choose) {
  const element = document.createElement("button");
  element.className = "option";
  element.type = "button";
  element.disabled = busy;
  element.ariaPressed = String(pressed);
  element.textContent = text;
  element.addEventListener("click", choose);
  return element;
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
    if (next?.delta !== undefined && (!stream.kind || stream.kind === next.kind)) {
      queue.shift();
      stream.kind = next.kind;
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
    if (type === "assistant.delta") enqueue({ kind: "assistant", delta: payload.text ?? "" });
    else if (type === "reasoning.summary.delta") enqueue({ kind: "reasoning", delta: payload.text ?? "" });
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
  if (health?.ok && !state.model) {
    const configured = (await health.json()).models.claude;
    const model = MODELS.find(entry => entry.id === configured);
    if (model) {
      state.model = model.id;
      state.thinking ??= model.effort;
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
  choosing = false;
  const { model, thinking } = state;
  localStorage.removeItem(KEY);
  state = { ...load(), model, thinking };
  save();
  render();
}

function chooseModel(query) {
  if (!query) {
    reset();
    note("tool", "Pick a model and an effort below. Your next prompt starts a new session.");
    return;
  }
  const words = query.split(" ");
  const match = MODELS.find(model => words.every(word => `claude ${model.id} ${model.label}`.toLowerCase().includes(word)));
  if (!match) {
    note("error", `No model matches "${query}". Choose from ${MODELS.map(model => model.label).join(", ")}.`);
    return;
  }
  const thinking = match.efforts.includes(state.thinking) ? state.thinking : match.effort;
  reset();
  state.model = match.id;
  state.thinking = thinking;
  save();
  note("tool", `Your next prompt starts a new Claude · ${match.label} session at ${thinking} effort.`);
}

function effort(level) {
  const model = current();
  if (!model.efforts.includes(level)) {
    note("error", `${model.label} takes ${model.efforts.join(", ")}, not ${level}.`);
    return;
  }
  state.thinking = level;
  save();
  note("tool", state.session ? `Reasoning effort is ${level} from the next turn.` : `Your next session starts at ${level} effort.`);
}

function command(text) {
  const [name, ...words] = text.split(/\s+/);
  if (name === "/model") chooseModel(words.join(" ").toLowerCase());
  else if (name === "/effort" && words[0]) {
    choosing = false;
    effort(words[0].toLowerCase());
  } else if (name === "/effort") {
    choosing = true;
    const model = current();
    note("tool", `Reasoning effort is ${state.thinking ?? model.effort}. Pick one below, or type /effort followed by ${model.efforts.join(", ")}.`);
  } else return false;
  elements.prompt.value = "";
  return true;
}

async function ask(text) {
  if (!text || busy || command(text)) return;
  state.session ??= identifier("web");
  const requestId = identifier("turn");
  state.open = requestId;
  elements.prompt.value = "";
  add({ kind: "user", text });
  setBusy(true);
  const model = current();
  try {
    const response = await fetch(`sessions/${encodeURIComponent(state.session)}/turns?after=${state.cursor ?? 0}`, { method: "POST", headers: headers({ accept: "text/event-stream", "content-type": "application/json" }), body: JSON.stringify({ request_id: requestId, prompt: text, model: model.id, thinking: state.thinking ?? model.effort }) });
    if (!(await follow(response, requestId))) await reconnect(requestId);
  } catch (error) {
    note("error", error instanceof Error ? error.message : String(error));
  } finally {
    enqueue({
      run: () => {
        state.open = undefined;
        save();
        setBusy(false);
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
elements.prompt.addEventListener("keydown", event => {
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

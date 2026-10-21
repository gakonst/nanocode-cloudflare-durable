const KEY = "nanocode-cloudflare-durable";
const TOKEN = `${KEY}:token`;
const RECONNECT_MS = 1000;
const RESULT_CHARACTERS = 160;
const TYPING_DELAY_MS = 500;
const CRASH_AFTER_MS = 1500;

const elements = {
  access: document.getElementById("access"),
  chat: document.getElementById("chat"),
  crash: document.getElementById("crash"),
  form: document.getElementById("form"),
  log: document.getElementById("log"),
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
let model;
let typingTimer;
let reply = { posted: 0, text: "" };

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

function render() {
  elements.log.replaceChildren(...(state.entries.length === 0 ? [paragraph("entry entry-tool", "Ask a question to start a turn.")] : state.entries.map((entry, index) => {
    const previous = state.entries[index - 1];
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
    turn.append(paragraph(`entry entry-${entry.kind}`, entry.text));
    return turn;
  })));
  elements.log.scrollTo({ top: elements.log.scrollHeight });
  elements.status.textContent = `${model ?? "Claude"} · ${state.session ?? "new session"} · ${busy ? "running" : "idle"}`;
  for (const control of [elements.prompt, elements.send, elements.reset, ...elements.suggestions]) control.disabled = busy;
  elements.crash.disabled = !state.session;
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
  clearTimeout(typingTimer);
  elements.typing.className = "typing";
  elements.typing.replaceChildren();
  if (value) typingTimer = setTimeout(() => {
    elements.typing.className = "typing typing-visible";
    const speaker = document.createElement("span");
    speaker.className = "speaker speaker-agent";
    speaker.textContent = "agent";
    elements.typing.replaceChildren(speaker, " is typing");
  }, TYPING_DELAY_MS);
  render();
}

function flush() {
  const { chunks } = paragraphs(reply.text, reply.posted, true);
  reply = { posted: 0, text: "" };
  for (const text of chunks) add({ kind: "assistant", text });
}

function apply(logged, requestId) {
  const body = logged.event ?? {};
  if (body.source === "agent" && body.event) {
    const { type, payload } = body.event;
    if (type === "assistant.delta") {
      reply.text += payload.text ?? "";
      const { chunks, next } = paragraphs(reply.text, reply.posted);
      reply.posted = next;
      for (const text of chunks) add({ kind: "assistant", text });
      return false;
    }
    if (type === "tool.call") {
      flush();
      add({ kind: "tool", text: `${payload.tool} ${JSON.stringify(payload.arguments)}` });
    } else if (type === "tool.result") {
      flush();
      const result = typeof payload.result === "string" ? payload.result : payload.result?.text ?? "";
      add({ kind: "tool", text: `${payload.tool} ${payload.status}: ${clip(result)}` });
    }
    return false;
  }
  if (logged.request_id !== requestId) return false;
  if (body.type === "retrying") add({ kind: "tool", text: `Retrying: ${body.error ?? ""}` });
  if (body.type === "interrupted") add({ kind: "tool", text: "The session object reset. Its alarm started it again, and the turn continues from its checkpoint." });
  if (body.type !== "result") return false;
  flush();
  if (body.status !== "completed") add({ kind: "error", text: `The turn ${body.status}: ${JSON.stringify(body.error ?? "")}` });
  return true;
}

async function follow(response, requestId) {
  if (!response.ok || !response.body) {
    add({ kind: "error", text: `${response.status} ${await response.text()}` });
    return true;
  }
  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
  let buffered = "";
  for (;;) {
    const read = await reader.read().catch(() => undefined);
    if (!read) {
      flush();
      add({ kind: "tool", text: "The stream dropped. Reconnecting from the last event." });
      return false;
    }
    if (read.done) {
      flush();
      return false;
    }
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
      if (data && apply(JSON.parse(data), requestId)) {
        void reader.cancel().catch(() => undefined);
        return true;
      }
    }
  }
}

async function configure() {
  const open = await fetch("/health").catch(() => undefined);
  elements.access.hidden = open?.ok === true;
  const health = open?.ok ? open : await fetch("/health", { headers: headers() }).catch(() => undefined);
  if (health?.ok) model = (await health.json()).models.claude;
  render();
}

function headers(extra = {}) {
  const token = elements.token.value;
  return { ...extra, ...(token ? { authorization: `Bearer ${token}` } : {}) };
}

async function reconnect(requestId) {
  for (;;) {
    const response = await fetch(`/sessions/${encodeURIComponent(state.session)}/events?after=${state.cursor ?? 0}`, { headers: headers({ accept: "text/event-stream" }) }).catch(() => undefined);
    if (response && await follow(response, requestId)) return;
    await new Promise(resolve => setTimeout(resolve, RECONNECT_MS));
  }
}

async function ask(text) {
  if (!text || busy) return;
  state.session ??= identifier("web");
  const requestId = identifier("turn");
  state.open = requestId;
  elements.prompt.value = "";
  add({ kind: "user", text });
  setBusy(true);
  try {
    const response = await fetch(`/sessions/${encodeURIComponent(state.session)}/turns?after=${state.cursor ?? 0}`, { method: "POST", headers: headers({ accept: "text/event-stream", "content-type": "application/json" }), body: JSON.stringify({ request_id: requestId, prompt: text }) });
    if (!(await follow(response, requestId))) await reconnect(requestId);
  } catch (error) {
    add({ kind: "error", text: error instanceof Error ? error.message : String(error) });
  } finally {
    state.open = undefined;
    save();
    setBusy(false);
  }
}

async function crash() {
  if (!state.session) return add({ kind: "error", text: "Start a session before arming a crash." });
  const response = await fetch(`/sessions/${encodeURIComponent(state.session)}/crash`, { method: "POST", headers: headers({ "content-type": "application/json" }), body: JSON.stringify({ after_ms: CRASH_AFTER_MS }) });
  add(response.ok ? { kind: "tool", text: "Armed: the next tool call resets its Durable Object 1.5 s in." } : { kind: "error", text: `${response.status} ${await response.text()}` });
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
elements.reset.addEventListener("click", () => {
  localStorage.removeItem(KEY);
  state = load();
  render();
});
elements.chat.addEventListener("click", event => {
  if (event.target.closest("a, button, input, textarea") || getSelection()?.toString()) return;
  elements.prompt.focus();
});

render();
void configure();
if (state.open && state.session) {
  setBusy(true);
  void reconnect(state.open).finally(() => {
    state.open = undefined;
    save();
    setBusy(false);
  });
}

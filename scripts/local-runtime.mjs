import { createHash } from "node:crypto";
import { readFileSync, readdirSync, mkdirSync, mkdtempSync } from "node:fs";
import { resolve } from "node:path";

export const LOCAL_TOKEN = "local-measurement";
export const GATEWAY_KEY = "local-provider";

const GATEWAY = `export default { fetch(request) { return fetch(request); } };`;

function shim(entry, wallet) {
  return `import worker, { AgentSession as Session } from "./${entry}";
${wallet ? `export { Wallet } from "./${entry}";` : ""}
function gateway(upstream) {
  return {
    async run(request, options) {
      const route = request.provider === "anthropic" && request.endpoint === "v1/messages" ? { url: "https://provider.invalid/v1/messages", header: ["x-api-key", "${GATEWAY_KEY}"] }
        : request.provider === "openai" && request.endpoint === "responses" ? { url: "https://provider.invalid/openai/v1/responses", header: ["authorization", "Bearer ${GATEWAY_KEY}"] }
        : null;
      if (!route) return Response.json({ error: "unsupported gateway route" }, { status: 400 });
      const headers = new Headers(request.headers);
      if (headers.has("x-api-key") || headers.has("authorization")) return Response.json({ error: "the Worker sent a provider credential" }, { status: 400 });
      headers.set(...route.header);
      return upstream.fetch(new Request(route.url, { method: "POST", headers, body: JSON.stringify(request.query), signal: options?.signal }));
    },
    async getUrl(provider) { return "https://gateway.invalid/local" + (provider ? "/" + provider : ""); },
  };
}
function unified(upstream) {
  return async (model, inputs, options) => {
    if (!model.startsWith("anthropic/") || !options?.returnRawResponse || !options.gateway?.id) return Response.json({ error: "unsupported unified billing call" }, { status: 400 });
    const headers = new Headers(options.extraHeaders);
    if (headers.has("x-api-key") || headers.has("authorization")) return Response.json({ error: "the Worker sent a provider credential" }, { status: 400 });
    headers.set("x-api-key", "${GATEWAY_KEY}");
    const provider = model.slice("anthropic/".length).replaceAll(".", "-");
    return upstream.fetch(new Request("https://provider.invalid/v1/messages", { method: "POST", headers, body: JSON.stringify({ ...inputs, model: provider }), signal: options.signal }));
  };
}
const local = env => ({ ...env, AI: { gateway: () => gateway(env.GATEWAY_UPSTREAM), run: unified(env.GATEWAY_UPSTREAM) } });
const timers = new Map();
const schedule = globalThis.setTimeout;
const unschedule = globalThis.clearTimeout;
globalThis.setTimeout = (callback, delay = 0, ...args) => {
  const handle = schedule((...values) => { timers.delete(handle); callback(...values); }, delay, ...args);
  timers.set(handle, Date.now() + Number(delay || 0));
  return handle;
};
globalThis.clearTimeout = handle => { timers.delete(handle); unschedule(handle); };
const pendingTimers = () => [...timers.values()].map(due => Math.max(0, due - Date.now())).sort((left, right) => right - left);
function counted(storage) {
  const sql = storage.sql;
  const exec = sql.exec.bind(sql);
  const totals = { statements: 0, rows_read: 0, rows_written: 0, alarm_writes: 0 };
  for (const name of ["setAlarm", "deleteAlarm"]) {
    const operation = storage[name].bind(storage);
    storage[name] = (...args) => { totals.alarm_writes++; return operation(...args); };
  }
  const statements = {};
  let cursors = [];
  let paused = false;
  sql.exec = (...args) => { const cursor = exec(...args); if (paused) return cursor; totals.statements++; cursors.push([String(args[0]).replace(/\s+/g, " ").slice(0, 96), cursor, args.slice(1).reduce((total, value) => total + String(value ?? "").length, 0)]); return cursor; };
  const read = () => {
    for (const [query, cursor, bytes] of cursors) {
      totals.rows_read += cursor.rowsRead;
      totals.rows_written += cursor.rowsWritten;
      const entry = statements[query] ??= { statements: 0, rows_read: 0, rows_written: 0, param_bytes: 0 };
      entry.statements++;
      entry.rows_read += cursor.rowsRead;
      entry.rows_written += cursor.rowsWritten;
      entry.param_bytes += bytes;
    }
    cursors = [];
    return { ...totals, by_statement: structuredClone(statements) };
  };
  read.uncounted = operation => { paused = true; try { return operation(); } finally { paused = false; } };
  return read;
}
export class AgentSession extends Session {
  constructor(ctx, env) {
    const sql = counted(ctx.storage);
    const waitUntil = ctx.waitUntil.bind(ctx);
    const waiting = { count: 0 };
    ctx.waitUntil = promise => { waiting.count++; waitUntil(Promise.resolve(promise).finally(() => { waiting.count--; })); };
    super(ctx, local(env));
    this.waiting = waiting;
    this.sqlTotals = sql;
    const persisted = this.store?.persisted?.bind(this.store);
    if (persisted) this.store.persisted = scope => sql.uncounted(() => persisted(scope));
  }
  async fetch(request) {
    const response = await super.fetch(request);
    if (new URL(request.url).pathname !== "/internal/metrics" || response.status !== 200) return response;
    return Response.json({ ...await response.json(), sql: this.sqlTotals(), idle: { timers_ms: pendingTimers(), wait_until: this.waiting.count } });
  }
}
export default { fetch(request, env, ctx) { return worker.fetch(request, local(env), ctx); } };
`;
}

const wallets = contents => /export\s*\{[^}]*\bWallet\b[^}]*\}/.test(contents);

const entryOf = directory => `${directory}/${readdirSync(directory).find(file => file.endsWith(".js"))}`;

function modules(directory) {
  const files = readdirSync(directory);
  const scripts = files.filter(file => file.endsWith(".js"));
  if (scripts.length !== 1) throw new Error(`Expected one compiled Worker in ${directory}; run npm run dry-run`);
  const path = `${directory}/${scripts[0]}`;
  const contents = readFileSync(path, "utf8");
  const imports = [...contents.matchAll(/from\s+["']\.\/([^"']+\.wasm)["']/g)].map(match => match[1]);
  return [{ type: "ESModule", path: `${directory}/local-shim.mjs`, contents: shim(scripts[0], wallets(contents)) }, { type: "ESModule", path, contents }, ...[...new Set(imports)].map(file => ({ type: "CompiledWasm", path: `${directory}/${file}`, contents: readFileSync(process.env.NANOCODE_WASM_PATH ?? `${directory}/${file}`) }))];
}

const MCP_HOSTS = new Set(["gateway.tempo-dev.workers.dev", "mercator.sh"]);

async function offline(request) {
  const url = new URL(request.url);
  if (url.hostname === "cloudflare-dns.com" && MCP_HOSTS.has(url.searchParams.get("name"))) {
    const type = url.searchParams.get("type") === "AAAA" ? 28 : 1;
    return Response.json({ Status: 0, Answer: [{ name: url.searchParams.get("name"), type, TTL: 300, data: type === 1 ? "104.16.0.1" : "2606:4700::1" }] });
  }
  if (MCP_HOSTS.has(url.hostname) && url.pathname === "/mcp") {
    if (request.method !== "POST") return new Response(null, { status: 405 });
    const message = await request.json();
    if (message.id === undefined) return new Response(null, { status: 202 });
    const result = message.method === "initialize" ? { protocolVersion: message.params?.protocolVersion ?? "2025-06-18", capabilities: { tools: {} }, serverInfo: { name: "local", version: "0" } }
      : message.method === "tools/list" ? { tools: [] }
      : undefined;
    return Response.json(result ? { jsonrpc: "2.0", id: message.id, result } : { jsonrpc: "2.0", id: message.id, error: { code: -32601, message: "method not found" } });
  }
  throw new Error("The Worker attempted direct outbound network access");
}

export function localOptions({ provider, agent = {}, workers = [], persistence, bundle = process.env.NANOCODE_BUNDLE_PATH ?? "output/deploy" } = {}) {
  mkdirSync("output", { recursive: true });
  return {
    unsafeInspectDurableObjects: true,
    resourcePersistencePath: persistence ?? resolve(mkdtempSync("output/local-state-")),
    workers: [
      {
        name: "nanocode",
        modules: modules(bundle),
        compatibilityDate: "2026-10-16",
        durableObjects: { SESSIONS: { className: "AgentSession", useSQLite: true }, ...(wallets(readFileSync(entryOf(bundle), "utf8")) ? { WALLETS: { className: "Wallet", useSQLite: true } } : {}) },
        serviceBindings: { GATEWAY_UPSTREAM: "gateway-upstream" },
        bindings: { AI_GATEWAY_ID: "local", ACCESS_TOKEN_SHA256: createHash("sha256").update(LOCAL_TOKEN).digest("hex"), MODEL_AUTH: "api_key", RECOVERY_DELAY_MS: "1000", EVENT_PAGE_SIZE: "128", EVENT_TAIL_BYTES: "4194304", EVENT_COALESCE_MS: "100", MODEL_SOCKET_LINGER_MS: "0", RUNTIME_MEMORY_BUDGET_BYTES: "67108864", EVENT_STREAM_KEEPALIVE_MS: "100", REMOTE_MAX_FRAME_BYTES: "1048576", REMOTE_TIMEOUT_MS: "30000", ...agent },
        outboundService: offline,
      },
      {
        name: "gateway-upstream",
        modules: [{ type: "ESModule", path: "gateway-upstream.mjs", contents: GATEWAY }],
        compatibilityDate: "2026-10-16",
        ...(provider ? { outboundService: provider } : {}),
      },
      ...workers,
    ],
  };
}

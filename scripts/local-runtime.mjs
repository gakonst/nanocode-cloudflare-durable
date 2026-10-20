import { createHash } from "node:crypto";
import { readFileSync, readdirSync, mkdirSync, mkdtempSync } from "node:fs";
import { resolve } from "node:path";

export const LOCAL_TOKEN = "local-measurement";
export const GATEWAY_KEY = "local-provider";

const GATEWAY = `export default { fetch(request) { return fetch(request); } };`;

function shim(entry) {
  return `import worker, { AgentSession as Session } from "./${entry}";
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
const local = env => ({ ...env, AI: { gateway: () => gateway(env.GATEWAY_UPSTREAM) } });
function counted(storage) {
  const sql = storage.sql;
  const exec = sql.exec.bind(sql);
  const totals = { statements: 0, rows_read: 0, rows_written: 0, alarm_writes: 0 };
  for (const name of ["setAlarm", "deleteAlarm"]) {
    const operation = storage[name].bind(storage);
    storage[name] = (...args) => { totals.alarm_writes++; return operation(...args); };
  }
  let cursors = [];
  let paused = false;
  sql.exec = (...args) => { const cursor = exec(...args); if (paused) return cursor; totals.statements++; cursors.push(cursor); return cursor; };
  const read = () => {
    for (const cursor of cursors) { totals.rows_read += cursor.rowsRead; totals.rows_written += cursor.rowsWritten; }
    cursors = [];
    return { ...totals };
  };
  read.uncounted = operation => { paused = true; try { return operation(); } finally { paused = false; } };
  return read;
}
export class AgentSession extends Session {
  constructor(ctx, env) {
    const sql = counted(ctx.storage);
    super(ctx, local(env));
    this.sqlTotals = sql;
    const persisted = this.store.persisted.bind(this.store);
    this.store.persisted = scope => sql.uncounted(() => persisted(scope));
  }
  async fetch(request) {
    const response = await super.fetch(request);
    if (new URL(request.url).pathname !== "/internal/metrics" || response.status !== 200) return response;
    return Response.json({ ...await response.json(), sql: this.sqlTotals() });
  }
}
export default { fetch(request, env, ctx) { return worker.fetch(request, local(env), ctx); } };
`;
}

function modules(directory) {
  const files = readdirSync(directory);
  const scripts = files.filter(file => file.endsWith(".js"));
  if (scripts.length !== 1) throw new Error(`Expected one compiled Worker in ${directory}; run npm run dry-run`);
  const path = `${directory}/${scripts[0]}`;
  const contents = readFileSync(path, "utf8");
  const imports = [...contents.matchAll(/from\s+["']\.\/([^"']+\.wasm)["']/g)].map(match => match[1]);
  return [{ type: "ESModule", path: `${directory}/local-shim.mjs`, contents: shim(scripts[0]) }, { type: "ESModule", path, contents }, ...[...new Set(imports)].map(file => ({ type: "CompiledWasm", path: `${directory}/${file}`, contents: readFileSync(process.env.NANOCODE_WASM_PATH ?? `${directory}/${file}`) }))];
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
        durableObjects: { SESSIONS: { className: "AgentSession", useSQLite: true } },
        serviceBindings: { GATEWAY_UPSTREAM: "gateway-upstream" },
        bindings: { AI_GATEWAY_ID: "local", ACCESS_TOKEN_SHA256: createHash("sha256").update(LOCAL_TOKEN).digest("hex"), MODEL_AUTH: "api_key", RECOVERY_DELAY_MS: "1000", EVENT_PAGE_SIZE: "128", EVENT_COALESCE_MS: "100", MODEL_SOCKET_LINGER_MS: "0", RUNTIME_MEMORY_BUDGET_BYTES: "67108864", EVENT_STREAM_KEEPALIVE_MS: "100", REMOTE_MAX_FRAME_BYTES: "1048576", REMOTE_TIMEOUT_MS: "30000", ...agent },
        outboundService: async () => { throw new Error("The Worker attempted direct outbound network access"); },
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

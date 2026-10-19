import { readFileSync, readdirSync, mkdirSync, mkdtempSync } from "node:fs";
import { resolve } from "node:path";
import { credentials } from "./credentials.mjs";

function modules(directory) {
  const files = readdirSync(directory);
  const scripts = files.filter(file => file.endsWith(".js"));
  if (scripts.length !== 1) throw new Error(`Expected one compiled Worker in ${directory}; run npm run dry-run`);
  const path = `${directory}/${scripts[0]}`;
  const contents = readFileSync(path, "utf8");
  const imports = [...contents.matchAll(/from\s+["']\.\/([^"']+\.wasm)["']/g)].map(match => match[1]);
  return [{ type: "ESModule", path, contents }, ...[...new Set(imports)].map(file => ({ type: "CompiledWasm", path: `${directory}/${file}`, contents: readFileSync(process.env.NANOCODE_WASM_PATH ?? `${directory}/${file}`) }))];
}

export function localOptions({ provider, live = false, agent = {}, egress = {}, persistence } = {}) {
  mkdirSync("output", { recursive: true });
  const secretBindings = { ANTHROPIC_API_KEY: "local-provider", ANTHROPIC_ENDPOINT: "https://provider.invalid/v1/messages" };
  if (live) {
    delete secretBindings.ANTHROPIC_API_KEY;
    delete secretBindings.ANTHROPIC_ENDPOINT;
    if (process.env.OAUTH_FILE) secretBindings.OAUTH_JSON = JSON.stringify(credentials(process.env.OAUTH_FILE));
    else if (process.env.ANTHROPIC_API_KEY) secretBindings.ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
    else throw new Error("Live measurements require OAUTH_FILE with a dedicated login or ANTHROPIC_API_KEY");
  }
  return {
    unsafeInspectDurableObjects: true,
    resourcePersistencePath: persistence ?? resolve(mkdtempSync("output/local-state-")),
    workers: [
      {
        name: "nanocode-worker",
        modules: modules("output/deploy"),
        compatibilityDate: "2026-10-16",
        durableObjects: { SESSIONS: { className: "AgentSession", useSQLite: true } },
        serviceBindings: {
          EGRESS: { name: "nanocode-egress", entrypoint: "SessionEgress" },
          AUTHORITY: { name: "nanocode-egress", entrypoint: "ConnectAuthority" },
          SERVICE_EGRESS: { name: "nanocode-egress", entrypoint: "ConnectServiceEgress" },
          REMOTE: { name: "nanocode-egress", entrypoint: "RemoteExecutorEgress" },
        },
        bindings: { WORKER_TOKEN: "local-measurement", ACCOUNT_ID: "default", MODEL_AUTH: live && secretBindings.OAUTH_JSON ? "subscription" : "api_key", RECOVERY_DELAY_MS: "1000", EVENT_PAGE_SIZE: "128", CONNECT_STREAM_REVALIDATE_MS: "100", RUNNER_ENABLED: "false",  REMOTE_MAX_FRAME_BYTES: "1048576", REMOTE_TIMEOUT_MS: "30000", ...agent },
        outboundService: async () => { throw new Error("Public Worker attempted direct outbound network access"); },
      },
      {
        name: "nanocode-egress",
        modules: modules("output/egress"),
        compatibilityDate: "2026-10-16",
        durableObjects: { ACCOUNTS: { className: "AccountAuthority", useSQLite: true } },
        serviceBindings: { SESSION_FENCES: { name: "nanocode-worker", entrypoint: "SessionControl" } },
        bindings: { ACCOUNT_ID: "default", CREDENTIAL_CACHE_TTL_MS: "600000", REFRESH_MARGIN_MS: "60000", CONNECT_TOKEN_TTL_MS: "3600000", CONNECT_REFRESH_TTL_MS: "2592000000", CONNECT_CODE_TTL_MS: "300000", RECOVERY_DELAY_MS: "1000", MODEL_RESPONSE_HEADERS_TIMEOUT_MS: "120000", MODEL_STREAM_IDLE_TIMEOUT_MS: "120000", ...secretBindings, ...egress },
        ...(provider ? { outboundService: provider } : {}),
      },
    ],
  };
}

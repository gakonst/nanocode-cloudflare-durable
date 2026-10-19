import { createHash, randomBytes } from "node:crypto";
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

export const LOCAL_TOKEN = "local-measurement";

export function localOptions({ provider, live = false, agent = {}, egress = {}, egressServices = {}, vault = {}, workers = [], persistence } = {}) {
  mkdirSync("output", { recursive: true });
  const egressBindings = { ANTHROPIC_ENDPOINT: "https://provider.invalid/v1/messages" };
  const vaultSecrets = { ANTHROPIC_API_KEY: "local-provider" };
  if (live) {
    delete egressBindings.ANTHROPIC_ENDPOINT;
    delete vaultSecrets.ANTHROPIC_API_KEY;
    if (process.env.OAUTH_FILE) vaultSecrets.OAUTH_JSON = JSON.stringify(credentials(process.env.OAUTH_FILE));
    else if (process.env.ANTHROPIC_API_KEY) vaultSecrets.ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
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
        },
        bindings: { WORKER_TOKEN_SHA256: createHash("sha256").update(LOCAL_TOKEN).digest("hex"), ACCOUNT_ID: "default", MODEL_AUTH: live && vaultSecrets.OAUTH_JSON ? "subscription" : "api_key", RECOVERY_DELAY_MS: "1000", EVENT_PAGE_SIZE: "128", EVENT_COALESCE_MS: "100", MODEL_SOCKET_LINGER_MS: "0", RUNTIME_MEMORY_BUDGET_BYTES: "67108864", EVENT_STREAM_KEEPALIVE_MS: "100", RUNNER_ENABLED: "false", REMOTE_MAX_FRAME_BYTES: "1048576", REMOTE_TIMEOUT_MS: "30000", ...agent },
        outboundService: async () => { throw new Error("Public Worker attempted direct outbound network access"); },
      },
      {
        name: "nanocode-egress",
        modules: modules("output/egress"),
        compatibilityDate: "2026-10-16",
        serviceBindings: { VAULT: { name: "nanocode-vault", entrypoint: "CredentialBroker" }, ...egressServices },
        bindings: { ACCOUNT_ID: "default", CREDENTIAL_CACHE_TTL_MS: "600000", REFRESH_MARGIN_MS: "60000", MODEL_RESPONSE_HEADERS_TIMEOUT_MS: "120000", MODEL_STREAM_IDLE_TIMEOUT_MS: "120000", ...egressBindings, ...egress },
        ...(provider ? { outboundService: provider } : {}),
      },
      {
        name: "nanocode-vault",
        modules: modules("output/vault"),
        compatibilityDate: "2026-10-16",
        durableObjects: { CREDENTIALS: { className: "CredentialVault", useSQLite: true } },
        bindings: { ACCOUNT_ID: "default", REFRESH_MARGIN_MS: "60000", VAULT_KEY: randomBytes(32).toString("base64url"), ...vaultSecrets, ...vault },
        ...(provider ? { outboundService: provider } : {}),
      },
      ...workers,
    ],
  };
}

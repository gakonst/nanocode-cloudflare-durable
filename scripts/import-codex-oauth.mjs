import { readFileSync } from "node:fs";
import { spawnSync } from "node:child_process";

const path = process.argv[2];
if (!path) throw new Error("Pass the auth.json path from a dedicated Codex login; this deployment will own credential refresh");
const document = JSON.parse(readFileSync(path, "utf8"));
const entry = document.tokens ?? document;
const tokens = { access_token: entry.access_token ?? entry.access, refresh_token: entry.refresh_token ?? entry.refresh, id_token: entry.id_token, account_id: entry.account_id, expires: entry.expires };
if (document.auth_mode && document.auth_mode !== "chatgpt") throw new Error("A ChatGPT subscription login is required");
if (typeof tokens.access_token !== "string" || typeof tokens.refresh_token !== "string") throw new Error("No Codex subscription credentials found");
const result = spawnSync("npx", ["wrangler", "secret", "put", "CODEX_OAUTH_JSON", "--config", "wrangler.egress.jsonc"], { input: JSON.stringify({ auth_mode: "chatgpt", tokens }), stdio: ["pipe", "inherit", "inherit"] });
if (result.error) throw result.error;
process.exit(result.status ?? 1);

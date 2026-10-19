import { spawnSync } from "node:child_process";
import { credentials } from "./credentials.mjs";

const result = spawnSync("npx", ["wrangler", "secret", "put", "OAUTH_JSON", "--config", "wrangler.vault.jsonc"], { input: JSON.stringify(credentials(process.argv[2])), stdio: ["pipe", "inherit", "inherit"] });
if (result.error) throw result.error;
process.exit(result.status ?? 1);

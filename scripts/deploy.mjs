import { spawnSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync, unlinkSync } from "node:fs";
import { resolve } from "node:path";

function deploy(config) {
  const result = spawnSync(process.execPath, ["node_modules/wrangler/bin/wrangler.js", "deploy", "--config", config], { stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`Deployment failed for ${config}`);
}

mkdirSync("output", { recursive: true });
const bootstrap = resolve("output/egress-bootstrap.json");
const config = JSON.parse(readFileSync("wrangler.egress.jsonc", "utf8"));
config.main = resolve(config.main);
config.services = config.services.filter(service => service.service !== "nanocode-worker");
writeFileSync(bootstrap, JSON.stringify(config, null, 2));
try {
  deploy("wrangler.vault.jsonc");
  deploy(bootstrap);
  deploy("wrangler.jsonc");
  deploy("wrangler.egress.jsonc");
} finally { unlinkSync(bootstrap); }

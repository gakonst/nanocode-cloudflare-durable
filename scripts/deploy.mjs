import { spawnSync } from "node:child_process";

function deploy(config) {
  const result = spawnSync(process.execPath, ["node_modules/wrangler/bin/wrangler.js", "deploy", "--config", config], { stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`Deployment failed for ${config}`);
}

deploy("wrangler.vault.jsonc");
deploy("wrangler.egress.jsonc");
deploy("wrangler.jsonc");

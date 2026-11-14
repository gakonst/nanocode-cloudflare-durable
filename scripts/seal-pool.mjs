import { execFileSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const name = process.argv[2];
const account = process.env.CLOUDFLARE_ACCOUNT_ID;
if (!name || !account) throw new Error("usage: CLOUDFLARE_ACCOUNT_ID=<account> node scripts/seal-pool.mjs <worker name>");

const wrangler = (args, options = {}) => execFileSync("npx", ["wrangler", ...args], { encoding: "utf8", ...options });
const token = JSON.parse(wrangler(["auth", "token", "--json"])).token;
const api = async (route) => {
  const response = await fetch(`https://api.cloudflare.com/client/v4/accounts/${account}${route}`, { headers: { authorization: `Bearer ${token}` } });
  const body = await response.json();
  if (!body.success) throw new Error(`${route}: ${JSON.stringify(body.errors)}`);
  return body;
};

const namespaces = (await api("/workers/durable_objects/namespaces?per_page=1000")).result;
const namespace = namespaces.find((entry) => entry.script === name && entry.class === "AgentSession");
if (!namespace) throw new Error(`${name} has no AgentSession namespace`);

const ids = [];
for (let cursor = ""; ; ) {
  const page = await api(`/workers/durable_objects/namespaces/${namespace.id}/objects?limit=10000${cursor ? `&cursor=${cursor}` : ""}`);
  ids.push(...page.result.map((entry) => entry.id));
  cursor = page.result_info?.cursor ?? "";
  if (!cursor || !page.result.length) break;
}

const dir = fs.mkdtempSync(path.join(os.tmpdir(), "seal-pool-"));
const secret = randomBytes(16).toString("hex");
const sealer = `${name}-seal`;
fs.writeFileSync(path.join(dir, "index.js"), `export default { async fetch(request, env) { if (new URL(request.url).pathname !== "/" + env.SECRET) return new Response(null, { status: 404 }); return Response.json({ ready: await env.POOL.get(env.POOL.idFromName("pool")).seal(await request.json()) }); } };`);
fs.writeFileSync(path.join(dir, "wrangler.json"), JSON.stringify({ name: sealer, main: "index.js", compatibility_date: "2026-10-16", workers_dev: true, vars: { SECRET: secret }, durable_objects: { bindings: [{ name: "POOL", class_name: "SessionPool", script_name: name }] } }));
try {
  const deployed = wrangler(["deploy", "-c", path.join(dir, "wrangler.json")]);
  const url = /https:\/\/\S+\.workers\.dev/.exec(deployed)?.[0];
  if (!url) throw new Error(`no workers.dev URL in: ${deployed}`);
  for (let attempt = 0; ; attempt++) {
    const response = await fetch(`${url}/${secret}`, { method: "POST", body: JSON.stringify(ids) });
    if (response.ok) {
      console.log(JSON.stringify({ worker: name, legacy: ids.length, ...(await response.json()) }));
      break;
    }
    if (response.status !== 404 || attempt > 30) throw new Error(`seal answered ${response.status}: ${await response.text()}`);
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }
} finally {
  wrangler(["delete", "--name", sealer, "--force"], { stdio: "ignore" });
  fs.rmSync(dir, { recursive: true });
}

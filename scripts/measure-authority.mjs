import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { randomBytes } from "node:crypto";
import { writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { localOptions } from "./local-runtime.mjs";

const secret = () => randomBytes(32).toString("base64url");
const seed = { access: secret(), refresh: secret(), expires: Date.now() - 1000, scopes: ["user:profile", "user:inference"] };
let currentAccess;
let currentRefresh = seed.refresh;
let refreshes = 0;
let providerCalls = 0;
let rejectAccess;
let hangRefresh = false;
let observedMarker = false;
let delayValid = false;
const transport = process.argv.includes("--transport");
let mf;
const assertions = [];
const check = (value, message) => { if (!value) throw new Error(message); assertions.push(message); };
const storage = () => mf.unsafeGetDurableObjectStorage("nanocode-egress", "AccountAuthority", { name: "default" });
const provider = async request => {
  if (new URL(request.url).hostname === "platform.claude.com") {
    refreshes++;
    const marker = await (await storage()).exec("SELECT state FROM credentials WHERE name = 'model'");
    observedMarker ||= marker[0]?.state === "exchanging";
    const input = await request.json();
    if (input.refresh_token !== currentRefresh) throw new Error("Refresh ownership repeated a consumed credential");
    if (hangRefresh) return new Promise(() => {});
    currentAccess = secret();
    currentRefresh = secret();
    return Response.json({ access_token: currentAccess, refresh_token: currentRefresh, expires_in: 3600, token_type: "Bearer", scope: "user:profile user:inference" });
  }
  providerCalls++;
  const auth = request.headers.get("authorization");
  if (auth === `Bearer ${rejectAccess}`) return Response.json({ error: { type: "authentication_error", message: "mock failure" } }, { status: 401 });
  if (auth !== `Bearer ${currentAccess}`) throw new Error("Unexpected access generation");
  if (delayValid) { delayValid = false; await new Promise(resolve => setTimeout(resolve, 800)); }
  return new Response("ok", { headers: { "content-type": "text/plain" } });
};
const options = localOptions({ provider, agent: { MODEL_AUTH: "subscription" }, egress: { OAUTH_JSON: JSON.stringify(seed), VAULT_KEY: secret(), ...(transport ? { MODEL_RESPONSE_HEADERS_TIMEOUT_MS: "200" } : {}) } });
const dispatch = async () => {
  const bindings = await mf.getBindings("nanocode-worker");
  return bindings.EGRESS.fetch(new Request("https://egress.internal/model", { method: "POST", headers: { "content-type": "application/octet-stream", "x-nanocode-model": JSON.stringify({ authority: { account_id: "default", session_id: "authority-measurement" }, subscription: true, headers: [] }) }, body: "{}" }));
};
const launch = async () => { mf = new Miniflare(convertV4MiniflareOptions(options)); await mf.ready; };
const until = async condition => {
  const started = performance.now();
  while (!await condition()) {
    if (performance.now() - started > 10000) throw new Error("Authority measurement timed out");
    await new Promise(resolve => setTimeout(resolve, 5));
  }
};
try {
  await launch();
  const batch = await Promise.all(Array.from({ length: 20 }, dispatch));
  check(batch.every(response => response.status === 200) && refreshes === 1, "Concurrent cold requests coalesce into one rotating refresh");
  check(observedMarker, "Uncertain-exchange marker is committed before contacting token endpoint");
  for (const response of batch) await response.text();
  let durable = await storage();
  const encrypted = await durable.exec("SELECT state, generation, substr(envelope, 1, 3) AS format FROM credentials WHERE name = 'model'");
  check(encrypted[0].state === "ready" && encrypted[0].generation === 2 && encrypted[0].format === "v1.", "Rotated credentials are encrypted and committed before release");
  const bindings = await mf.getBindings("nanocode-worker");
  const metrics = await (await bindings.EGRESS.fetch(new Request("https://egress.internal/metrics"))).json();
  const before = metrics.metrics.model_authority_rpcs;
  await (await dispatch()).text();
  const warm = await (await bindings.EGRESS.fetch(new Request("https://egress.internal/metrics"))).json();
  check(warm.metrics.model_authority_rpcs === before, "Warm model dispatch makes zero account authority RPCs");
  await mf.dispose();
  await launch();
  check((await dispatch()).status === 200 && refreshes === 1, "Cold egress restoration reuses the committed rotated credential");
  rejectAccess = currentAccess;
  delayValid = transport;
  const forcedStarted = performance.now();
  const forced = await Promise.all(Array.from({ length: 20 }, dispatch));
  check(forced.filter(response => response.status === 200).length === (transport ? 19 : 20) && refreshes === 2, "Concurrent 401 responses force one generation-fenced refresh");
  if (transport) check(forced.filter(response => response.status === 504).length === 1 && performance.now() - forcedStarted < 3000, "The second model dispatch after a401 refresh has a bounded header deadline");
  for (const response of forced) await response.text();
  const refreshesBeforeInterruption = refreshes;
  await mf.dispose();
  options.workers[1].bindings.OAUTH_JSON = JSON.stringify({ ...seed, access: secret(), refresh: currentRefresh, expires: Date.now() - 1 });
  hangRefresh = true;
  await launch();
  const uncertain = dispatch().catch(() => undefined);
  await until(() => refreshes === refreshesBeforeInterruption + 1);
  await mf.dispose();
  await uncertain;
  hangRefresh = false;
  await launch();
  durable = await storage();
  const retained = await durable.exec("SELECT state FROM credentials WHERE name = 'model'");
  check(retained[0].state === "exchanging", "Runtime replacement retains the uncertain refresh marker");
  const blocked = await dispatch();
  check(blocked.status === 401 && refreshes === refreshesBeforeInterruption + 1, "Uncertain refresh is not repeated after runtime replacement");
  await blocked.text();
  await mf.dispose();
  options.workers[1].bindings.OAUTH_JSON = JSON.stringify({ ...seed, access: secret(), refresh: currentRefresh, expires: Date.now() - 1 });
  await launch();
  check((await dispatch()).status === 200 && refreshes === refreshesBeforeInterruption + 2, "Fresh credential import resets the blocked refresh state");
  const report = { runtime: "local workerd", assertions, refreshes, providerCalls, credential_values_reported: false };
  writeFileSync("output/authority-measurement.json", JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
} finally { await mf?.dispose(); }

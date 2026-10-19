import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";
import { EXECUTOR_ID, json, type ModelProvider } from "../contracts";
import { ACCOUNT_PATTERN, Failure, modelAccount, modelProvider, readLimited, sha256hex } from "../egress/common";
import { CODEX, CODEX_ACCOUNT, codexPolicy, codexSeed, oauthError, open, seal, SECRET_PATTERN, SUBSCRIPTION, subscriptionSeed, type StoredToken, type TokenPolicy } from "./credentials";

export interface VaultEnv {
  CREDENTIALS: DurableObjectNamespace<CredentialVault>;
  ACCOUNT_ID?: string;
  REFRESH_MARGIN_MS: string;
  VAULT_KEY?: string;
  OAUTH_JSON?: string;
  CODEX_OAUTH_JSON?: string;
  ANTHROPIC_API_KEY?: string;
  RUNNER_TOKENS_JSON?: string;
}

export type Denied = { ok: false; status: number; error: string };
export type ModelAccess = { ok: true; generation: number; expires_at: number | null; headers: [string, string][]; subscription: boolean } | Denied;
export type RunnerAccess = { ok: true; token: string } | Denied;
export type CredentialState = { state: string; generation: number; expires_at: number | null };
export type VaultStatus = { ok: true; encryption: boolean; claude: "subscription" | "api_key" | "unconfigured"; model: CredentialState | null; codex: CredentialState | null; runners: string[]; counters: Record<string, number> } | Denied;

type CredentialRow = { name: string; seed: string; envelope: string; generation: number; state: string; forced: number; expires_at: number | null; updated_at: number };
type Granted = Extract<ModelAccess, { ok: true }>;

const SCHEMA = "CREATE TABLE IF NOT EXISTS credentials (name TEXT PRIMARY KEY, seed TEXT NOT NULL, envelope TEXT NOT NULL, generation INTEGER NOT NULL, state TEXT NOT NULL, forced INTEGER NOT NULL, expires_at INTEGER, updated_at INTEGER NOT NULL)";
const CODEX_CREDENTIAL = "model:codex";
const RUNNER_TOKEN = /^[\x21-\x7e]{1,4096}$/;

function denied(error: unknown): Denied {
  if (error instanceof Failure) return { ok: false, status: error.status, error: error.code };
  return { ok: false, status: 503, error: "vault_unavailable" };
}

function margin(env: VaultEnv): number {
  const value = Number(env.REFRESH_MARGIN_MS);
  if (!Number.isSafeInteger(value) || value <= 0) throw new Failure(500, "configuration_error");
  return value;
}

let runnerSource: string | undefined;
let parsedRunners = new Map<string, string>();

function runnerTokens(env: VaultEnv): Map<string, string> {
  const source = env.RUNNER_TOKENS_JSON ?? "";
  if (source === runnerSource) return parsedRunners;
  const result = new Map<string, string>();
  if (source) {
    let document: unknown;
    try { document = JSON.parse(source); } catch { throw new Failure(500, "configuration_error"); }
    if (!document || typeof document !== "object" || Array.isArray(document)) throw new Failure(500, "configuration_error");
    for (const [id, token] of Object.entries(document)) {
      if (!EXECUTOR_ID.test(id) || typeof token !== "string" || !RUNNER_TOKEN.test(token)) throw new Failure(500, "configuration_error");
      result.set(id, token);
    }
  }
  runnerSource = source;
  parsedRunners = result;
  return result;
}

export class CredentialVault extends DurableObject<VaultEnv> {
  private gates = new Map<string, Promise<unknown>>();
  private counters = { refreshes: 0, refresh_failures: 0 };

  constructor(ctx: DurableObjectState, env: VaultEnv) {
    super(ctx, env);
    ctx.storage.sql.exec(SCHEMA);
  }

  async model(provider: ModelProvider, rejected?: number): Promise<ModelAccess> {
    try {
      if (rejected !== undefined && !Number.isSafeInteger(rejected)) throw new Failure(400, "invalid_request");
      if (provider === "codex") return await this.gate(CODEX_CREDENTIAL, () => this.codex(rejected));
      const mode = this.claude();
      if (mode === "unconfigured") throw new Failure(503, "credential_missing");
      if (mode === "api_key") {
        if (rejected !== undefined) throw new Failure(401, "credential_rejected");
        return { ok: true, generation: 0, expires_at: null, headers: [["x-api-key", this.env.ANTHROPIC_API_KEY!]], subscription: false };
      }
      return await this.gate("model", () => this.subscription(rejected));
    } catch (error) {
      return denied(error);
    }
  }

  async status(): Promise<VaultStatus> {
    try {
      return {
        ok: true,
        encryption: !!this.env.VAULT_KEY,
        claude: this.claude(),
        model: await this.state("model", this.env.OAUTH_JSON),
        codex: await this.state(CODEX_CREDENTIAL, this.env.CODEX_OAUTH_JSON),
        runners: [...runnerTokens(this.env).keys()],
        counters: { ...this.counters },
      };
    } catch (error) {
      return denied(error);
    }
  }

  private claude(): "subscription" | "api_key" | "unconfigured" {
    if (this.env.OAUTH_JSON || this.row("model")) return "subscription";
    return this.env.ANTHROPIC_API_KEY ? "api_key" : "unconfigured";
  }

  private async state(name: string, source: string | undefined): Promise<CredentialState | null> {
    const row = this.row(name);
    if (source !== undefined && (!row || row.seed !== await sha256hex(`${name}:${source}`))) return { state: "pending_seed", generation: row?.generation ?? 0, expires_at: null };
    return row ? { state: row.state, generation: row.generation, expires_at: row.expires_at } : null;
  }

  private row(name: string): CredentialRow | undefined {
    return this.ctx.storage.sql.exec<CredentialRow>("SELECT * FROM credentials WHERE name = ?", name).toArray()[0];
  }

  private run(query: string, ...bindings: unknown[]): void {
    this.ctx.storage.sql.exec(query, ...bindings.map(value => value === undefined ? null : value));
  }

  private gate<T>(name: string, run: () => Promise<T>): Promise<T> {
    const next = (this.gates.get(name) ?? Promise.resolve()).then(run);
    const settled = next.then(() => {}, () => {});
    this.gates.set(name, settled);
    void settled.then(() => { if (this.gates.get(name) === settled) this.gates.delete(name); });
    return next;
  }

  private aad(name: string): string {
    return `${modelAccount(this.env)}|${name}`;
  }

  private async load(name: string, source: string | undefined, parse: () => StoredToken): Promise<{ row: CredentialRow; token: StoredToken }> {
    const existing = this.row(name);
    if (source !== undefined) {
      const seed = await sha256hex(`${name}:${source}`);
      if (!existing || existing.seed !== seed) {
        const token = parse();
        const envelope = await seal(this.env.VAULT_KEY, this.aad(name), token);
        const row: CredentialRow = { name, seed, envelope, generation: (existing?.generation ?? 0) + 1, state: "ready", forced: 0, expires_at: token.expires_at, updated_at: Date.now() };
        this.run("INSERT OR REPLACE INTO credentials VALUES (?, ?, ?, ?, ?, ?, ?, ?)", row.name, row.seed, row.envelope, row.generation, row.state, row.forced, row.expires_at, row.updated_at);
        await this.ctx.storage.sync();
        return { row, token };
      }
    }
    if (!existing) throw new Failure(503, "credential_missing");
    return { row: existing, token: await open<StoredToken>(this.env.VAULT_KEY, this.aad(name), existing.envelope) };
  }

  private async subscription(rejected?: number): Promise<Granted> {
    const lifetime = margin(this.env);
    const source = this.env.OAUTH_JSON;
    let { row, token } = await this.load("model", source, () => subscriptionSeed(source!));
    if (row.state !== "ready") throw new Failure(401, row.state === "exchanging" ? "refresh_uncertain" : row.state);
    const force = rejected !== undefined && rejected === row.generation;
    if (force && row.forced) throw new Failure(401, "credential_rejected");
    if (force || token.expires_at === null || token.expires_at - Date.now() <= lifetime) {
      const scopes = SUBSCRIPTION.scopes.filter(scope => scope !== "org:create_api_key");
      for (const scope of token.scopes) if ((scope === "user:projects:read" || scope === "user:projects:write") && !scopes.includes(scope)) scopes.push(scope);
      const current = token;
      ({ row, token } = await this.exchange("model", row, token, force, lifetime, scope => fetch(SUBSCRIPTION.token_url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ grant_type: "refresh_token", refresh_token: current.refresh, client_id: SUBSCRIPTION.client_id, scope }),
        redirect: "manual",
        signal: AbortSignal.timeout(30_000),
      }), scopes.join(" "), current.scopes.length ? current.scopes.join(" ") : undefined));
    }
    return { ok: true, generation: row.generation, expires_at: token.expires_at, headers: [["authorization", `Bearer ${token.access}`]], subscription: true };
  }

  private async codex(rejected?: number): Promise<Granted> {
    const lifetime = margin(this.env);
    const source = this.env.CODEX_OAUTH_JSON;
    let { row, token } = await this.load(CODEX_CREDENTIAL, source, () => codexSeed(source!));
    if (row.state !== "ready") throw new Failure(401, row.state === "exchanging" ? "refresh_uncertain" : row.state);
    if (typeof token.account_id !== "string" || !CODEX_ACCOUNT.test(token.account_id)) throw new Failure(500, "configuration_error");
    const force = rejected !== undefined && rejected === row.generation;
    if (force && row.forced) throw new Failure(401, "credential_rejected");
    if (force || token.expires_at === null || token.expires_at - Date.now() <= lifetime) {
      const current = token;
      ({ row, token } = await this.exchange(CODEX_CREDENTIAL, row, token, force, lifetime, () => fetch(CODEX.token_url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ client_id: CODEX.client_id, grant_type: "refresh_token", refresh_token: current.refresh }),
        redirect: "manual",
        signal: AbortSignal.timeout(30_000),
      }), undefined, undefined, codexPolicy(current, lifetime)));
    }
    const headers: [string, string][] = [["authorization", `Bearer ${token.access}`], ["chatgpt-account-id", token.account_id!]];
    if (token.fedramp) headers.push(["x-openai-fedramp", "true"]);
    return { ok: true, generation: row.generation, expires_at: token.expires_at, headers, subscription: true };
  }

  private async exchange(name: string, row: CredentialRow, token: StoredToken, forced: boolean, minimumLifetime: number, send: (scope?: string) => Promise<Response>, scope: string | undefined, fallback: string | undefined, policy?: TokenPolicy): Promise<{ row: CredentialRow; token: StoredToken }> {
    const subscription = name === "model";
    if (token.refresh_expires_at !== null && Date.now() >= token.refresh_expires_at) {
      this.run("UPDATE credentials SET state = 'login_required', updated_at = ? WHERE name = ?", Date.now(), name);
      await this.ctx.storage.sync();
      throw new Failure(401, "login_required");
    }
    this.run("UPDATE credentials SET state = 'exchanging', updated_at = ? WHERE name = ?", Date.now(), name);
    await this.ctx.storage.sync();
    this.counters.refreshes++;
    const attempt = async (value?: string) => {
      const response = await send(value);
      return { status: response.status, body: (await readLimited(response, 65_536)) ?? "" };
    };
    let response: { status: number; body: string };
    try {
      response = await attempt(scope);
      if (response.status === 400 && fallback !== undefined && oauthError(response.body).error === "invalid_scope") response = await attempt(fallback);
    } catch {
      this.counters.refresh_failures++;
      throw new Failure(401, "refresh_uncertain");
    }
    if (response.status !== 200) {
      this.counters.refresh_failures++;
      const error = oauthError(response.body);
      let state: string | undefined;
      if (policy) state = policy.rejected(response.status, response.body);
      else if ([400, 401, 403].includes(response.status) && (error.error === "invalid_grant" || error.error === "access_denied") && error.error_description === "account_on_hold") state = "on_hold";
      else if ([400, 401].includes(response.status) && error.error === "invalid_grant") state = "login_required";
      if (state) {
        this.run("UPDATE credentials SET state = ?, updated_at = ? WHERE name = ?", state, Date.now(), name);
        await this.ctx.storage.sync();
        throw new Failure(401, state);
      }
      throw new Failure(401, "refresh_uncertain");
    }
    const body = oauthError(response.body) as Record<string, unknown>;
    const now = Date.now();
    if (policy) {
      const accepted = policy.accept(body, now);
      if (typeof accepted === "string") {
        this.counters.refresh_failures++;
        this.run("UPDATE credentials SET state = ?, updated_at = ? WHERE name = ?", accepted, Date.now(), name);
        await this.ctx.storage.sync();
        throw new Failure(401, accepted);
      }
      if (!accepted) {
        this.counters.refresh_failures++;
        throw new Failure(401, "refresh_uncertain");
      }
      return await this.commit(name, row, accepted, forced);
    }
    const scopes = typeof body.scope === "string" ? body.scope.split(/\s+/).filter(Boolean) : token.scopes;
    const expiresIn = body.expires_in;
    const validExpiry = expiresIn === undefined ? !subscription : Number.isSafeInteger(expiresIn) && (expiresIn as number) * 1000 > minimumLifetime && (expiresIn as number) > 0;
    const valid = typeof body.access_token === "string" && SECRET_PATTERN.test(body.access_token)
      && (body.refresh_token === undefined || (typeof body.refresh_token === "string" && SECRET_PATTERN.test(body.refresh_token)))
      && (body.token_type === undefined || (typeof body.token_type === "string" && body.token_type.toLowerCase() === "bearer"))
      && (body.refresh_token_expires_in === undefined || (Number.isSafeInteger(body.refresh_token_expires_in) && (body.refresh_token_expires_in as number) > 0))
      && validExpiry
      && (!subscription || (typeof body.scope === "string" && scopes.includes("user:profile") && scopes.includes("user:inference")));
    if (!valid) {
      this.counters.refresh_failures++;
      throw new Failure(401, "refresh_uncertain");
    }
    const next: StoredToken = {
      access: body.access_token as string,
      refresh: (body.refresh_token as string | undefined) ?? token.refresh,
      expires_at: expiresIn === undefined ? null : now + (expiresIn as number) * 1000,
      scopes,
      refresh_expires_at: body.refresh_token_expires_in !== undefined ? now + (body.refresh_token_expires_in as number) * 1000 : token.refresh_expires_at,
    };
    return await this.commit(name, row, next, forced);
  }

  private async commit(name: string, row: CredentialRow, next: StoredToken, forced: boolean): Promise<{ row: CredentialRow; token: StoredToken }> {
    const envelope = await seal(this.env.VAULT_KEY, this.aad(name), next);
    const updated: CredentialRow = { ...row, envelope, generation: row.generation + 1, state: "ready", forced: forced ? 1 : 0, expires_at: next.expires_at, updated_at: Date.now() };
    this.run("UPDATE credentials SET envelope = ?, generation = ?, state = 'ready', forced = ?, expires_at = ?, updated_at = ? WHERE name = ?", updated.envelope, updated.generation, updated.forced, updated.expires_at, updated.updated_at, name);
    await this.ctx.storage.sync();
    return { row: updated, token: next };
  }
}

export class CredentialBroker extends WorkerEntrypoint<VaultEnv> {
  async model(input: { account_id: string; provider?: ModelProvider; rejected_generation?: number }): Promise<ModelAccess> {
    try {
      this.owner(input?.account_id);
      return await this.vault().model(modelProvider(input.provider), input.rejected_generation) as ModelAccess;
    } catch (error) {
      return denied(error);
    }
  }

  async runner(input: { account_id: string; executor_id: string }): Promise<RunnerAccess> {
    try {
      this.owner(input?.account_id);
      if (typeof input.executor_id !== "string" || !EXECUTOR_ID.test(input.executor_id)) throw new Failure(400, "invalid_request");
      const token = runnerTokens(this.env).get(input.executor_id);
      if (!token) throw new Failure(503, "executor_unavailable");
      return { ok: true, token };
    } catch (error) {
      return denied(error);
    }
  }

  async status(input: { account_id: string }): Promise<VaultStatus> {
    try {
      this.owner(input?.account_id);
      return await this.vault().status() as VaultStatus;
    } catch (error) {
      return denied(error);
    }
  }

  private owner(account: unknown): void {
    if (typeof account !== "string" || !ACCOUNT_PATTERN.test(account)) throw new Failure(400, "invalid_request");
    if (account !== modelAccount(this.env)) throw new Failure(403, "forbidden");
  }

  private vault() {
    return this.env.CREDENTIALS.get(this.env.CREDENTIALS.idFromName(modelAccount(this.env)));
  }
}

export default {
  fetch(): Response {
    return json({ error: "not_found" }, 404);
  },
} satisfies ExportedHandler<VaultEnv>;

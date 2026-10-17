# Development and deployment

This is a separate Cloudflare app. Its sibling `../nanocodex` supplies the Rust SDK. Keep SDK changes in that workspace and Worker-specific code here. Do not add code comments, README files, Cargo features, wasm-bindgen glue, tests, or test-only dependencies.

The WASM agent and SQLite checkpoints run inside the same session Durable Object. A stable session UUID is persisted before SDK initialization. Requests are queued durably and deduplicated by `request_id`; reusing an ID with different input returns 409. SDK ownership fences, revision checks, immutable records, model receipts, tool receipts, subagent journals, and checkpoints use the persistent store. No memory store is used for agent state.

Alarms drive work independently of client connections. A watchdog alarm is persisted before execution, using `RECOVERY_DELAY_MS` as an operator recovery policy. Completed requests return saved results. Interrupted model calls may repeat; dispatched tools retain the SDK's uncertain-outcome handling. The Worker does not promise exactly-once model billing or external side effects. Concurrent sessions use independent Durable Objects. Each session executes queued root turns serially, while SDK subagents can run concurrently.

`SubscriptionAuth` owns the shared rotating subscription credential. It serializes refreshes, commits an uncertain-exchange marker before contacting the token endpoint, and persists rotated credentials before releasing them. An uncertain refresh requires importing a fresh credential document. Credentials stay in Cloudflare secrets and encrypted Durable Object storage. A Container is not required for authentication or inference. Worker secrets and access tokens must never appear in source, command arguments, logs, or benchmark reports.

Cloudflare chooses physical Durable Object placement. Agent execution and checkpoint storage are co-located by construction; this does not pin all objects or the OAuth broker to an exact Cloudflare colo. SQLite access stays inside each session, while OAuth refresh is coordinated by the shared broker. The default tool collection omits local machine tools and enables SDK delegation. Shell, filesystem, desktop, and remote Code Mode execution require a separately configured execution host; this app does not create a machine environment.

Install Rust 1.97 or newer, the `wasm32-unknown-unknown` target, and Node.js. Run `npm ci`, `cargo check --locked --lib --target wasm32-unknown-unknown`, `npm run check`, and `npm run dry-run`. Run SDK checks from `../nanocodex` with `cargo check --locked --lib` and `cargo check --locked --lib --target wasm32-unknown-unknown`. Local compilation keeps incremental artifacts without debug information. The optimized WASM build uses the `wasm` profile. Dependency resolution is pinned by both lockfiles.

Deploy from this directory:

For interactive login, use the existing Brave Browser profile through the installed CUA provider recorded in `~/.nanocodex/runtimes/openai-cua/provider.json`. That provider owns the working native application connection. A fresh Chrome profile does not share the user's saved login or browser session. Unlock 1Password through its normal UI when the saved credential requires it.

1. Run `npx wrangler login`, or provide `CLOUDFLARE_API_TOKEN` through your secret manager.
2. Run `npm run deploy` to create the Worker and both SQLite Durable Object namespaces.
3. Set an API access token with `npx wrangler secret put WORKER_TOKEN`.
4. Run `node scripts/import-oauth.mjs /absolute/path/to/oauth.json`. Omitting the path reads the default Claude Code credential from the macOS keychain. The importer accepts Anthropic entries from Pi/OMP-style documents and Claude Code's `claudeAiOauth` document, and pipes only the normalized Claude credential into `wrangler secret put OAUTH_JSON`. It does not write a credential file.

For API-key authentication instead, set `ANTHROPIC_API_KEY` with Wrangler secrets and omit `OAUTH_JSON`. Subscription authentication takes precedence when both are configured. Do not run the same rotating refresh credential independently in multiple deployments or local clients; use a dedicated login when independent refresh ownership is needed. Uploading a fresh credential document changes the seed digest and resets the broker's retained credential state.

Every request requires `Authorization: Bearer $WORKER_TOKEN`. Submit `POST /sessions/<name>/turns` with JSON `{"request_id":"job-1","prompt":"Your instruction"}`; the response is 202 while queued or running. Poll `GET /sessions/<name>/turns/job-1` for the final result. Repeating the POST with the same input returns the saved result after completion. `GET /health` reports the configured model and authentication mode. `ANTHROPIC_ENDPOINT` is an operator transport override for a compatible endpoint; subscription requests retain the SDK's pinned wire compatibility.

Runtime measurements:

- `npm run dry-run && node scripts/measure-local.mjs` runs the compiled WASM in local workerd against a deterministic mock streaming provider, measures initial and restored turns and receipt replay, then restarts workerd during inference and verifies recovery. The idle-session measurement uses Durable Object eviction; the in-flight measurement forces runtime replacement because ordinary eviction waits for active references.
- `node scripts/measure-local.mjs --live` uses the Mac keychain subscription for two short Opus 5.5 requests, including session restoration after eviction. Set `OAUTH_FILE` to use a credential file instead. Reports contain only the fixed measurement prompt's response and store counts.
- `node scripts/benchmark-transfer.mjs 65536 256` compares JSON byte-array round trips with the raw memory copy used by the WASM HTTP body bridge for 16 MiB of data. This isolates serialization cost, not inference throughput or full application speed. Chunk size and count are benchmark inputs.

Keep all reports and generated bundles in ignored `output/`, `dist/`, and `target/`. Local workerd latency does not establish production colo latency. First-time optimized builds, cached builds, mock-provider overhead, and live model latency are separate measurements.

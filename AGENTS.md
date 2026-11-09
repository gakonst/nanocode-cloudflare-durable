# Development and deployment

This repository is durable-cloudflare (nanocodexyz/durable-cloudflare, formerly nanocode-cloudflare-durable and nanocode-worker): nanocode agents running in Durable Objects: a default deployment and demo of nanocode-ts's `nanocode-ts/cloudflare`, which holds the runtime this repository used to (`src/` moved there), the Cloudflare counterpart of nanocodexyz/durable-vercel. nanocodexyz/nanocode-ts supplies the agent runtime, the Rust SDK compiled into one WebAssembly module with its host-agnostic TypeScript host and its Cloudflare adapter, as the release tarball `https://github.com/nanocodexyz/nanocode-ts/releases/download/build-<short commit>/nanocode-ts-<short commit>.tgz`, which nanocode-ts's CI builds from each commit on its main and `package-lock.json` pins with its integrity. The tarball carries the compiled JavaScript and module, so this repository builds no Rust: after merging a change there, wait for its `build-<short commit>` release and move the URL with `npm install <tarball URL>`, and read the SDK revision it builds against in its `sdk-revision.json`. Keep SDK changes in the SDK, host-agnostic bridge changes and generic Durable Object plumbing in nanocode-ts and Cloudflare business logic here. The `../nanocode-worker` path is a symlink to this checkout for consumers that still name it. Hosted platform pieces (private egress with credential storage, the vault, execution-host fleets, identity and connectors) live in gakonst/nanocode-platform, which consumes this library.

## Rules

These principles come from nanocodexyz/nanocode's `AGENTS.md` and apply to every change here.

- Keep code simple and changes small. Match the surrounding code and preserve unrelated work.
- No code comments, doc comments, README edits, tests or test-only dependencies. Only comments that a tool reads stay.
- This repository holds no Rust: do not add Cargo features or wasm-bindgen glue. SDK changes belong in nanocodexyz/nanocode, and host-agnostic bridge and Durable Object changes in nanocodexyz/nanocode-ts.
- No magic constants or arbitrary caps: a limit comes from a platform or protocol, a measurement or operator policy. Surface invalid states; never hide them with silent truncation, clamping or fallbacks.
- Make invalid states unrepresentable: model a lifecycle as one discriminated union whose variants carry only what that phase has.
- Preserve public APIs.
- Inference, subagents and durability live in the brain; Code Mode runs on the user's machine. Never silently replay dispatched work: report uncertain outcomes.
- Validate with `npm run build` (which type-checks), `npm run dry-run`, the local workerd measurements (`node scripts/measure-*.mjs`, `npm run drill`) and turns against the deployed demo, and back performance changes with reproducible before/after measurements.
- Check behavior with harnesses in ignored `output/`. Keep every artifact there; never commit them, credentials or personal data.
- Work on master in the main checkout, commit only your own changes and push each one; no pull requests. There is no CI: build, release and deploy by hand. Use subagents only when asked.
- This file holds principles only and rarely changes. DECISIONS.md is the growing log: add what each change establishes and why, with the commands, dates and measurements behind it.

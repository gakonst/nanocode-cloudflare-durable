<h1 align="center">durable-cloudflare</h1>

<p align="center">
  <a href="https://nanocode.xyz/cloudflare">Live Demo</a> · <a href="https://nanocode.xyz/experiments/durable-nanocode-on-cloudflare">How It Works</a> · <a href="https://nanocode.xyz/docs/guides/workers">Guide</a>
</p>

[nanocode](https://nanocode.xyz) agents in Cloudflare Durable Objects: one object per session, with the agent's journal in the object's own SQLite. A template and the source of [nanocode.xyz/cloudflare](https://nanocode.xyz/cloudflare).

<p align="center">
  <img alt="A turn goes from the client through the Worker to the session object, which journals in its SQLite and is restarted by an alarm after a reset" src="assets/session.svg" />
</p>

- **Durable.** Every model call and tool call is journaled. An object that resets mid-turn is started again by its alarm and continues from the last checkpoint.
- **No sandbox.** Code Mode cells run beside the agent in the object: bash on a workspace kept in the same SQLite, git, Python on Pyodide and WebAssembly.
- **One module.** The runtime is [nanocode-ts](https://github.com/nanocodexyz/nanocode-ts), the Rust SDK compiled to WebAssembly. This repository holds the Worker, its tools and the page.

## Run It

```sh
npm install
npm run dev       # wrangler dev, at http://localhost:8787
npm run drill     # Crash a turn midway and check that it resumes
npm run deploy    # Deploy the Worker and its Python Worker
```

Model requests go through the account's AI Gateway (`AI_GATEWAY_ID` in `wrangler.jsonc`). `DECISIONS.md` records what each change established, with its measurements.

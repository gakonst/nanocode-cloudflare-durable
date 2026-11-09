import { Plans } from "nanocode-ts/client";
import * as Transport from "nanocode-ts/cloudflare/transport";
import { Keyring } from "nanocode-ts/machines";
import { Agent, type Theme } from "nanocode-ts/react/agent";
import { PLAN_HARNESSES, PUBLIC_HARNESSES } from "../worker/models.ts";

const PROMPTS = [
  "Clone wevm/viem and list its five largest source files",
  "Plot a sine wave with numpy and matplotlib",
  "Run a WebAssembly add function",
  "Search the web with Mercator for today's Tempo news, spending at most $0.05",
];

export function Demo(props: { theme: Theme; title?: string }) {
  return (
    <Agent
      connect={({ headers, selection, session }) => ({
        keyring: Keyring.create({ headers, url: "v1/keys" }),
        plans: Plans.create({ headers, url: "plans" }),
        transport: Transport.create({ headers, model: () => ({ model: selection().model, provider: selection().harness, thinking: selection().thinking }), session }),
      })}
      crash="Armed: the next tool call resets its Durable Object 1.5 s in."
      footer={props.title && <Footer title={props.title} />}
      harnesses={PUBLIC_HARNESSES}
      planHarnesses={PLAN_HARNESSES}
      prompts={PROMPTS}
      scripts
      storage="nanocode-cloudflare-durable"
      theme={props.theme}
      {...(props.title ? { title: props.title } : {})}
      token={async () => !(await fetch("health").then(response => response.ok, () => false))}
      wallet={{ name: "nanocode on Cloudflare", relay: "api/device/", websiteUrl: "https://nanocode.xyz/cloudflare/" }}
    />
  );
}

function Footer(props: { title: string }) {
  return (
    <>
      <a className="nc-brand" href="https://nanocode.xyz/cloudflare/" rel="noreferrer" target="_blank">
        <svg aria-hidden="true" height="14" viewBox="0 0 24 24" width="14"><path d="M19.5 20H6a5 5 0 0 1-.9-9.92A7 7 0 0 1 18.4 9.1 5.5 5.5 0 0 1 19.5 20z" fill="#F38020" /></svg>
        on Cloudflare
      </a>
      <span className="nc-about"> · {props.title} · durable agent in a Durable Object</span>
      <span className="nc-about"> · built with <a href="https://github.com/nanocodexyz/nanocode-ts" rel="noreferrer" target="_blank">nanocode-ts</a></span>
      <span className="nc-site"> · <a href="https://nanocode.xyz/" rel="noreferrer" target="_blank">nanocode.xyz →</a></span>
    </>
  );
}

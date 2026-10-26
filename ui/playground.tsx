import "nanocode-ts/react/zyzz.css";
import { createConfig } from "nanocode-ts/client";
import { cloudflare } from "nanocode-ts/cloudflare/transport";
import { Keyring } from "nanocode-ts/machines";
import { Playground } from "nanocode-ts/react/playground";
import { Demos, Header, Lead, Main, origin, Page, styles as site, Title } from "nanocode-ts/react/site";
import { createRoot } from "react-dom/client";
import { PUBLIC_HARNESSES as HARNESSES } from "../worker/models.ts";
import "./fonts.ts";

const KEY = "nanocode-cloudflare-durable:playground";
const HAIKU = HARNESSES[0]!.models[0]!;

const highlight = () => import("nanocode-ts/highlight").then(module => module.highlight);

function config(name: string, history: boolean) {
  const transport = cloudflare({
    headers: () => ({ ...Keyring.visitorHeaders() }),
    model: () => ({ model: HAIKU.id, provider: "claude", thinking: HAIKU.efforts.includes("low" as never) ? "low" : HAIKU.effort }),
    session: { load: () => ({}), save: () => {} },
  });
  return createConfig({ ...(history ? { history: { key: KEY + ":" + name } } : {}), revealMs: 0, transport });
}

const configs = { button: config("button", false), chat: config("chat", true), rewrite: config("rewrite", false) };

document.body.className = site.body().className;

createRoot(document.getElementById("root")!).render(
  <>
    <Header experiment={origin + "/cloudflare/"} />
    <Main>
      <Page>
        <Title subtitle="One agent, any look, any form factor.">Playground</Title>
        <Demos current="cloudflare" />
        <Lead>Every view below is the same durable nanocode agent in a Cloudflare Durable Object, rendered with nanocode-ts's headless React components. A theme is a skin (class names for each part) plus a few CSS variables; switch it, or pick your own tokens, and the chat, an AI button and an inline rewrite all follow. The code under the preview is all it takes.</Lead>
        <Playground configs={configs} highlight={highlight} status={"Claude · " + HAIKU.label + " · Durable Object"} />
        <p><a href="./">← Back to the terminal chat</a></p>
      </Page>
    </Main>
  </>,
);

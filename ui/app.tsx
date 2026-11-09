import "nds/zyzz.css";
import { Demos, Header, Lead, Main, origin, Page, styles as site, Title } from "nds/site";
import { createRoot } from "react-dom/client";
import { Demo } from "./agent.tsx";
import "./fonts.ts";

document.body.className = site.body().className;

createRoot(document.getElementById("root")!).render(
  <>
    <Header experiment={`${origin}/cloudflare/`} />
    <Main>
      <Page>
        <Title subtitle="Durable agents in Durable Objects.">nanocode on Cloudflare</Title>
        <Demos current="cloudflare" />
        <Lead>This agent runs in a Cloudflare Durable Object: the nanocode SDK compiled to WebAssembly, with its journal in the object's own SQLite. Its Code Mode cells run beside it in the object: bash on a workspace kept in the same SQLite, git clone, WebAssembly, Python with numpy, pandas and matplotlib on Pyodide, and a fetch, Mercator and the Tempo gateway that pay MPP services from your Tempo Wallet, whose access key the Worker keeps encrypted so turns go on paying after you close the tab. When the object resets mid-turn, its alarm starts it again, and the turn continues from the last checkpoint. Cloudflare AI Gateway serves its model requests, or your own ChatGPT or Claude plan once you connect it: its credential stays sealed in a Durable Object of this deployment, and ChatGPT's requests leave through a Cloudflare Container, because chatgpt.com refuses requests from Workers. Its source is <a href="https://github.com/nanocodexyz/durable-cloudflare">nanocodexyz/durable-cloudflare</a>.</Lead>
        <Demo theme="nanocode" />
        <p>Also as a <a href="chatgpt">ChatGPT-style</a> and a <a href="claude">Claude-style</a> app.</p>
        <p><a href={`${origin}/experiments/durable-nanocode-on-cloudflare`}>Read how it works →</a></p>
      </Page>
    </Main>
  </>,
);

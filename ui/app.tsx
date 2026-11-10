import "nds/zyzz.css";
import { Demos, Header, Links, Main, origin, Page, Points, styles as site, Terminal, Title } from "nds/site";
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
        <Points>
          <li><strong>One Durable Object per session.</strong> The nanocode SDK as WebAssembly, with its journal in the object's SQLite.</li>
          <li><strong>Survives a reset.</strong> Pick "Kill the next tool call" and the turn resumes from its last checkpoint.</li>
          <li><strong>A shell without a machine.</strong> Cells run bash, git, Python and WebAssembly beside the agent.</li>
          <li><strong>Yours to connect.</strong> Connect adds a Tempo Wallet, your ChatGPT or Claude plan, or your own machine.</li>
        </Points>
        <Terminal><Demo theme="nanocode" /></Terminal>
        <Links>
          <a href={`${origin}/experiments/durable-nanocode-on-cloudflare`}>How it works</a>
          <a href={`${origin}/docs/guides/workers`}>Build your own</a>
          <a href={`${origin}/#prompts`}>Prompts</a>
          <a href="https://github.com/nanocodexyz/durable-cloudflare">Source</a>
          <a href="chatgpt">ChatGPT-style</a>
          <a href="claude">Claude-style</a>
        </Links>
      </Page>
    </Main>
  </>,
);

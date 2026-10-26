import { ChatGpt } from "nanocode-ts/react/clones";
import { createRoot } from "react-dom/client";
import "./clones/page.css";
import { createClone, highlight } from "./clones/chat.tsx";
import { prompts } from "./clones/prompts.ts";
import { Cloudflare, WalletButton } from "./clones/wallet.tsx";

const clone = createClone("nanocode-cloudflare-durable:chatgpt", "codex");

const ABOUT: Record<string, string> = {
  "glm-5.3": "Open-weights model on Workers AI",
  "gpt-6-luna": "Fast for everyday questions",
  "gpt-6.1-sol": "Thinks longer for harder problems",
};

function App() {
  const { bound, choose, model, models } = clone.useModel();
  return (
    <ChatGpt
      actions={<WalletButton className="nc-wallet-button" />}
      config={clone.config}
      footer={<><Cloudflare className="nc-brand" /><span className="nc-about"> · ChatGPT-style demo · durable agent in a Durable Object</span><span className="nc-about"> · built with <a href="https://github.com/gakonst/nanocode-ts" rel="noreferrer" target="_blank">nanocode-ts</a></span><span className="nc-site"> · <a href="https://nanocode.xyz/" rel="noreferrer" target="_blank">nanocode.xyz →</a></span></>}
      highlight={highlight}
      picker={{ bound, choose, current: model.id, models: models.map(item => ({ detail: ABOUT[item.id] ?? item.name, id: item.id, label: item.label })) }}
      prompts={prompts}
      title="ChatGPT-style demo"
      warm={() => clone.transport.warm()}
    />
  );
}

createRoot(document.getElementById("root")!).render(<App />);

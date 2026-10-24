import type { TurnBlock } from "nanocode-ts/client";
import { AttachmentList, Markdown, NanocodeProvider, useSend, type Skin, ToolCell, useActivity, useConversations, useTurns } from "nanocode-ts/react";
import { useState } from "react";
import { createRoot } from "react-dom/client";
import "./clones/chatgpt.css";
import { type Block, createClone, CopyCode, grouped, highlight, seconds, useComposer, useDismiss, useResume, useSidebar, useStick } from "./clones/chat.tsx";
import { prompts } from "./clones/hand.ts";
import { Cloudflare, HandBadge, WalletButton } from "./clones/wallet.tsx";
import { ArrowUp, Check, Chevron, Compose, Plus, Sidebar, Stop, Trash } from "./clones/icons.tsx";

const clone = createClone("nanocode-cloudflare-durable:chatgpt", "codex");

const ABOUT: Record<string, string> = {
  "glm-5.3": "Open-weights model on Workers AI",
  "gpt-6-luna": "Fast for everyday questions",
  "gpt-6.1-sol": "Thinks longer for harder problems",
};

const skin: Skin = {
  icons: { cancelled: "–", completed: "✓", failed: "✕", running: "", yielded: "↻" },
  parts: {
    body: () => ({ className: "gpt-cell-body" }),
    children: () => ({ className: "gpt-cell-children" }),
    code: () => ({ className: "gpt-pre" }),
    header: () => ({ className: "gpt-cell-head" }),
    heading: () => ({ className: "md-heading" }),
    icon: () => ({ className: "gpt-cell-icon" }),
    inline: () => ({ className: "gpt-inline" }),
    meta: () => ({ className: "gpt-cell-meta" }),
    more: () => ({ className: "gpt-cell-more" }),
    name: () => ({ className: "gpt-cell-name" }),
    title: () => ({ className: "gpt-cell-title" }),
    table: () => ({ className: "md-table" }),
    tool: () => ({ className: "gpt-cell" }),
  },
};

function ModelMenu() {
  const { bound, choose, model, models } = clone.useModel();
  const [open, setOpen] = useState(false);
  const ref = useDismiss(open, () => setOpen(false));

  return (
    <div className="gpt-model" ref={ref}>
      <button aria-expanded={open} aria-haspopup="menu" className="gpt-model-button" onClick={() => setOpen(value => !value)} type="button">
        <span>{model.label}</span><Chevron />
      </button>
      {open && (
        <div className="gpt-menu" role="menu">
          <p className="gpt-menu-title">Model</p>
          {models.map(item => (
            <button aria-checked={item.id === model.id} className="gpt-menu-item" key={item.id} onClick={() => { choose(item.id); setOpen(false); }} role="menuitemradio" type="button">
              <span><strong>{item.label}</strong><small>{ABOUT[item.id] ?? item.name}</small></span>
              {item.id === model.id && <Check />}
            </button>
          ))}
          {bound && <p className="gpt-menu-note">This chat keeps its model. Another model starts a new chat.</p>}
        </div>
      )}
    </div>
  );
}

function Thought(props: { block: TurnBlock }) {
  const [open, setOpen] = useState<boolean>();
  const done = !props.block.live;
  const took = seconds(props.block);
  const shown = open ?? !done;
  const label = !done ? "Thinking" : took ? "Thought for " + took + "s" : "Thought";

  return (
    <div className="gpt-thought" data-done={done} data-open={shown}>
      <button aria-expanded={shown} onClick={() => setOpen(!shown)} type="button">{label}<Chevron /></button>
      {shown && <div className="gpt-thought-text">{props.block.text.replace(/\*\*(.+?)\*\*/g, "$1")}</div>}
    </div>
  );
}

function Tools(props: { block: Extract<Block, { kind: "tools" }> }) {
  const [open, setOpen] = useState<boolean>();
  const running = props.block.cells.some(cell => cell.status === "running");
  const shown = open ?? running;
  const count = props.block.cells.length;
  const code = props.block.cells.every(cell => cell.name === "code" || cell.tool === "exec");
  const label = running ? (code ? "Running code" : "Working") : (code ? "Ran code" : "Used " + count + (count === 1 ? " tool" : " tools"));

  return (
    <div className="gpt-tools" data-open={shown} data-running={running}>
      <button aria-expanded={shown} className="gpt-tools-head" onClick={() => setOpen(!shown)} type="button">
        <span>{label}</span>
        <span className="gpt-tools-names">{props.block.cells.map(cell => cell.title || cell.name).filter(name => name !== "code").join(" · ")}</span>
        <Chevron />
      </button>
      {shown && <div className="gpt-tools-body">{props.block.cells.map(cell => <ToolCell cell={cell} key={cell.call_id} live={props.block.live} />)}</div>}
    </div>
  );
}

function BlockView(props: { block: Block }) {
  const block = props.block;
  if (block.kind === "tools") return <Tools block={block} />;
  if (block.kind === "assistant") return <div className="gpt-md" data-live={block.live}><Markdown code={(language, code) => <CopyCode code={code} language={language} />} text={block.text} /></div>;
  if (block.kind === "reasoning") return <Thought block={block} />;
  return <p className="gpt-note" data-error={block.kind === "error"}>{block.text}</p>;
}

function Composer(props: { onSend(): void }) {
  const composer = useComposer({ onSend: props.onSend, warm: () => clone.transport.warm() });

  return (
    <div className="gpt-composer" data-dragging={composer.attachments.dragging} onClick={composer.focus} {...composer.attachments.drop}>
      <AttachmentList items={composer.attachments.items} onRemove={composer.attachments.remove} />
      {composer.attachments.enabled && <>
        <input {...composer.attachments.input} />
        <button aria-label="Attach files" className="gpt-attach" onClick={event => { event.stopPropagation(); composer.attachments.open(); }} title="Attach files" type="button"><Plus /></button>
      </>}
      <textarea placeholder={composer.busy ? "Steer the response" : "Ask anything"} {...composer.textarea} />
      {composer.canCancel && composer.empty
        ? <button aria-label="Stop" className="gpt-send" onClick={event => { event.stopPropagation(); void composer.cancel(); }} type="button"><Stop /></button>
        : <button aria-label="Send" className="gpt-send" disabled={composer.empty} onClick={event => { event.stopPropagation(); composer.submit(); }} type="button"><ArrowUp /></button>}
    </div>
  );
}

function Footer() {
  return <p className="gpt-foot"><Cloudflare className="nc-brand" /> · ChatGPT-style demo · durable agent in a Durable Object · <HandBadge className="nc-hand" /> · built with <a href="https://github.com/gakonst/nanocode-ts" rel="noreferrer" target="_blank">nanocode-ts</a></p>;
}

function Prompts(props: { onSend(): void }) {
  const { send } = useSend();
  return <div className="nc-prompts">{prompts.map(prompt => <button className="nc-prompt" key={prompt} onClick={() => { props.onSend(); void send(prompt); }} type="button">{prompt}</button>)}</div>;
}

function App() {
  useResume();
  const thread = useTurns();
  const threads = useConversations();
  const sidebar = useSidebar();
  const log = useStick(thread);
  const activity = useActivity();

  const waiting = activity.phase === "waiting" || activity.phase === "running";
  const newChat = () => { threads.create(); sidebar.close(); };

  return (
    <div className="gpt" data-sidebar={sidebar.open}>
      <aside className="gpt-sidebar">
        <div className="gpt-sidebar-top">
          <button aria-label="Close sidebar" className="gpt-icon" onClick={sidebar.toggle} type="button"><Sidebar /></button>
          <button aria-label="New chat" className="gpt-icon" disabled={!threads.canCreate} onClick={newChat} type="button"><Compose /></button>
        </div>
        <button className="gpt-new" disabled={!threads.canCreate} onClick={newChat} type="button"><Compose />New chat</button>
        <p className="gpt-section">Chats</p>
        <nav className="gpt-chats">
          {threads.conversations.map(item => (
            <div className="gpt-chat" data-current={item.id === threads.current} key={item.id}>
              <button disabled={!threads.canSelect} onClick={() => { threads.select(item.id); sidebar.close(); }} type="button">{item.title || "New chat"}</button>
              <button aria-label="Delete chat" className="gpt-chat-remove" onClick={() => threads.remove(item.id)} type="button"><Trash /></button>
            </div>
          ))}
        </nav>
      </aside>
      <div className="gpt-scrim" onClick={sidebar.toggle} />
      <main className="gpt-main">
        <header className="gpt-top">
          <button aria-label="Open sidebar" className="gpt-icon gpt-open" onClick={sidebar.toggle} type="button"><Sidebar /></button>
          <ModelMenu />
          <WalletButton className="nc-wallet-button" />
          <button aria-label="New chat" className="gpt-icon gpt-top-new" disabled={!threads.canCreate} onClick={newChat} type="button"><Compose /></button>
        </header>
        {thread.length === 0 ? (
          <div className="gpt-hero">
            <h1>What can I help with?</h1>
            <Composer onSend={log.pin} />
            <Prompts onSend={log.pin} />
            <Footer />
          </div>
        ) : (
          <>
            <div aria-live="polite" className="gpt-log" onScroll={log.onScroll} ref={log.ref}>
              <div className="gpt-thread">
                {thread.map((turn, index) => (
                  <section className="gpt-turn" key={turn.id}>
                    {turn.prompt && <div className="gpt-user"><div className="gpt-bubble">{turn.prompt.text}{turn.prompt.attachments?.length ? <AttachmentList items={turn.prompt.attachments} /> : null}</div></div>}
                    <div className="gpt-assistant">
                      {grouped(turn.blocks).map((block, position) => <BlockView block={block} key={position} />)}
                      {waiting && index === thread.length - 1 && <span aria-label="Responding" className="gpt-dot" />}
                    </div>
                  </section>
                ))}
              </div>
            </div>
            <div className="gpt-bottom">
              <Composer onSend={log.pin} />
              <Footer />
            </div>
          </>
        )}
      </main>
    </div>
  );
}

createRoot(document.getElementById("root")!).render(
  <NanocodeProvider config={clone.config} highlight={highlight} skin={skin}>
    <App />
  </NanocodeProvider>,
);

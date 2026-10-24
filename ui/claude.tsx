import type { TurnBlock } from "nanocode-ts/client";
import { Markdown, NanocodeProvider, useSend, type Skin, ToolCell, useActivity, useConversations, useTurns } from "nanocode-ts/react";
import { useState } from "react";
import { createRoot } from "react-dom/client";
import "./clones/claude.css";
import { type Block, createClone, CopyCode, greeting, grouped, highlight, seconds, useComposer, useDismiss, useResume, useSidebar, useStick } from "./clones/chat.tsx";
import { prompts } from "./clones/hand.ts";
import { Cloudflare, HandBadge, WalletButton } from "./clones/wallet.tsx";
import { ArrowUp, Check, Chevron, Plus, Sidebar, Stop, Trash } from "./clones/icons.tsx";

const clone = createClone("nanocode-cloudflare-durable:claude", "claude");

const ABOUT: Record<string, string> = {
  "claude-haiku-5-5": "Fastest for quick answers",
  "claude-sonnet-5-5": "Smart, efficient model for everyday use",
};

const skin: Skin = {
  icons: { cancelled: "–", completed: "✓", failed: "!", running: "", yielded: "↻" },
  parts: {
    body: () => ({ className: "cl-cell-body" }),
    children: () => ({ className: "cl-cell-children" }),
    code: () => ({ className: "cl-pre" }),
    header: () => ({ className: "cl-cell-head" }),
    heading: () => ({ className: "md-heading" }),
    icon: () => ({ className: "cl-cell-icon" }),
    inline: () => ({ className: "cl-inline" }),
    meta: () => ({ className: "cl-cell-meta" }),
    more: () => ({ className: "cl-cell-more" }),
    name: () => ({ className: "cl-cell-name" }),
    section: () => ({ className: "cl-cell-section" }),
    title: () => ({ className: "cl-cell-title" }),
    table: () => ({ className: "md-table" }),
    tool: () => ({ className: "cl-cell" }),
  },
};

function ModelMenu() {
  const { bound, choose, model, models } = clone.useModel();
  const [open, setOpen] = useState(false);
  const ref = useDismiss(open, () => setOpen(false));

  return (
    <div className="cl-model" ref={ref}>
      <button aria-expanded={open} aria-haspopup="menu" className="cl-model-button" onClick={() => setOpen(value => !value)} type="button">
        {model.label}<Chevron />
      </button>
      {open && (
        <div className="cl-menu" role="menu">
          {models.map(item => (
            <button aria-checked={item.id === model.id} className="cl-menu-item" key={item.id} onClick={() => { choose(item.id); setOpen(false); }} role="menuitemradio" type="button">
              <span><strong>{item.label}</strong><small>{ABOUT[item.id] ?? item.name}</small></span>
              {item.id === model.id && <Check />}
            </button>
          ))}
          {bound && <p className="cl-menu-note">This chat keeps its model. Another model starts a new chat.</p>}
        </div>
      )}
    </div>
  );
}

function Thinking(props: { block: TurnBlock }) {
  const [open, setOpen] = useState(false);
  const done = !props.block.live;
  const took = seconds(props.block);
  const label = !done ? "Thinking…" : took ? "Thought for " + took + "s" : "Thought process";

  return (
    <div className="cl-thinking" data-done={done} data-open={open}>
      <button aria-expanded={open} onClick={() => setOpen(!open)} type="button"><span>{label}</span><Chevron /></button>
      {(open || !done) && <div className="cl-thinking-text">{props.block.text.replace(/\*\*(.+?)\*\*/g, "$1")}</div>}
    </div>
  );
}

function BlockView(props: { block: Block }) {
  const block = props.block;
  if (block.kind === "tools") return <div className="cl-tools">{block.cells.map(cell => <ToolCell cell={cell} collapsed key={cell.call_id} live={block.live} />)}</div>;
  if (block.kind === "assistant") return <div className="cl-md"><Markdown code={(language, code) => <CopyCode code={code} language={language} />} text={block.text} /></div>;
  if (block.kind === "reasoning") return <Thinking block={block} />;
  return <p className="cl-note" data-error={block.kind === "error"}>{block.text}</p>;
}

function Composer(props: { onSend(): void }) {
  const composer = useComposer({ onSend: props.onSend, warm: () => clone.transport.warm() });

  return (
    <div className="cl-composer">
      <textarea placeholder={composer.busy ? "Add to the response…" : "How can I help you today?"} {...composer.textarea} />
      <div className="cl-composer-row">
        <ModelMenu />
        {composer.canCancel && composer.empty
          ? <button aria-label="Stop" className="cl-send" onClick={() => void composer.cancel()} type="button"><Stop /></button>
          : <button aria-label="Send" className="cl-send" disabled={composer.empty} onClick={event => { event.stopPropagation(); composer.submit(); }} type="button"><ArrowUp /></button>}
      </div>
    </div>
  );
}

function Footer() {
  return <p className="cl-foot"><Cloudflare className="nc-brand" /> · Claude-style demo · durable agent in a Durable Object · <HandBadge className="nc-hand" /> · built with <a href="https://github.com/gakonst/nanocode-ts" rel="noreferrer" target="_blank">nanocode-ts</a></p>;
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
  const busy = activity.busy;
  const title = threads.conversations.find(item => item.id === threads.current)?.title;

  const streaming = activity.phase === "writing" || activity.phase === "thinking";
  const newChat = () => { threads.create(); sidebar.close(); };

  return (
    <div className="cl" data-sidebar={sidebar.open}>
      <aside className="cl-sidebar">
        <div className="cl-sidebar-top">
          <button aria-label="Close sidebar" className="cl-icon" onClick={sidebar.toggle} type="button"><Sidebar /></button>
          <span className="cl-brand">Claude-style demo</span>
        </div>
        <button className="cl-new" disabled={!threads.canCreate} onClick={newChat} type="button"><span className="cl-new-icon"><Plus /></span>New chat</button>
        <p className="cl-section">Recents</p>
        <nav className="cl-chats">
          {threads.conversations.map(item => (
            <div className="cl-chat" data-current={item.id === threads.current} key={item.id}>
              <button disabled={!threads.canSelect} onClick={() => { threads.select(item.id); sidebar.close(); }} type="button">{item.title || "New chat"}</button>
              <button aria-label="Delete chat" className="cl-chat-remove" onClick={() => threads.remove(item.id)} type="button"><Trash /></button>
            </div>
          ))}
        </nav>
      </aside>
      <div className="cl-scrim" onClick={sidebar.toggle} />
      <main className="cl-main">
        <header className="cl-top">
          <WalletButton className="nc-wallet-button" />
          <button aria-label="Open sidebar" className="cl-icon cl-open" onClick={sidebar.toggle} type="button"><Sidebar /></button>
          <span className="cl-title">{thread.length ? title || "New chat" : ""}</span>
          <button aria-label="New chat" className="cl-icon cl-top-new" disabled={!threads.canCreate} onClick={newChat} type="button"><Plus /></button>
        </header>
        {thread.length === 0 ? (
          <div className="cl-hero">
            <h1><span aria-hidden="true" className="cl-spark">✻</span>{greeting()}</h1>
            <Composer onSend={log.pin} />
            <Prompts onSend={log.pin} />
            <Footer />
          </div>
        ) : (
          <>
            <div aria-live="polite" className="cl-log" onScroll={log.onScroll} ref={log.ref}>
              <div className="cl-thread">
                {thread.map((turn, index) => (
                  <section className="cl-turn" key={turn.id}>
                    {turn.prompt && <div className="cl-user"><span aria-hidden="true" className="cl-avatar">Y</span><div>{turn.prompt.text}</div></div>}
                    <div className="cl-assistant">
                      {grouped(turn.blocks).map((block, position) => <BlockView block={block} key={position} />)}
                      {index === thread.length - 1 && <span aria-label={busy ? "Responding" : undefined} className="cl-spark cl-status" data-busy={busy} data-streaming={streaming}>✻</span>}
                    </div>
                  </section>
                ))}
              </div>
            </div>
            <div className="cl-bottom">
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

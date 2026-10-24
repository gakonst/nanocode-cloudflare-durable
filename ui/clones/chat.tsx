import { createConfig, type ToolCell as Cell, type TurnBlock } from "nanocode-ts/client";
import { cloudflare } from "nanocode-ts/cloudflare/transport";
import { Keyring } from "nanocode-ts/machines";
import { Code, useAttachments, useConfig, useConversations, useSend } from "nanocode-ts/react";
import { type KeyboardEvent, useEffect, useLayoutEffect, useRef, useState, useSyncExternalStore } from "react";
import { PUBLIC_HARNESSES } from "../../worker/models.ts";
import { attach, prewarm, setTab, tabFiles, useWallet } from "./hand.ts";

export type Model = (typeof PUBLIC_HARNESSES)[number]["models"][number];

export type Block = TurnBlock | { kind: "tools"; cells: Cell[]; live: boolean };

type Saved = { picked?: string; bound: Record<string, string> };

const TOKEN = "nanocode-cloudflare-durable:token";

export const highlight = () => import("nanocode-ts/highlight").then(module => module.highlight);

export function createClone(key: string, harness: "claude" | "codex") {
  const models: readonly Model[] = PUBLIC_HARNESSES.find(entry => entry.id === harness)?.models ?? [];
  const listeners = new Set<() => void>();
  const storageKey = key + ":models";
  let saved: Saved = { bound: {} };
  let version = 0;

  try {
    saved = { ...saved, ...JSON.parse(localStorage.getItem(storageKey) ?? "{}") };
  } catch {}

  const persist = () => {
    const ids = new Set(config.getState().conversations.map(item => item.id));
    saved.bound = Object.fromEntries(Object.entries(saved.bound).filter(([id]) => ids.has(id)));
    localStorage.setItem(storageKey, JSON.stringify(saved));
    version++;
    for (const listener of listeners) listener();
  };

  const headers = (): Record<string, string> => {
    const token = localStorage.getItem(TOKEN);
    return { ...Keyring.visitorHeaders(), ...(token ? { authorization: "Bearer " + token } : {}) };
  };

  const current = (): Model => {
    const conversation = config.getState().conversation;
    const id = (conversation && saved.bound[conversation]) || saved.picked;
    return models.find(model => model.id === id) ?? models[0]!;
  };

  const transport = tabFiles(cloudflare({
    hand: session => (localStorage.getItem(TOKEN) ? Promise.resolve(undefined) : attach(session)),
    headers,
    model: () => {
      const model = current();
      const conversation = config.getState().conversation;

      if (conversation && transport.session().id && !saved.bound[conversation]) {
        saved.bound[conversation] = model.id;
        persist();
      }

      return { model: model.id, provider: harness, thinking: (model.efforts as readonly string[]).includes("low") ? "low" : model.effort };
    },
  }));

  const config = createConfig({ history: { key }, transport });

  const choose = (id: string) => {
    const conversation = config.getState().conversation;
    saved.picked = id;
    if (conversation && saved.bound[conversation] && saved.bound[conversation] !== id) config.create();
    persist();
  };

  const subscribe = (listener: () => void) => {
    listeners.add(listener);
    const off = config.subscribe(listener);
    return () => {
      listeners.delete(listener);
      off();
    };
  };

  const useModel = () => {
    useSyncExternalStore(subscribe, () => version + ":" + config.getState().conversation);
    const conversation = config.getState().conversation;
    return { bound: Boolean(conversation && saved.bound[conversation]), choose, model: current(), models };
  };

  return { config, transport, useModel };
}

export function grouped(blocks: readonly TurnBlock[]): Block[] {
  const result: Block[] = [];

  for (const block of blocks) {
    const last = result[result.length - 1];
    if (block.tool && last?.kind === "tools") {
      last.cells.push(block.tool);
      last.live ||= block.live;
    } else if (block.tool) result.push({ cells: [block.tool], kind: "tools", live: block.live });
    else result.push(block);
  }

  return result;
}

export function seconds(block: TurnBlock): number | undefined {
  return block.live || block.started_at === undefined || block.ended_at === undefined ? undefined : Math.max(1, Math.round((block.ended_at - block.started_at) / 1000));
}

export function useStick(dependency: unknown) {
  const ref = useRef<HTMLDivElement>(null);
  const stick = useRef(true);

  useLayoutEffect(() => {
    if (stick.current && ref.current) ref.current.scrollTop = ref.current.scrollHeight;
  }, [dependency]);

  const onScroll = () => {
    const node = ref.current;
    if (node) stick.current = node.scrollHeight - node.scrollTop - node.clientHeight < 64;
  };

  return { onScroll, pin: () => { stick.current = true; }, ref };
}

export function useComposer(options: { onSend?(): void; warm(): void }) {
  const { cancel, canCancel, isPending: busy, send } = useSend();
  const attachments = useAttachments();
  const { tab } = useWallet();
  const [prompt, setPrompt] = useState("");
  const input = useRef<HTMLTextAreaElement>(null);

  useLayoutEffect(() => {
    const node = input.current;
    if (!node) return;
    node.style.height = "auto";
    node.style.height = Math.min(node.scrollHeight, 240) + "px";
  }, [prompt]);

  const submit = () => {
    const text = prompt.trim();
    const attached = attachments.ready;
    if ((!text && !attached.length) || attachments.uploading) return;
    const command = /^\/tab(?:\s+(on|off))?$/i.exec(text);
    if (command && !attached.length) {
      setPrompt("");
      setTab(command[1] ? command[1].toLowerCase() === "on" : !tab);
      return;
    }
    setPrompt("");
    attachments.clear();
    options.onSend?.();
    void send(text, attached);
  };

  const textarea = {
    "aria-label": "Message",
    onChange: (event: { target: { value: string } }) => {
      setPrompt(event.target.value);
      if (!busy && event.target.value) {
        options.warm();
        prewarm(event.target.value);
      }
    },
    onPaste: attachments.onPaste,
    onKeyDown: (event: KeyboardEvent<HTMLTextAreaElement>) => {
      if (event.nativeEvent.isComposing || event.key !== "Enter" || event.shiftKey) return;
      event.preventDefault();
      submit();
    },
    ref: input,
    rows: 1,
    value: prompt,
  };

  return { attachments, busy, cancel, canCancel, empty: !prompt.trim() && !attachments.ready.length, focus: () => input.current?.focus(), submit, textarea };
}

export function useResume() {
  const config = useConfig();
  const { current } = useConversations();

  useEffect(() => {
    void config.resume();
  }, [current]);
}

export function CopyCode(props: { code: string; language: string }) {
  const [copied, setCopied] = useState(false);
  const code = props.code.replace(/\n$/, "");

  const copy = () => {
    void navigator.clipboard?.writeText(code).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  };

  return (
    <div className="md-code">
      <div className="md-code-bar">
        <span>{props.language === "text" ? "" : props.language}</span>
        <button onClick={copy} type="button">{copied ? "Copied" : "Copy"}</button>
      </div>
      <Code code={code} framed language={props.language} />
    </div>
  );
}

export function greeting(): string {
  const hour = new Date().getHours();
  return hour < 5 ? "Good evening" : hour < 12 ? "Good morning" : hour < 18 ? "Good afternoon" : "Good evening";
}

export function useDismiss(open: boolean, close: () => void) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const pointer = (event: PointerEvent) => {
      if (!ref.current?.contains(event.target as Node)) close();
    };
    const key = (event: globalThis.KeyboardEvent) => {
      if (event.key === "Escape") close();
    };
    addEventListener("pointerdown", pointer);
    addEventListener("keydown", key);
    return () => {
      removeEventListener("pointerdown", pointer);
      removeEventListener("keydown", key);
    };
  }, [open]);

  return ref;
}

export function useSidebar() {
  const wide = () => matchMedia("(min-width: 768px)").matches;
  const [open, setOpen] = useState(wide);

  useEffect(() => {
    const media = matchMedia("(min-width: 768px)");
    const change = () => setOpen(media.matches);
    media.addEventListener("change", change);
    return () => media.removeEventListener("change", change);
  }, []);

  return { close: () => { if (!wide()) setOpen(false); }, open, toggle: () => setOpen(value => !value) };
}

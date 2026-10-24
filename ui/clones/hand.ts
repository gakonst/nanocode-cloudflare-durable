import type { HandStatus } from "nanocode-ts";
import type { Bash } from "nanocode-ts/bash";
import type { TransportFiles } from "nanocode-ts/client";
import { Keyring, Mcp, Payment, Payments, Wallet } from "nanocode-ts/machines";
import wasmUrl from "nanocode-ts/nanocode_wasm.wasm";
import { useSyncExternalStore } from "react";

Wallet.configure({ name: "nanocode on Cloudflare", relay: "api/device/", websiteUrl: "https://nanocode.xyz/cloudflare/" });

const TAB = "nanocode-cloudflare-durable:tab";
const TOKEN = "nanocode-cloudflare-durable:token";
const decoder = new TextDecoder();
const locked = Payment.locked(Mcp.servers);

type State = { connected?: Wallet.Wallet; busy: boolean; error?: string; device?: Wallet.Prompt; hand: HandStatus & { session?: string }; tab: boolean };

let state: State = { busy: false, hand: { state: "closed" }, tab: localStorage.getItem(TAB) === "1" };
let payments: Payments.Payments | undefined;
const listeners = new Set<() => void>();

function set(next: Partial<State>) {
  state = { ...state, ...next };
  for (const listener of listeners) listener();
}

const local = (path: string) => new URL(path, location.href).href;

const visited = (path: string) => local(path + "?" + new URLSearchParams({ visitor: Keyring.visitor() }));

export const pay: typeof fetch = (input, init) => {
  const url = String(input instanceof Request ? input.url : input);
  const body = init?.body instanceof Uint8Array && Mcp.server(url) ? decoder.decode(init.body) : init?.body;
  return (payments?.fetch ?? locked)(input, body === init?.body ? init : { ...init, body });
};

const shells = new Map<string, ReturnType<typeof Bash.create>>();

const TYPES: Record<string, string> = { avif: "image/avif", bmp: "image/bmp", gif: "image/gif", jpeg: "image/jpeg", jpg: "image/jpeg", png: "image/png", svg: "image/svg+xml", webp: "image/webp" };

export function tabFiles<T extends { files?: TransportFiles; session(): { id?: string } }>(transport: T): T {
  const files = transport.files;
  if (!files) return transport;
  const read = files.read.bind(files);
  files.read = async (path, signal) => {
    const shell = state.tab ? shells.get(transport.session().id ?? "") : undefined;
    const bytes = shell ? await (await shell).fs.read(path).catch(() => undefined) : undefined;
    if (!bytes) return read(path, signal);
    return new Blob([bytes.slice().buffer as ArrayBuffer], { type: TYPES[path.split(".").pop()?.toLowerCase() ?? ""] ?? "application/octet-stream" });
  };
  return transport;
}

let keeper: Promise<{ attach(session: string): Promise<string | undefined>; close(): void; prewarm(): Promise<unknown> }> | undefined;

function load() {
  return keeper ??= Promise.all([import("nanocode-ts"), import("nanocode-ts/bash")]).then(([{ compileModule, handKeeper }, { Bash }]) => {
    let module: Promise<WebAssembly.Module> | undefined;
    return handKeeper({
      fetch: pay,
      listed: async session => ((await (await fetch(local("sessions/" + encodeURIComponent(session) + "/hands"), { headers: Keyring.visitorHeaders() })).json()) as { hands?: string[] }).hands?.includes("browser") ?? false,
      mcpServers: Mcp.servers.map(server => ({ name: server.name, url: server.url })),
      mcp: { core: Mcp.core },
      module: () => module ??= compileModule(new URL(wasmUrl, import.meta.url).href),
      onStatus: hand => set({ hand }),
      url: session => visited("sessions/" + encodeURIComponent(session) + "/hands/browser").replace(/^http/, "ws"),
      workspace: async session => {
        let shell = shells.get(session);
        if (!shell) shells.set(session, shell = Bash.create({ cwd: "/workspace", fetch: pay, github: { archive: (owner, repo, ref) => local("api/github/" + owner + "/" + repo + "/" + ref) } }));
        const ready = await shell;
        return { cwd: ready.cwd, workspace: ready.workspace() };
      },
    });
  });
}

export async function attach(session: string): Promise<string | undefined> {
  if (!state.tab) return undefined;
  return (await load()).attach(session);
}

export function setTab(tab: boolean) {
  if (tab) localStorage.setItem(TAB, "1");
  else localStorage.removeItem(TAB);
  set({ tab });
  if (tab) warmTab();
  else {
    void keeper?.then(current => current.close());
    set({ hand: { state: "closed" } });
  }
}

function warmTab() {
  if (state.tab && !localStorage.getItem(TOKEN)) void load().then(current => current.prewarm());
}

export function prewarm(text: string) {
  if (!state.tab || localStorage.getItem(TOKEN)) return;
  void import("nanocode-ts/bash").then(bash => {
    const packages = bash.pythonHint(text);
    if (packages) void bash.preloadPython({}, packages);
  });
}

if (typeof requestIdleCallback === "function") requestIdleCallback(warmTab, { timeout: 1500 });
else setTimeout(warmTab, 500);

export function where(hand: State["hand"], tab: boolean) {
  if (!tab) return "Code runs on Cloudflare";
  return hand.state === "attached" ? "Code runs in this tab" : hand.state === "connecting" ? "Attaching this tab…" : "Code will run in this tab";
}

function use(connected: Wallet.Wallet | undefined) {
  payments = connected && Payments.create(connected, { servers: Mcp.servers });
  set({ connected });
}

Wallet.onPrompt(device => set({ device }));

const saved = Wallet.restore();
if (saved) void Wallet.published(saved).then(onChain => onChain && use(saved), () => {});

export function adopt(connected: Wallet.Wallet | undefined) {
  if (!connected && Wallet.restore()) return;
  if (state.connected?.keyAddress !== connected?.keyAddress) use(connected);
}

export async function login() {
  set({ busy: true, error: undefined });
  try {
    const connected = await Wallet.connect();
    await Wallet.publish(connected);
    use(connected);
  } catch (error) {
    set({ error: error instanceof Error ? error.message : String(error) });
  } finally {
    set({ busy: false, device: undefined });
  }
}

export async function logout() {
  const connected = state.connected;
  Wallet.forget();
  use(undefined);
  if (connected) await Wallet.revoke(connected).catch(() => {});
}

export function useWallet() {
  return useSyncExternalStore(listener => { listeners.add(listener); return () => void listeners.delete(listener); }, () => state);
}

export const prompts = [
  "Clone wevm/viem and list its five largest source files",
  "Run a WebAssembly add function",
  "Store a note in the Tempo gateway's KV and read it back",
  "Search the web with Mercator for today's Tempo news, spending at most $0.05",
];

import { handKeeper, type HandStatus } from "nanocode-ts";
import { Bash } from "nanocode-ts/bash";
import { Mcp, Payment, Payments, Wallet } from "nanocode-ts/machines";
import wasmUrl from "nanocode-ts/nanocode_wasm.wasm";
import { useSyncExternalStore } from "react";

Wallet.configure({ name: "nanocode on Cloudflare", relay: "api/device/", websiteUrl: "https://nanocode.xyz/cloudflare/" });

const decoder = new TextDecoder();
const locked = Payment.locked(Mcp.servers);
const shells = new Map<string, Promise<Bash>>();
let module: Promise<WebAssembly.Module> | undefined;

type State = { connected?: Wallet.Wallet; busy: boolean; error?: string; device?: Wallet.Prompt; hand: HandStatus & { session?: string } };

let state: State = { busy: false, hand: { state: "closed" } };
let payments: Payments.Payments | undefined;
const listeners = new Set<() => void>();

function set(next: Partial<State>) {
  state = { ...state, ...next };
  for (const listener of listeners) listener();
}

const local = (path: string) => new URL(path, location.href).href;

export const pay: typeof fetch = (input, init) => {
  const url = String(input instanceof Request ? input.url : input);
  const body = init?.body instanceof Uint8Array && Mcp.server(url) ? decoder.decode(init.body) : init?.body;
  return (payments?.fetch ?? locked)(input, body === init?.body ? init : { ...init, body });
};

export const keeper = handKeeper({
  fetch: pay,
  listed: async session => ((await (await fetch(local("sessions/" + encodeURIComponent(session) + "/hands"))).json()) as { hands?: string[] }).hands?.includes("browser") ?? false,
  mcpServers: Mcp.servers.map(server => ({ name: server.name, url: server.url })),
  module: () => module ??= WebAssembly.compileStreaming(fetch(wasmUrl)),
  onStatus: hand => set({ hand }),
  url: session => local("sessions/" + encodeURIComponent(session) + "/hands/browser").replace(/^http/, "ws"),
  workspace: async session => {
    let shell = shells.get(session);
    if (!shell) shells.set(session, shell = Bash.create({ cwd: "/workspace", fetch: pay, github: { archive: (owner, repo, ref) => local("api/github/" + owner + "/" + repo + "/" + ref) } }));
    const ready = await shell;
    return { cwd: ready.cwd, workspace: ready.workspace() };
  },
});

function use(connected: Wallet.Wallet | undefined) {
  payments = connected && Payments.create(connected, { servers: Mcp.servers });
  set({ connected });
}

Wallet.onPrompt(device => set({ device }));

const saved = Wallet.restore();
if (saved) void Wallet.published(saved).then(onChain => onChain && use(saved), () => {});

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
  "Clone wevm/viem and chart its largest source files with Python",
  "Run a WebAssembly add function in this tab",
  "Store a note in the Tempo gateway's KV and read it back",
  "Search the web with Mercator for today's Tempo news",
];


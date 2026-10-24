import { Keyring, Wallet } from "nanocode-ts/machines";
import { useSyncExternalStore } from "react";

Wallet.configure({ name: "nanocode on Cloudflare", relay: "api/device/", websiteUrl: "https://nanocode.xyz/cloudflare/" });

const TOKEN = "nanocode-cloudflare-durable:token";

type State = { connected?: Wallet.Wallet; busy: boolean; error?: string; device?: Wallet.Prompt };

let state: State = { busy: false };
const listeners = new Set<() => void>();

function set(next: Partial<State>) {
  state = { ...state, ...next };
  for (const listener of listeners) listener();
}

const headers = (): Record<string, string> => {
  const token = localStorage.getItem(TOKEN);
  return { ...Keyring.visitorHeaders(), ...(token ? { authorization: "Bearer " + token } : {}) };
};

const keyring = Keyring.keyring({ url: "v1/keys", headers });

async function use(connected: Wallet.Wallet | undefined) {
  if (connected) await keyring.register(connected);
  set({ connected });
}

Wallet.onPrompt(device => set({ device }));

const saved = Wallet.restore();
if (saved) void Wallet.published(saved).then(async onChain => { if (onChain) await use(saved); }).catch(() => {});

export async function login() {
  set({ busy: true, error: undefined });
  try {
    const connected = await Wallet.connect();
    await Wallet.publish(connected);
    await use(connected);
  } catch (error) {
    set({ error: error instanceof Error ? error.message : String(error) });
  } finally {
    set({ busy: false, device: undefined });
  }
}

export async function logout() {
  const connected = state.connected;
  Wallet.forget();
  set({ connected: undefined });
  await keyring.logout().catch(() => {});
  if (connected) await Wallet.revoke(connected).catch(() => {});
}

export function useWallet() {
  return useSyncExternalStore(listener => { listeners.add(listener); return () => void listeners.delete(listener); }, () => state);
}

import { DurableObject } from "cloudflare:workers";

export const PUBLIC_REQUESTS = 30;
export const PUBLIC_WINDOW_MS = 60_000;

type Window = { start: number; count: number };

export class PublicLimit extends DurableObject {
  async admit(): Promise<boolean> {
    const now = Date.now();
    const saved = await this.ctx.storage.get<Window>("window");
    const window = saved && now - saved.start < PUBLIC_WINDOW_MS ? saved : { start: now, count: 0 };
    if (window.count >= PUBLIC_REQUESTS) return false;
    await this.ctx.storage.put("window", { start: window.start, count: window.count + 1 }, { allowUnconfirmed: true });
    return true;
  }
}

export function client(address: string): string {
  if (!address.includes(":")) return address;
  const [head, tail] = address.split("::");
  const left = head ? head.split(":") : [];
  const right = tail ? tail.split(":") : [];
  const groups = tail === undefined ? left : [...left, ...Array<string>(8 - left.length - right.length).fill("0"), ...right];
  return `${groups.slice(0, 4).join(":")}::/64`;
}

import { DurableObject } from "cloudflare:workers";
import type { PythonReply, PythonRequest } from "nanocode-ts/bash";

const PORT = 8080;
const IDLE_MS = 5 * 60_000;
const BOOT_MS = 60_000;

export class Python extends DurableObject {
  private async ready(): Promise<Fetcher> {
    const container = this.ctx.container;
    if (!container) throw new Error("this Durable Object has no container");
    if (!container.running) container.start({ enableInternet: true });
    await container.setInactivityTimeout(IDLE_MS);
    const port = container.getTcpPort(PORT);
    const deadline = Date.now() + BOOT_MS;

    for (;;) {
      try {
        const response = await port.fetch("http://python/health");
        if (response.ok) return port;
      } catch (error) {
        if (Date.now() > deadline) throw error;
      }
      if (Date.now() > deadline) throw new Error("the Python container did not start");
      await new Promise(resolve => setTimeout(resolve, 200));
    }
  }

  async run(request: PythonRequest): Promise<unknown> {
    const port = await this.ready();
    const response = await port.fetch("http://python/run", { body: JSON.stringify(request), headers: { "content-type": "application/json" }, method: "POST" });
    const reply = await response.json() as PythonReply & { error?: string };
    if (!response.ok) throw new Error(reply.error ?? "the Python container answered " + response.status);
    return reply;
  }
}

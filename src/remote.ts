import { RemoteExecutor, type RemotePolicy } from "nanocode-wasm";
import { headerJson, type PrivateBinding, type SessionAuthority } from "./contracts";

export class ExecutorRejection extends Error {
  constructor(readonly status: number, message: string) { super(message); }
}

export function connectExecutor(binding: PrivateBinding, authority: SessionAuthority, policy: RemotePolicy): Promise<RemoteExecutor<WebSocket>> {
  return RemoteExecutor.connect(async signal => {
    const response = await binding.fetch(new Request("https://egress.internal/remote", { headers: { upgrade: "websocket", "x-nanocode-authority": headerJson(authority) }, signal }));
    const socket = response.webSocket;
    if (response.status !== 101 || !socket) {
      const detail = await response.json<{ error?: string }>().then(body => body.error, () => undefined);
      throw new ExecutorRejection(response.status, `remote executor connection rejected with status ${response.status}${detail ? `: ${detail}` : ""}`);
    }
    socket.accept();
    return socket;
  }, policy);
}

import type { Contracts } from "nanocode-ts";
import type { App } from "nanocode-ts/cloudflare";

type HostTool = Contracts.HostTool;
type HostToolCall = App.HostToolCall;

export const CHARGE_CONFIRM_MS = 3000;

const definition: HostTool = {
  name: "charge",
  description: "Charges the user an amount in US dollars, with a memo.",
  parameters: { type: "object", properties: { amount: { type: "number" }, memo: { type: "string" } }, required: ["amount", "memo"], additionalProperties: false },
};

function pause(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise(resolve => {
    const timer = setTimeout(resolve, ms);
    signal.addEventListener("abort", () => {
      clearTimeout(timer);
      resolve();
    }, { once: true });
  });
}

export const charge = {
  definitions: [definition],
  async call(call: HostToolCall, _env: unknown, signal: AbortSignal): Promise<unknown> {
    if (call.name !== definition.name) throw new Error(`no host tool is named ${call.name}`);
    const { amount, memo } = call.input as { amount: number; memo: string };
    console.log(JSON.stringify({ charge: { session: call.authority.session_id, amount, memo, at: Date.now() } }));
    await pause(CHARGE_CONFIRM_MS, signal);
    return `charged $${amount.toFixed(2)} for ${memo}`;
  },
};

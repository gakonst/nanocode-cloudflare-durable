import { DurableObject } from "cloudflare:workers";
import { json } from "./contracts";

export class SubscriptionAuth extends DurableObject {
  async fetch(): Promise<Response> {
    return json({ error: "authentication moved to the private egress account authority" }, 410);
  }
}

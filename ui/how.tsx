import "nanocode-ts/react/zyzz.css";
import { Block, Card, Cards, Diagram, Docs, Header, Main, origin, styles } from "nanocode-ts/react/site";
import { createRoot } from "react-dom/client";
import "./fonts.ts";

document.body.className = styles.body().className;

createRoot(document.getElementById("root")!).render(
  <>
    <Header experiment={`${origin}/cloudflare/`} />
    <Main>
      <Docs>
        <h1>How It Works</h1>
        <p>nanocode on Cloudflare runs a nanocode agent in a Cloudflare Durable Object. The agent keeps its journal in the object's own SQLite, so a turn that loses its object continues in the next one from the last checkpoint. Its source is <a href="https://github.com/gakonst/nanocode-cloudflare-durable">gakonst/nanocode-cloudflare-durable</a>.</p>

        <h2>Overview</h2>
        <p>A request reaches the Worker, which forwards it to one Durable Object per account and session. The object runs the nanocode SDK compiled to WebAssembly from <a href="https://github.com/gakonst/nanocode-ts">nanocode-ts</a>, and its model requests go to Cloudflare AI Gateway through the <code>AI</code> binding.</p>
        <p>Before a turn runs, the object stores a watchdog alarm. If the object resets mid-turn, the alarm starts it again, and the turn resumes from the checkpoint in SQLite.</p>
        <Diagram>{`browser ─POST─▶ Worker ─fetch─▶ session Durable Object ─▶ AI Gateway
                                     │
                                     │ checkpoints, turns, events
                                     ▼
                             the object's SQLite
                                     ▲
                                     │ alarm after a reset
                           session Durable Object ─▶ resumes the turn`}</Diagram>
        <p>To try it, open the <a href="./">home page</a>, press "Kill the next tool call", then ask for a charge. The object resets during the charge, its alarm starts it again, and the agent reports that the charge may have gone through.</p>

        <h2>The Agent</h2>
        <p><code>createNanocode</code> returns the session Durable Object class and the Worker's <code>fetch</code> handler. The deployment passes its authentication, its model egress, its host tools and the crash route.</p>
        <Block code={`// One session object per account and session name
const nanocode = createNanocode<Env>({
  authenticate,
  egress: env => aiGateway(() => env.AI.gateway(env.AI_GATEWAY_ID)),
  tools: charge,
  chaos: true,
})

export const { AgentSession } = nanocode
export default { fetch: nanocode.fetch }`} label="worker/index.ts" language="typescript" />

        <h2>The Journal</h2>
        <p>nanocode keeps its state in SQLite through the <code>SqlHost</code> interface of nanocode-ts. <code>DurableObjectSql</code> from <code>nanocode-ts/cloudflare</code> runs each operation's statements in one <code>transactionSync</code>, so every checkpoint, turn and event is durable when the call returns.</p>
        <Block code={`// 1. Run every statement of the operation in one transaction
return this.storage.transactionSync(() =>
  statements.map(([query, ...values]) => {
    const cursor = this.storage.sql.exec(query, ...values)
    // 2. Return the rows to nanocode as arrays
    return [...cursor.raw()]
  }))`} label="nanocode-ts/src/cloudflare/index.ts" language="typescript" />
        <p>The object's SQLite refuses a value above 8 MiB, so the adapter passes that limit to nanocode. nanocode then replaces an oversized event with a marker instead of failing the turn.</p>

        <h2>Turns and Tools</h2>
        <ul>
          <li><strong>Turns:</strong> A turn is a row in nanocode's turn inbox, keyed by its request id. A second request with the same id and prompt returns the same turn.</li>
          <li><strong>Tools:</strong> Before a host tool runs, the object waits for its storage to sync, so the tool's receipt is durable. After a reset, nanocode reports an interrupted call to the model as an unknown outcome.</li>
          <li><strong>Models:</strong> The egress sends nanocode's requests to AI Gateway through the AI binding, and AI Gateway pays the provider from the account's Unified Billing credits. The Worker holds no provider key.</li>
          <li><strong>Events:</strong> nanocode logs every event in SQLite. A client streams them as server-sent events and resumes after the last event id it read.</li>
        </ul>

        <h2>Crash Recovery</h2>
        <p>These are the events of a turn in <code>npm run drill</code>, with the object reset 1.5 s into the charge. The events after the reset come from the object that the alarm started.</p>
        <Block code={`{"type": "tool.call", "tool": "charge"}
  the object resets during the charge
{"type": "running"}
{"type": "tool.result", "tool": "charge", "status": "unknown"}
{"type": "assistant.message"}
{"type": "result", "status": "completed"}`} language="json" />
        <p>The model receives "Tool execution interrupted by a host crash; outcome unknown. Do not assume it did not run or automatically repeat it." The drill checks that the charge ran once and that the model asked for it once.</p>

        <h2>Measurements</h2>
        <table>
          <thead><tr><th>Measurement</th><th>Result</th></tr></thead>
          <tbody>
            <tr><td>Drill on local workerd, 2026-10-20, alarm after 1 s</td><td>Reset at 2.0 s, turn completed at 2.5 s, one charge</td></tr>
            <tr><td>Deployed Haiku turn, 2026-10-21</td><td>3.5 s on a new session, 1.1 s on the next turn</td></tr>
            <tr><td>Deployed crash during the charge, 2026-10-21</td><td>Turn completed 7.3 s after submission, one charge</td></tr>
            <tr><td>Deployed Codex turn, 2026-10-19</td><td>6.8 s on a new session, 2.1 s on the next turn</td></tr>
            <tr><td>Deployed Claude turn, 2026-10-19</td><td>5.0 s from submission</td></tr>
          </tbody>
        </table>

        <h2>Limits</h2>
        <ul>
          <li><strong>Access:</strong> Anyone can run turns on Claude Haiku 5.5, with prompts of up to 2,000 characters and 30 session requests a minute per IP address. Metrics, the session socket and hands need the access token. The pages are static assets.</li>
          <li><strong>Recovery delay:</strong> A reset turn resumes when its alarm fires, at most <code>RECOVERY_DELAY_MS</code> after the reset, 5 s in this deployment.</li>
          <li><strong>Steering:</strong> A message sent during a turn becomes the next turn.</li>
        </ul>

        <h2>More</h2>
        <Cards>
          <Card href="https://github.com/gakonst/nanocode-cloudflare-durable" title="Source →">The Worker, the session object, the app and the drill.</Card>
          <Card href="https://github.com/gakonst/nanocode-ts" title="nanocode-ts →">The runtime and its Durable Object SqlHost.</Card>
          <Card href="https://nanocode.xyz/vercel" title="nanocode on Vercel →">The same agent on durable-harness in Vercel Functions.</Card>
          <Card href="https://nanocode.xyz/docs/introduction/getting-started" title="nanocode docs →">The SDK, its runtime and its hosts.</Card>
        </Cards>
      
      </Docs>
    </Main>
  </>,
);

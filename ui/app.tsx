import "nanocode-ts/react/zyzz.css";
import { Config, Pickers, Plans } from "nanocode-ts/client";
import * as Transport from "nanocode-ts/cloudflare/transport";
import { Keyring } from "nanocode-ts/machines";
import { Prompts, Unlock, useUnlock, walletCards } from "nanocode-ts/machines/react";
import { Chat, HandConnect, NanocodeProvider, type Picker, PlanConnect, useConfig, useConversations, useHand, usePlans, useSend } from "nanocode-ts/react";
import { skin, styles } from "nanocode-ts/react/skin";
import { Demos, Header, Lead, Main, origin, Page, styles as site, Title } from "nanocode-ts/react/site";
import { useEffect, useState, useSyncExternalStore } from "react";
import { createRoot } from "react-dom/client";
import { PLAN_HARNESSES, PUBLIC_HARNESSES } from "../worker/models.ts";
import "./fonts.ts";
import { prompts } from "./clones/prompts.ts";

type Model = Pickers.Model & { label: string };

type Harness = Pickers.Harness & { models: Model[] };

type State = { model?: string; thinking?: string };

const KEY = "nanocode-cloudflare-durable";
const TOKEN = `${KEY}:token`;
let HARNESSES: Harness[] = PUBLIC_HARNESSES;
let MODELS: Model[] = HARNESSES.flatMap(harness => harness.models);

function offer(status: Plans.Status | undefined) {
  HARNESSES = PUBLIC_HARNESSES.map(harness => {
    if (status?.[harness.id].state !== "authenticated") return harness;
    const plan = PLAN_HARNESSES.find(entry => entry.id === harness.id)!;
    return { ...harness, models: [...plan.models, ...harness.models.filter(model => !plan.models.some(entry => entry.id === model.id))] };
  });
  MODELS = HARNESSES.flatMap(harness => harness.models);
}
const COMMANDS = [
  { label: "/model", detail: "choose harness, model and thinking for a new session" },
  { label: "/effort", detail: "choose the thinking of the next turns" },
  { label: "/new", detail: "start a new session" },
  { label: "/hand", detail: "run this session's code on your machine" },
];

const highlight = () => import("nanocode-ts/highlight").then(module => module.highlight);

const listeners = new Set<() => void>();
let state: State = load();
let version = 0;
let token = localStorage.getItem(TOKEN) ?? "";

function load(): State {
  return { ...JSON.parse(localStorage.getItem(KEY) ?? "{}") };
}

function save() {
  localStorage.setItem(KEY, JSON.stringify(state));
  version++;
  for (const listener of listeners) listener();
}

function useStore() {
  useSyncExternalStore(listener => { listeners.add(listener); return () => listeners.delete(listener); }, () => version);
  return state;
}

function current(): Model {
  return MODELS.find(model => model.id === state.model) ?? MODELS[0]!;
}

function harnessOf(model: Model) {
  return HARNESSES.find(harness => harness.models.includes(model))!;
}

const headers = (): Record<string, string> => ({ ...Keyring.visitorHeaders(), ...(token ? { authorization: `Bearer ${token}` } : {}) });

const keyring = Keyring.create({ url: "v1/keys", headers });

const transport = Transport.create({
  headers,
  model: () => ({ provider: harnessOf(current()).id, model: current().id, thinking: state.thinking ?? current().effort }),
  session: { load: () => ({}), save: () => save() },
});

const config = Config.create({ history: { key: KEY }, transport });

const plansClient = Plans.create({ url: "plans", headers });

function App() {
  const store = useStore();
  const chat = useConfig();
  const { send, isPending: busy } = useSend();
  const { current: conversation } = useConversations();
  const [picker, setPicker] = useState<Picker | null>(null);
  const [access, setAccess] = useState(false);
  const [secret, setSecret] = useState(token);
  const unlock = useUnlock(keyring);
  const hand = useHand(transport);
  const plans = usePlans(plansClient);
  offer(plans.status);

  const configure = async () => {
    const open = await fetch("health").catch(() => undefined);
    setAccess(open?.ok !== true);
    const health = await transport.health();
    if (health?.ok && !MODELS.some(entry => entry.id === state.model)) {
      const configured = (await health.json() as { models: { claude: string } }).models.claude;
      const model = MODELS.find(entry => entry.id === configured) ?? MODELS[0];
      if (model) {
        state.model = model.id;
        if (!model.efforts.includes(state.thinking as Model["effort"])) state.thinking = model.efforts.includes("low" as Model["effort"]) ? "low" : model.effort;
        save();
      }
    }
  };

  useEffect(() => {
    void configure();
  }, []);

  useEffect(() => {
    void chat.resume();
  }, [conversation]);

  const pickThinking = (id: string) => {
    const model = MODELS.find(entry => entry.id === id);
    setPicker({ title: `Thinking · ${id}`, items: () => Pickers.thinking(model, { thinking: id === current().id ? state.thinking : undefined }), onChoose: item => { setPicker(null); think(id, item.label); } });
  };

  const pick = (model: string) => {
    if (transport.session().id && model !== current().id) {
      setPicker(null);
      return chat.note("tool", "the model is fixed for this session; use /new before choosing another model");
    }
    pickThinking(model);
  };

  const think = (model: string, level: string) => {
    state.model = model;
    state.thinking = level;
    save();
    chat.note("tool", `using ${model} · thinking ${level}`);
  };

  const clear = () => {
    chat.clear();
    hand.reset();
    save();
  };

  const command = (text: string) => {
    const [name, ...words] = text.split(/\s+/);
    const argument = words.join(" ").toLowerCase();
    if (name === "/new") {
      clear();
      chat.note("tool", "new session");
      return true;
    }
    if (name === "/hand") {
      hand.show();
      return true;
    }
    if (name === "/model") {
      if (!argument) {
        setPicker({ title: "Model", filter: true, items: query => Pickers.models(HARNESSES, { model: current().id, query }), onChoose: item => pick(item.label) });
        return true;
      }
      const match = MODELS.find(model => argument.split(" ").every(word => `${model.id} ${model.name}`.toLowerCase().includes(word)));
      if (match) pick(match.id);
      else chat.note("error", `no model matches "${argument}"; choose one of ${MODELS.map(model => model.id).join(", ")}`);
      return true;
    }
    if (name !== "/effort") return false;
    const model = current();
    if (!argument) {
      pickThinking(model.id);
      return true;
    }
    if (model.efforts.includes(argument as Model["effort"])) think(model.id, argument);
    else chat.note("error", `${model.id} takes ${model.efforts.join("/")} thinking, not ${argument}`);
    return true;
  };

  const warm = (text: string) => {
    if (busy || !text || text.startsWith("/")) return;
    transport.warm();
  };

  const crash = async () => {
    if (!transport.session().id) return chat.note("error", "Start a session before arming a crash.");
    const response = await transport.crash();
    if (response.ok) chat.note("tool", "Armed: the next tool call resets its Durable Object 1.5 s in.");
    else chat.note("error", `${response.status} ${await response.text()}`);
  };

  const model = current();

  return (
    <Chat
      actions={<>
        <button disabled={busy} onClick={clear} type="button" {...styles.suggestion({ quiet: true })}>New session</button>
      </>}
      after={walletCards(unlock)}
      commands={[...COMMANDS, ...unlock.commands]}
      empty="Ask a question to start a turn, or type /model to choose a model."
      extras={<>
        <PlanConnect plans={plans} />
        <HandConnect hand={hand} />
        <Unlock machine={unlock} />
        <Prompts machine={unlock} />
        {access && (
        <label {...styles.row()}>
          <span aria-hidden="true" {...styles.chevron()}>#</span>
          <input
            aria-label="Access token"
            onBlur={() => void configure()}
            onChange={event => {
              setSecret(event.target.value);
              token = event.target.value;
              localStorage.setItem(TOKEN, token);
            }}
            placeholder="Access token"
            type="password"
            value={secret}
            {...styles.input()}
          />
        </label>
        )}
      </>}
      link={{ href: "https://nanocode.xyz/", label: "nanocode.xyz →" }}
      onInput={warm}
      onPickerClose={() => setPicker(null)}
      onSubmit={text => { if (unlock.handles(text)) void unlock.command(text); else if (!command(text)) void send(text); }}
      picker={picker}
      status={`${harnessOf(model).label}${plans.status?.[harnessOf(model).id].state === "authenticated" && Plans.serves(harnessOf(model).id, model.id) ? " plan" : ""} · ${model.label} · ${store.thinking ?? model.effort} · ${transport.session().id ?? "new session"}${hand.where ? ` · ${hand.where}` : ""} · ${busy ? "running" : "idle"}`}
      suggestions={[...prompts.map(prompt => ({ label: prompt, onClick: () => void send(prompt), disabled: busy })), { label: "Kill the next tool call", onClick: () => void crash(), disabled: !transport.session().id }]}
    />
  );
}

document.body.className = site.body().className;

createRoot(document.getElementById("root")!).render(
  <>
    <Header experiment={`${origin}/cloudflare/`} />
    <Main>
      <Page>
        <Title subtitle="Durable agents in Durable Objects.">nanocode on Cloudflare</Title>
        <Demos current="cloudflare" />
        <Lead>This agent runs in a Cloudflare Durable Object: the nanocode SDK compiled to WebAssembly, with its journal in the object's own SQLite. Its Code Mode cells run beside it in the object: bash on a workspace kept in the same SQLite, git clone, WebAssembly, Python with numpy, pandas and matplotlib on Pyodide, and a fetch, Mercator and the Tempo gateway that pay MPP services from your Tempo Wallet, whose access key the Worker keeps encrypted so turns go on paying after you close the tab. When the object resets mid-turn, its alarm starts it again, and the turn continues from the last checkpoint. Cloudflare AI Gateway serves its model requests, or your own ChatGPT or Claude plan once you connect it: its credential stays sealed in a Durable Object of this deployment, and ChatGPT's requests leave through a Cloudflare Container, because chatgpt.com refuses requests from Workers. Its source is <a href="https://github.com/gakonst/nanocode-cloudflare-durable">gakonst/nanocode-cloudflare-durable</a>.</Lead>
        <NanocodeProvider config={config} highlight={highlight} skin={skin}>
          <App />
        </NanocodeProvider>
        <p>Also as a <a href="chatgpt">ChatGPT-style</a> and a <a href="claude">Claude-style</a> app, or theme it yourself in the <a href="playground">playground</a>.</p>
        <p><a href="how-it-works">Read how it works →</a></p>
      </Page>
    </Main>
  </>,
);

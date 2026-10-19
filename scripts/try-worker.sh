#!/usr/bin/env bash
set +x
set -euo pipefail

usage() {
  printf 'Usage: %s [claude|codex] [paradigm-a|paradigm-b|here] [prompt]\n' "$0"
  printf 'here attaches this machine as the hand for its own prompt with nanocodex runner --code-mode while the prompt is submitted.\n'
}

if [[ ${1:-} == --help || ${1:-} == -h ]]; then
  usage
  exit 0
fi

provider=${1:-claude}
executor=${2:-paradigm-a}
prompt=${3:-Use remote Code Mode and Bash to run pwd and uname -a, then summarize the output.}
if (( $# > 3 )); then
  usage >&2
  exit 2
fi
case "$provider" in
  claude|codex) ;;
  *) usage >&2; exit 2 ;;
esac
case "$executor" in
  paradigm-a|paradigm-b|here) ;;
  *) usage >&2; exit 2 ;;
esac

worker_token=${WORKER_TOKEN:-}
if [[ -z $worker_token ]]; then
  keychain_helper="$HOME/Library/Application Support/nanocode-worker/keychain-secret"
  if [[ ! -x $keychain_helper ]]; then
    printf 'Provide WORKER_TOKEN through your secret manager.\n' >&2
    exit 1
  fi
  worker_token=$("$keychain_helper" get nanocode-worker:WORKER_TOKEN)
fi
if [[ ! $worker_token =~ ^[-A-Za-z0-9._~+/=]+$ ]]; then
  printf 'The Worker token is empty or invalid.\n' >&2
  exit 1
fi

session="try-$(uuidgen)"
origin=nanocode-worker.gakonst.workers.dev
hand_pid=""
stop_hand() {
  if [[ -z $hand_pid ]]; then return; fi
  if ! kill "$hand_pid" 2>/dev/null; then printf 'The local hand exited early; see %s.\n' "$hand_log" >&2; fi
}
trap stop_hand EXIT
if [[ $executor == here ]]; then
  hand="cli-$(uuidgen | tr '[:upper:]' '[:lower:]' | cut -c1-8)"
  hand_log="${TMPDIR:-/tmp}/nanocode-hand-$session.log"
  NANOCODEX_RUNNER_TOKEN=$worker_token "${NANOCODEX_BIN:-nanocodex}" runner --code-mode --connect "wss://$origin/sessions/$session/hands/$hand" --name "$hand" -C "$PWD" 2>"$hand_log" &
  hand_pid=$!
  executor="$hand (this machine)"
fi
request_data=$(node --input-type=module -e '
const [provider, executor, prompt, hand] = process.argv.slice(1);
const target = hand ? { hand } : { executor };
process.stdout.write(JSON.stringify(JSON.stringify({ request_id: "try-1", provider, ...target, prompt })));
' "$provider" "$executor" "$prompt" "${hand:-}")
stream_events() {
  node --input-type=module -e '
import { createInterface } from "node:readline";
const [provider, executor] = process.argv.slice(1);
const calls = new Map();
let data = [], terminal = false, last = "\n", speaker = "", channel = "", nextCall = 0;
function write(value) {
  if (!value) return;
  process.stdout.write(value);
  last = value.slice(-1);
}
function line(value = "") {
  if (last !== "\n") write("\n");
  write(value + "\n");
}
function pretty(value) {
  return typeof value === "string" ? value : JSON.stringify(value, null, 2);
}
function readable(value) {
  if (typeof value !== "string") return pretty(value);
  try {
    const parsed = JSON.parse(value);
    if (parsed && typeof parsed === "object") return typeof parsed.output === "string" ? parsed.output.trimEnd() : pretty(parsed);
  } catch {}
  return value;
}
function content(value) {
  if (Array.isArray(value)) return value.map(block => content(block)).filter(Boolean).join("\n");
  if (typeof value?.text === "string") return readable(value.text);
  if (Array.isArray(value?.content_blocks)) return content(value.content_blocks);
  if (value?.type) return "[" + value.type + "]";
  return readable(value);
}
function flush() {
  if (!data.length) return;
  let envelope;
  try { envelope = JSON.parse(data.join("\n")); }
  catch { process.stderr.write("Invalid Worker stream.\n"); process.exitCode = 1; data = []; return; }
  data = [];
  const outer = envelope.event;
  const child = outer?.source === "subagent" ? outer.update : undefined;
  const event = outer?.source === "agent" ? outer.event : child?.event;
  const agent = child?.id ? String(child.id) : "";
  const prefix = agent ? "[agent " + agent.slice(0, 8) + "] " : "";
  const payload = event?.payload ?? {};
  if (["assistant.delta", "reasoning.summary.delta"].includes(event?.type) && typeof payload.text === "string" && payload.text) {
    const nextChannel = event.type === "reasoning.summary.delta" ? "reasoning" : "answer";
    if (speaker !== agent || channel !== nextChannel) {
      if (nextChannel === "reasoning") line(prefix + "[reasoning]");
      else if (prefix || channel === "reasoning") line(prefix + "[answer]");
      speaker = agent;
      channel = nextChannel;
    }
    write(payload.text);
  }
  if (event?.type === "assistant.message" && last !== "\n") line();
  if (outer?.type === "queued") line("[queued]");
  if (outer?.type === "running") line("[running] " + provider + " on " + executor);
  if (event?.type === "model.call.started") { line(prefix + "[model] " + provider + " generating"); channel = ""; }
  if (child?.type === "added") line("[agent started] " + (child.agent?.name ?? child.agent?.id ?? "subagent"));
  if (child?.type === "completion") line(prefix + "[agent finished] " + pretty(child.status));
  if (event?.type === "tool.call" || event?.type === "tool.result") {
    const key = agent + ":" + payload.call_id;
    let call = calls.get(key);
    if (!call) { call = { number: ++nextCall, tool: payload.tool ?? "tool" }; calls.set(key, call); }
    if (event.type === "tool.call") {
      const parent = calls.get(agent + ":" + payload.parent_call_id);
      line(prefix + "[tool #" + call.number + "] " + call.tool + (parent ? " (in #" + parent.number + ")" : ""));
      const args = payload.arguments;
      if (typeof args?.cmd === "string") { line("$ " + args.cmd); if (args.workdir) line("cwd: " + args.workdir); }
      else if (typeof args?.code === "string") line(args.code);
      else if (args !== undefined) line(pretty(args));
    } else {
      const result = payload.structured_result;
      const duration = typeof payload.duration_ns === "number" ? ", " + (payload.duration_ns / 1e6).toFixed(1) + "ms" : "";
      const exit = typeof result?.exit_code === "number" ? ", exit " + result.exit_code : "";
      line(prefix + "[result #" + call.number + "] " + call.tool + " (" + (payload.status ?? "finished") + duration + exit + ")");
      const output = typeof result?.output === "string" ? result.output : content(payload.result ?? result);
      if (output) line(output.trimEnd());
    }
  }
  if (outer?.type === "result") {
    terminal = true;
    line("[" + (outer.status ?? "failed") + "]");
    if (outer.status !== "completed") {
      process.stderr.write(`Worker turn ${outer.status ?? "failed"}.\n`);
      process.exitCode = 1;
    }
  }
}
for await (const line of createInterface({ input: process.stdin, crlfDelay: Infinity })) {
  if (line === "") flush();
  else if (line.startsWith("data:")) data.push(line.slice(5).replace(/^ /, ""));
}
flush();
if (last !== "\n") write("\n");
if (!terminal) { process.stderr.write("Stream ended without a final result.\n"); process.exitCode = 1; }
' "$provider" "$executor"
}

curl -NsS --fail-with-body --connect-timeout 15 --max-time 300 --config - <<EOF | stream_events
url = "https://$origin/sessions/$session/turns"
header = "Authorization: Bearer $worker_token"
header = "Content-Type: application/json"
header = "Accept: text/event-stream"
data = $request_data
EOF

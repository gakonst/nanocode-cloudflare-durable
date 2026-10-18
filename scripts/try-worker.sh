#!/usr/bin/env bash
set +x
set -euo pipefail

usage() {
  printf 'Usage: %s [claude|codex] [paradigm-a|paradigm-b] [prompt]\n' "$0"
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
  paradigm-a|paradigm-b) ;;
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
request_data=$(node --input-type=module -e '
const [provider, executor, prompt] = process.argv.slice(1);
process.stdout.write(JSON.stringify(JSON.stringify({ request_id: "try-1", provider, executor, prompt })));
' "$provider" "$executor" "$prompt")
stream_text() {
  node --input-type=module -e '
import { createInterface } from "node:readline";
let data = [], terminal = false, text = "";
function flush() {
  if (!data.length) return;
  let envelope;
  try { envelope = JSON.parse(data.join("\n")); }
  catch { process.stderr.write("Invalid Worker stream.\n"); process.exitCode = 1; data = []; return; }
  data = [];
  const outer = envelope.event;
  const event = outer?.source === "agent" ? outer.event : undefined;
  if (event?.type === "assistant.delta" && typeof event.payload?.text === "string" && event.payload.text) {
    text = event.payload.text;
    process.stdout.write(text);
  }
  if (outer?.type === "result") {
    terminal = true;
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
if (text && !text.endsWith("\n")) process.stdout.write("\n");
if (!terminal) { process.stderr.write("Stream ended without a final result.\n"); process.exitCode = 1; }
'
}

curl -NsS --fail-with-body --connect-timeout 15 --max-time 300 --config - <<EOF | stream_text
url = "https://nanocode-worker.gakonst.workers.dev/sessions/$session/turns"
header = "Authorization: Bearer $worker_token"
header = "Content-Type: application/json"
header = "Accept: text/event-stream"
data = $request_data
EOF

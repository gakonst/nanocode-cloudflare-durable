#!/usr/bin/env bash
set +x
set -euo pipefail

usage() {
  printf 'Usage: %s [claude|codex] [paradigm-a|paradigm-b|here] [prompt]\n' "$0"
  printf 'here attaches this machine as the hand for its own prompt with nanocode hand and submits once the session lists it.\n'
  printf 'Pass - as the prompt to send one prompt per stdin line over the same connection, session and hand.\n'
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
  NANOCODE_RUNNER_TOKEN=$worker_token "${NANOCODE_BIN:-nanocode}" hand "wss://$origin/sessions/$session/hands/$hand" 2>"$hand_log" &
  hand_pid=$!
  executor="$hand (this machine)"
fi
client=(node "$(dirname "$0")/worker-client.mjs" --origin "https://$origin" --session "$session" --provider "$provider" --label "$executor")
if [[ -n ${hand:-} ]]; then client+=(--hand "$hand" --hand-log "$hand_log"); else client+=(--executor "$executor"); fi
WORKER_TOKEN=$worker_token "${client[@]}" "$prompt"

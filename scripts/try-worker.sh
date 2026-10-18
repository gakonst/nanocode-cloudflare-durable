#!/usr/bin/env bash
set +x
set -euo pipefail

usage() {
  printf 'Usage: %s [claude|codex] [paradigm-a|paradigm-b]\n' "$0"
}

if [[ ${1:-} == --help || ${1:-} == -h ]]; then
  usage
  exit 0
fi

provider=${1:-claude}
executor=${2:-paradigm-a}
if (( $# > 2 )); then
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
curl -NsS --fail-with-body --connect-timeout 15 --max-time 300 --config - <<EOF
url = "https://nanocode-worker.gakonst.workers.dev/sessions/$session/turns"
header = "Authorization: Bearer $worker_token"
header = "Content-Type: application/json"
header = "Accept: text/event-stream"
data = "{\"request_id\":\"try-1\",\"provider\":\"$provider\",\"executor\":\"$executor\",\"prompt\":\"Use remote Code Mode and Bash to run pwd and uname -a, then summarize the output.\"}"
EOF

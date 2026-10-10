#!/usr/bin/env bash
# Runs workflows-stack's conformance harness against conformance/ (the push handler), and the
# pull scenarios against the same workflows served by a pull worker.
#   scripts/conformance.sh <workflows-stack checkout at .workflows-stack-rev> [harness flags…]
set -euo pipefail
stack=$(cd "${1:?usage: scripts/conformance.sh <workflows-stack checkout> [harness flags…]}" && pwd)
shift
root=$(pwd)
port=${PORT:-3100}
pull_port=${PULL_PORT:-3300}
secret=whsec_$(head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n')
mkdir -p .bin
(cd "$stack" && go build -o "$root/.bin/workflows-conformance" ./cmd/workflows-conformance)
go build -o .bin/conformance-app ./conformance
pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; for p in "${pids[@]}"; do wait "$p" 2>/dev/null || true; done; }
trap cleanup EXIT
CONFORMANCE_SECRET=$secret PORT=$port .bin/conformance-app & pids+=($!)
pull=()
if [ "${CONFORMANCE_PULL:-1}" = 1 ] && grep -q 'pull-addr' <(.bin/workflows-conformance run --help 2>&1 || true); then
  CONFORMANCE_MODE=pull CONFORMANCE_LIVE=http://127.0.0.1:$pull_port .bin/conformance-app & pids+=($!)
  pull=(--pull-addr "127.0.0.1:$pull_port" --api-key conformance-key --pull-queue conformance)
fi
for _ in $(seq 1 100); do curl -s -o /dev/null "http://127.0.0.1:$port/" && break; sleep 0.1; done
.bin/workflows-conformance run --app "http://127.0.0.1:$port/" --secret "$secret" --scenarios "$stack/protocol/conformance" ${pull[@]+"${pull[@]}"} "$@"

#!/usr/bin/env bash
# Cada serviço executa uma única JVM e preserva seu log e código de saída.
set -euo pipefail
cd "$(dirname "$0")/.."
state="${TP2_STATE:-/state}"
name="${2:-$1}"
run_id="$(cat "$state/run-id")"
log="logs/$run_id/$name.log"

if [[ "$1" == "--health" ]]; then
  grep -qE '^[0-9:.]+[[:space:]]+[^[:space:]]+[[:space:]]+READY[[:space:]]' "$log"
  exit
fi

mapfile -t arguments < "$state/$name.args"
main="${arguments[0]}"
set +e
timeout --signal=TERM --kill-after=5s "${TIMEOUT:-1800}s" \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn -cp target/tp2-sync.jar \
  "tp2.$main" "${arguments[@]:1}" 2>&1 | tee "$log"
codes=("${PIPESTATUS[@]}")
set -e
printf '%s' "${codes[0]}" > "$state/$name.exit"
expected="$(cat "$state/$name.expected")"
if [[ "${codes[0]}" != "$expected" || "${codes[1]}" != "0" ]]; then
  printf '%s\n' "ERRO: $name encerrou com código ${codes[0]}; esperado $expected (log=${codes[1]})" >&2
  exit 1
fi

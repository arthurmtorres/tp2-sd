#!/usr/bin/env bash
# Executado por docker compose exec; o contêiner permanece ativo entre cenários.
set -euo pipefail
cd "$(dirname "$0")/.."
action="${1:?Informe --run ou --stop}"
run_id="${2:?Informe o identificador da execução}"
[[ "$run_id" =~ ^[a-zA-Z0-9_-]+$ ]] || exit 1
pid_file="${TP2_PID_DIR:-/tmp}/tp2-$run_id.pid"

if [[ "$action" == "--stop" ]]; then
  if [[ -f "$pid_file" ]]; then
    kill -TERM "$(cat "$pid_file")" 2>/dev/null || true
  fi
  exit 0
fi
[[ "$action" == "--run" ]] || exit 1
shift 2
main="${1:?Informe a classe Java}"
shift
child=""
cleanup() {
  if [[ -n "$child" ]]; then
    kill -TERM "$child" 2>/dev/null || true
    wait "$child" 2>/dev/null || true
  fi
  rm -f "$pid_file"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
timeout --signal=TERM --kill-after=5s "${TIMEOUT:-1800}s" \
  java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn -cp target/tp2-sync.jar \
  "tp2.$main" "$@" &
child=$!
printf '%s' "$child" > "$pid_file"
wait "$child"
